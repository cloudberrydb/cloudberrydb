#include "textFileRead.h"

extern "C"
{
#include "access/external.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "utils/lsyscache.h"

static List *getExtTableEntryOptions(Oid relid) {
	Relation	pg_foreign_table_rel;
	ScanKeyData ftkey;
	SysScanDesc ftscan;
	HeapTuple	fttuple;
	bool		isNull;
	List		*ftoptions_list = NIL;;

	pg_foreign_table_rel = table_open(ForeignTableRelationId, RowExclusiveLock);

	ScanKeyInit(&ftkey,
				Anum_pg_foreign_table_ftrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	ftscan = systable_beginscan(pg_foreign_table_rel, ForeignTableRelidIndexId,
								true, NULL, 1, &ftkey);
	fttuple = systable_getnext(ftscan);

	if (!HeapTupleIsValid(fttuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_foreign_table entry for relation \"%s\"",
						get_rel_name(relid))));
	}

	/* get the foreign table options */
	Datum ftoptions = heap_getattr(fttuple,
						   Anum_pg_foreign_table_ftoptions,
						   RelationGetDescr(pg_foreign_table_rel),
						   &isNull);

	if (isNull)
	{
		/* options array is always populated, {} if no options set */
		elog(ERROR, "could not find options for external protocol");
	}
	else
	{
		ftoptions_list = untransformRelOptions(ftoptions);
	}

	/* Finish up scan and close catalogs */
	systable_endscan(ftscan);
	table_close(pg_foreign_table_rel, RowExclusiveLock);

	return ftoptions_list;
}

}

namespace Datalake {
namespace Internal {

void textFileRead::createHandler(void* sstate) {
	dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;
	initParameter(sstate);
    initializeDataStructures(ss);
	createPolicy();
    readNextGroup();
}

void textFileRead::initializeDataStructures(dataLakeFdwScanState *ss) {
	skip_first_line = false;
	last = false;
	index = 0;
	createFileStream(ss);

	ListCell *lc;
    List *extoptions = getExtTableEntryOptions(RelationGetRelid(ss->rel));
	foreach(lc, extoptions) {
		DefElem *defel = (DefElem *) lfirst(lc);
		if (strcmp(defel->defname, "newline") == 0) {
			char* delimiter = defGetString(defel);
			if (pg_strcasecmp(delimiter, "lf") == 0) {
				recordDelimiterBytes = pstrdup("\n");
			} else if (pg_strcasecmp(delimiter, "cr") == 0) {
				recordDelimiterBytes = pstrdup("\r");
			} else if (pg_strcasecmp(delimiter, "crlf") == 0) {
				recordDelimiterBytes = pstrdup("\r\n");
			} else {
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("invalid value for NEWLINE (%s)", delimiter),
						errhint("valid options are: 'LF', 'CRLF', 'CR'")));
			}
			break;
		}
	}

	if (recordDelimiterBytes == NULL) {
		recordDelimiterBytes = pstrdup("\n");
	}
	allocateDataBuffer();
	lineReader = std::make_shared<lineRecordReader>(65536, recordDelimiterBytes, &memory_buffer);
}

void textFileRead::createFileStream(dataLakeFdwScanState *ss) {
	gopherConfig* conf = createGopherConfig((void*)(ss->options->gopher));
	if (conf->cache_strategy == GOPHER_CACHE) {
		options.enableCache = true;
	}

	fileStream = createFileSystem(conf);
	freeGopherConfig(conf);
}

void textFileRead::createPolicy() {
	std::vector<ListContainer> lists;
	extraFragmentLists(lists, scanstate->fragments);

	segid = GpIdentity.segindex;
	readPolicy.textFileBuild(segid, segnum, CUT_TEXTFILE_BLOCK_SIZE, lists, fileStream, options);
	readPolicy.distBlock();
	serial = readPolicy.start;
	lineReader->setSegId(segid);
}

void textFileRead::allocateDataBuffer() {
    dataLength = 8 *1024 *1024;
    dataBuffer = (char*)palloc(dataLength + 1);
    memory_buffer.buffer = dataBuffer;
    memory_buffer.buffer_length = dataLength;
}

int64_t textFileRead::readWithBuffer(void* buffer, int64_t length) {
    while(!last) {
        if (!readNextGroup()) {
            blockOffset = 0;
        } else {
            int64_t nread = readWithBufferInternal(buffer, length);
            if (nread == 0) {
                /* read next file or block. */
                continue;
            }
            return nread;
        }
    }
    return 0;
}

int64_t textFileRead::readForFile(void* buffer, int64_t length) {
    int64_t readBytes = 0;
    int64_t blockRemainBytes = curMetaInfo.rangeOffsetEnd - (curMetaInfo.rangeOffset + blockOffset);
    int64_t remainReadBytes = blockRemainBytes >= length ? length : blockRemainBytes;
    while(remainReadBytes) {
        int64_t nread = reader->read(buffer, length);
        blockOffset += nread;
        remainReadBytes -= nread;
        readBytes += nread;
    }
    return readBytes;
}

int64_t textFileRead::readForSnappyFile(void* buffer, int64_t length) {
    int64_t nread = reader->read(buffer, length);
    return nread;
}

int64_t textFileRead::readForDeflateFile(void* buffer, int64_t length) {
    int64_t nread = reader->read(buffer, length);
    return nread;
}

int64_t textFileRead::readDefaultForBlock(void* buffer, int64_t length) {
    bool first_line = skip_first_line;
    do {
        int size = 0;
        /* first block fetch line. */
        bool found_delimiter = lineReader->next(size);
        blockOffset += size;
        if (found_delimiter) {
            if (skip_first_line) {
                skip_first_line = false;
                continue;
            }
            if (first_line) {
                first_line = false;
            }
            index++;
            if (external_table_debug) {
                elog(LOG, "External table, filename %s index %ld postition %ld.", curFileName.c_str(), index, lineReader->getPos());
            }
            memcpy(buffer, memory_buffer.buffer + lineReader->getBufferStartPosn(), size);

            return size;
        } else {
            if (lineReader->getState() == LINE_RECORD_READ_BLOCK_OVER) {
                /* block read over. get next block */
                return 0;
            }

            if (skip_first_line) {
                continue;
            }
            memcpy(buffer, memory_buffer.buffer + lineReader->getBufferStartPosn(), size);
            return size;
        }
    } while(first_line);

    return 0;
}

int64_t textFileRead::readCustomForBlock(void* buffer, int64_t length) {
    int size = 0;
    while(skipNum > 0) {
        size = 0;
        bool found_delimiter = lineReader->next(size);
        if (size == 0) {
            return size;
        }
        blockOffset += size;
        if (found_delimiter) {
            if (skipNum) {
                if (external_table_debug) {
                    elog(LOG, "External table, filename %s block start position %ld "
                        "block end position %ld lineReader current position %ld, remaining "
                        "skip line number %d.",
                        curFileName.c_str(), curMetaInfo.rangeOffset, curMetaInfo.rangeOffsetEnd,
                        lineReader->getPos(), skipNum);
                }
                skipNum--;
            }
        }
    }

    size = 0;
    /* first block fetch line. */
    bool found_delimiter = lineReader->next(size);
    blockOffset += size;
    if (found_delimiter) {
        index++;
        if (external_table_debug) {
            elog(LOG, "External table, filename %s index %ld postition %ld.",
                curFileName.c_str(), index, lineReader->getPos());
        }
        memcpy(buffer, memory_buffer.buffer + lineReader->getBufferStartPosn(), size);
    } else {
        if (lineReader->getState() == LINE_RECORD_READ_BLOCK_OVER) {
            /* block read over. get next block */
            return 0;
        }
        memcpy(buffer, memory_buffer.buffer + lineReader->getBufferStartPosn(), size);
    }
    return size;
}

int64_t textFileRead::readForBlock(void* buffer, int64_t length) {
    return readCustomForBlock(buffer, length);
}

int64_t textFileRead::readWithBufferInternal(void* buffer, int64_t length) {
    if (curMetaInfo.compress == SNAPPY) {
        return readForSnappyFile(buffer, length);
    } else if (curMetaInfo.compress == DEFLATE) {
        return readForDeflateFile(buffer, length);
    } else if (curMetaInfo.compress != UNCOMPRESS) {
        return readForFile(buffer, length);
    } else {
        return readForBlock(buffer, length);
    }
}

bool textFileRead::readNextGroup() {
	while(true) {
		if (getNextGroup()) {
			return true;
		}

		if (!readNextFile()) {
			last = true;
			break;
		}
		serial++;
	}
	return false;
}

bool textFileRead::getNextGroup() {
    if (curMetaInfo.compress == UNCOMPRESS) {
        if (lineReader->getState() == LINE_RECORD_INIT) {
            lineReader->setState(LINE_RECORD_OK);
            return false;
        } else if (lineReader->getState() == LINE_RECORD_FETCH_FINAL_LINE) {
            return false;
        }
    } else if (curMetaInfo.compress == SNAPPY ||
                curMetaInfo.compress == DEFLATE) {
        if (reader->isReadFinish()) {
            return false;
        }
    } else if ((curMetaInfo.rangeOffset + blockOffset >= curMetaInfo.rangeOffsetEnd)) {
        return false;
    }

    return true;
}

bool textFileRead::readNextFile() {
    if (serial > (int)readPolicy.end) {
        return false;
    }
    auto it = readPolicy.block.find(serial);
    if (it != readPolicy.block.end()) {
        metaInfo info = it->second;
        if (info.exists) {
            if (curMetaInfo.fileName != info.fileName) {
                return openTextFile(info);
            } else {
                updateMetaInfo(info);
                bool finalBlock = (serial == (int)readPolicy.end) ? true : false;
                lineReader->updateSplit(info, finalBlock);
                skip_first_line = false;
                if (lineReader->getPos() >= curMetaInfo.rangeOffsetEnd) {
                    return true;
                }
                lineReader->setState(LINE_RECORD_OK);
            }
        }
    } else {
        if (external_table_debug) {
            int64_t blockCount = readPolicy.block.size();
            elog(LOG, "External table, block index %d "
                "not fond in block policy, block count %ld", serial, blockCount);
        }
        return false;
    }
    return true;
}

bool textFileRead::openTextFile(metaInfo info) {
    reader.reset();
    reader = getTextFileInput(info.compress);
    blockOffset = 0;
    curFileName = info.fileName;

    if (reader != NULL && reader->getState() == FILE_OPEN) {
        reader->close();
        if (external_table_debug) {
            elog(LOG, "external table, close filename %s block end postition %ld block serial number "
                "%d index number %ld.", curFileName.c_str(), info.rangeOffsetEnd, serial, index);
        }
    }
    index = 0;

    if (external_table_debug) {
        elog(LOG, "external table, open filename %s block start postition %ld block serial number %d.",
            curFileName.c_str(), info.rangeOffset, serial);
    }

    reader->open(fileStream, info.fileName, options);
    reader->setFileLength(info.rangeOffsetEnd);
    if (info.rangeOffset != 0) {
        reader->seek(info.rangeOffset);
    }
    lineReader->resetLineRecordReader();
    lineReader->setState(LINE_RECORD_OK);
    lineReader->updateNewReader(reader.get());
    bool finalBlock = (serial == (int)readPolicy.end) ? true : false;
	lineReader->updateSplit(info, finalBlock);
    updateMetaInfo(info);
    skipNum = (info.rangeOffset != 0) ? 2 : 0;
    return true;
}

void textFileRead::updateMetaInfo(metaInfo info) {
    curMetaInfo.exists = info.exists;
    curMetaInfo.fileName = info.fileName;
    curMetaInfo.fileLength = info.fileLength;
    curMetaInfo.blockSize = info.blockSize;
    curMetaInfo.rangeOffset = info.rangeOffset;
    curMetaInfo.rangeOffsetEnd = info.rangeOffsetEnd;
    curMetaInfo.compress = info.compress;
}

void textFileRead::destroyHandler() {
    if (dataBuffer) {
        pfree(dataBuffer);
        dataBuffer = NULL;
    }
    if (recordDelimiterBytes) {
        pfree(recordDelimiterBytes);
        recordDelimiterBytes = NULL;
    }
    if (reader) {
        reader->close();
    }
	releaseResources();
}


}
}