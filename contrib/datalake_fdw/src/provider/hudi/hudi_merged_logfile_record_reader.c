#include "postgres.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "utils/builtins.h"
#include "nodes/parsenodes.h"
#include "gopher/gopher.h"

#include "src/provider/common/file_reader.h"
#include "hudi_logfile_block_reader.h"
#include "hudi_merged_logfile_record_reader.h"
#include "hudi_hashtab_merger.h"
#include "hudi_btree_merger.h"
// #include "datalake_extension.h"

#include "src/provider/common/kryo.h"
#include "src/provider/common/kryo_input.h"

extern int hudiLogMergerThreshold;

static void processQueuedBlocks(HudiMergedLogfileRecordReader *reader);
static bool scanBlock(HudiMergedLogfileRecordReader *reader, HudiLogFileBlock *block);

HudiMergedLogfileRecordReader *
createMergedLogfileRecordReader(MemoryContext readerMcxt,
								List *columnDesc,
								TupleDesc tupDesc,
								bool *attrUsed,
								const char *instantTime,
								gopherFS gopherFilesystem,
								List *logfiles,
								ExternalTableMetadata *tableOptions)
{
	int64 totalFileSize;
	HudiMergedLogfileRecordReader *reader = palloc0(sizeof(HudiMergedLogfileRecordReader));

	reader->logfiles = logfiles;
	reader->columnDesc = columnDesc;
	reader->attrUsed = attrUsed;
	reader->instantTime = instantTime;
	reader->gopherFilesystem = gopherFilesystem;
	reader->readerMcxt = readerMcxt;

	reader->mergerMcxt = AllocSetContextCreate(CurrentMemoryContext,
											   "MergedLogRecordReaderContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	reader->deleteBlockMcxt = AllocSetContextCreate(CurrentMemoryContext,
											   "DeleteLogBlockContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	reader->deleteRowMcxt = AllocSetContextCreate(CurrentMemoryContext,
											   "DeleteLogRowContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	reader->tableOptions = tableOptions;

	/* NB: I intentionally saved the fileSize into the recordCount field */
	totalFileSize = getFileRecordCount(logfiles);
	if (totalFileSize < hudiLogMergerThreshold * 1024 * 1024)
	{
		reader->mergeProvider = (MergeProvider *) createHudiHashTableMerger(
														reader->mergerMcxt,
														reader->columnDesc,
														tableOptions->recordKeyFields,
														tableOptions->preCombineField
												  );
	}
	else
	{
		reader->mergeProvider = (MergeProvider *) createHudiBtreeMerger(
														reader->columnDesc,
														tableOptions->recordKeyFields,
														tableOptions->preCombineField,
														totalFileSize,
														tupDesc
												  );
	}

	PG_TRY();
	{
		performScan(reader);
	}
	PG_CATCH();
	{
		reader->mergeProvider->Close(reader->mergeProvider);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return reader;
}

void
performScan(HudiMergedLogfileRecordReader *reader)
{
	bool shouldContinue;
	HudiLogFileBlock *curBlock;
	HudiLogFileReader *curReader;
	FileFragment *logFile = list_nth(reader->logfiles, 0);

	elog(DEBUG1, "scanning log file %s", logFile->filePath);
	curReader = createHudiLogFileReader(reader->mergerMcxt, reader->gopherFilesystem, logFile->filePath);
	reader->logfiles = list_delete_first(reader->logfiles);

	while (true)
	{
		if (hudiLogFileNext(curReader, &curBlock))
		{
			shouldContinue = scanBlock(reader, curBlock);
			if (!shouldContinue)
			{
				hudiLogFileClose(curReader);
				pfree(logFile);
				break;
			}
		}
		else if (list_length(reader->logfiles) > 0)
		{
			hudiLogFileClose(curReader);
			pfree(logFile);

			logFile = list_nth(reader->logfiles, 0);

			elog(DEBUG1, "scanning log file %s", logFile->filePath);
			curReader = createHudiLogFileReader(reader->mergerMcxt, reader->gopherFilesystem, logFile->filePath);
			reader->logfiles = list_delete_first(reader->logfiles);
		}
		else
		{
			hudiLogFileClose(curReader);
			pfree(logFile);
			break;
		}
	}

	/* merge the last read block when all the blocks are done reading */
	if (list_length(reader->queuedBlocks) > 0)
		processQueuedBlocks(reader);
}

void
mergedLogfileRecordReaderClose(HudiMergedLogfileRecordReader *reader)
{
	if (reader->mergeProvider)
		reader->mergeProvider->Close(reader->mergeProvider);

	if (reader->mergerMcxt)
		MemoryContextDelete(reader->mergerMcxt);
	if (reader->deleteBlockMcxt)
		MemoryContextDelete(reader->deleteBlockMcxt);
	if (reader->deleteRowMcxt)
		MemoryContextDelete(reader->deleteRowMcxt);

	pfree(reader);
}

static bool
containsInstantTime(List *instantTimes, const char *instantTime)
{
	ListCell *lc;

	foreach(lc , instantTimes)
	{
		char *item = strVal(lfirst(lc));

		if (strcmp(item, instantTime) == 0)
			return true;
	}

	return false;
}

static bool
containsOrBeforeInstantTime(List *instantTimes, const char *firstNonSavepointCommit, const char *instantTime)
{
	if (containsInstantTime(instantTimes, instantTime))
		return true;

	if (firstNonSavepointCommit == NULL)
		return false;

	return strcmp(instantTime, firstNonSavepointCommit) < 0;
}

static bool
isNewInstantBlock(const char *blockInstantTime, List *queuedBlocks)
{
	const char *lastInstantTime;
	HudiLogFileBlock *logBlock;

	if (list_length(queuedBlocks) == 0)
		return false;

	logBlock = llast(queuedBlocks);
	if (logBlock->type == CORRUPTED_BLOCK)
		return false;

	lastInstantTime = logBlockGetInstantTime(logBlock);
	return !(strcmp(lastInstantTime, blockInstantTime) == 0);
}

static void
processDataBlock(HudiMergedLogfileRecordReader *reader, HudiLogFileBlock *block)
{
	Reader *blockReader;
	FileFragment *stream;
	bool found;
	char *schema;
	MemoryContext oldMcxt;
	InternalRecordWrapper record;
	HudiLogFileDataBlock *dataBlock = (HudiLogFileDataBlock *) block;

	oldMcxt = MemoryContextSwitchTo(reader->mergerMcxt);

	stream = palloc0(sizeof(FileFragment));
	stream->filePath = dataBlock->content;
	stream->format = AVRO_FILE_BLOCK;

	schema = (char *) logBlockGetSchema(block);
	blockReader = (Reader *) createFileReader(reader->readerMcxt, reader->columnDesc, reader->attrUsed,
											  false, stream, schema, dataBlock->length, strlen(schema));

	initRecord(&record, reader->mergeProvider->recordDesc, list_length(reader->columnDesc));
	while (true)
	{
		found = blockReader->Next(blockReader, (InternalRecord *) &record);
		if (!found)
			break;

		reader->mergeProvider->CombineAndUpdate(reader->mergeProvider, &record);
	}

	cleanupRecord(&record);
	blockReader->Close(blockReader);
	MemoryContextSwitchTo(oldMcxt);
}

static KryoDatum
deserializeDeleteRecords(HudiMergedLogfileRecordReader *reader, int version, char *content, int length)
{
	Kryo      *kryo;
	KryoInput *kryoInput;
	KryoDatum  datum;

	kryo = createKryo();
	kryoInput = createKryoInput((uint8_t *) content, length);

	elog(DEBUG1, "deserialize deleteRecords version :%d", version);
	datum = readClassAndObject(kryo, kryoInput);

	destroyKryo(kryo);
	return datum;
}

static List *
parseKeyValue(List *result, char *pair, char *recordKey)
{
	int       keyLen;
	int       valueLen;
	char     *sep = strchr(pair, ':');
	KeyValue *data = palloc0(sizeof(KeyValue));

	if (sep == NULL)
		elog(ERROR, "invalid record key %s: '%s' missing delimiter ':'", recordKey, pair);

	keyLen = sep - pair;

	if (keyLen == 0)
		elog(ERROR, "invalid record key %s: '%s' missing key before ':'", recordKey, pair);

	valueLen = strlen(pair) - keyLen + 1;
	if (valueLen == 2)
		elog(ERROR, "invalid record key %s: '%s' missing value after ':'", recordKey, pair);

	data->key = pnstrdup(pair, keyLen);
	data->value = pnstrdup(sep + 1, valueLen);

	return lappend(result, data);
}

/* id:1,name:1 */
static List *
extractRecordKeyValues(char *strRecordKey, int strSize, int keySize)
{
	char *pair;
	char *ctx;
	char *dupStr;
	char *start;
	char *sep;
	List *result = NIL;

	dupStr = pnstrdup(strRecordKey, strSize);
	sep = strchr(dupStr, ':');

	if (keySize == 1 && sep == NULL)
	{
		KeyValue *data = palloc0(sizeof(KeyValue));

		data->value = pnstrdup(strRecordKey, strSize);
		result = lappend(result, data);

		pfree(dupStr);
		return result;
	}

	start = dupStr;
	for (pair = strtok_r(start, ",", &ctx); pair; pair = strtok_r(NULL, ",", &ctx))
		result = parseKeyValue(result, pair, strRecordKey);

	pfree(dupStr);
	return result;
}


static void
populateDeleteRecord(MergeProvider *provider,
					 InternalRecordWrapper *recordWrapper,
					 KryoDatum recordKey,
					 KryoDatum orderingVal)
{
	int i;
	int colIndex;
	ListCell *lc;
	List *recordKeyFields;
	InternalRecordDesc *recordDesc = provider->recordDesc;
	KryoStringDatum *strRecordKey = kryoDatumToString(recordKey);

	recordKeyFields = extractRecordKeyValues(strRecordKey->s, strRecordKey->size, provider->recordDesc->nKeys);
	for (i = 0; i < recordDesc->nColumns; i++)
	{
		recordWrapper->record.values[i] = PointerGetDatum(NULL);
		recordWrapper->record.nulls[i] = true;
	}

	foreach_with_count(lc, recordKeyFields, i)
	{
		KeyValue *field = (KeyValue *) lfirst(lc);
		colIndex = recordDesc->keyIndexes[i];

		recordWrapper->record.values[colIndex] = createDatumByText(recordDesc->attrType[colIndex], field->value);
		recordWrapper->record.nulls[colIndex] = false;
	}

	if (provider->preCombineFieldIndex >= 0)
	{
		recordWrapper->record.values[provider->preCombineFieldIndex] =
						orderingVal ? createDatumByValue(provider->preCombineFieldType, orderingVal) : DatumGetInt64(0);
		recordWrapper->record.nulls[provider->preCombineFieldIndex] = false;
	}

	freeKeyValueList(recordKeyFields);
}

static void
processV1DeleteRecords(HudiMergedLogfileRecordReader *reader, KryoDatum hoodieKeys)
{
	int i;
	InternalRecordWrapper record;
	MemoryContext oldContext;

	initRecord(&record, reader->mergeProvider->recordDesc, list_length(reader->columnDesc));
	for (i = 0; i < kryoDatumToArray(hoodieKeys)->size; i++)
	{
		KryoDatum hoodieKey = kryoDatumToArray(hoodieKeys)->elements[i];
		KryoDatum recordKey = kryoDatumToArray(hoodieKey)->elements[1];

		MemoryContextReset(reader->deleteRowMcxt);

		oldContext = MemoryContextSwitchTo(reader->deleteRowMcxt);

		populateDeleteRecord(reader->mergeProvider, &record, recordKey, NULL);
		reader->mergeProvider->UpdateOnDelete(reader->mergeProvider, &record);

		MemoryContextSwitchTo(oldContext);
	}

	cleanupRecord(&record);
}

static void
processV2DeleteRecords(HudiMergedLogfileRecordReader *reader, KryoDatum deleteRecords)
{
	int i;
	InternalRecordWrapper record;
	MemoryContext oldContext;

	initRecord(&record, reader->mergeProvider->recordDesc, list_length(reader->columnDesc));
	for (i = 0; i < kryoDatumToArray(deleteRecords)->size; i++)
	{
		KryoDatum deleteRecord = kryoDatumToArray(deleteRecords)->elements[i];
		KryoDatum hoodieKey = kryoDatumToArray(deleteRecord)->elements[0];

		KryoDatum recordKey = kryoDatumToArray(hoodieKey)->elements[1];
		KryoDatum orderingVal = kryoDatumToArray(deleteRecord)->elements[1];

		MemoryContextReset(reader->deleteRowMcxt);

		oldContext = MemoryContextSwitchTo(reader->deleteRowMcxt);

		populateDeleteRecord(reader->mergeProvider, &record, recordKey, orderingVal);
		reader->mergeProvider->UpdateOnDelete(reader->mergeProvider, &record);

		MemoryContextSwitchTo(oldContext);
	}
}

static void
processDeleteBlock(HudiMergedLogfileRecordReader *reader, HudiLogFileBlock *block)
{
	int version;
	int length;
	KryoDatum datum;
	MemoryContext oldContext;
	HudiLogFileDataBlock *dataBlock = (HudiLogFileDataBlock *) block;

	version = *((int *) (dataBlock->content));
#ifndef WORDS_BIGENDIAN
	version = reverse32(version);
#endif

	length = *((int *) (dataBlock->content + 4));
#ifndef WORDS_BIGENDIAN
	length = reverse32(length);
#endif

	MemoryContextReset(reader->deleteBlockMcxt);
	oldContext = MemoryContextSwitchTo(reader->deleteBlockMcxt);

	datum = deserializeDeleteRecords(reader, version, dataBlock->content + 8, length);
	version == 1 ? processV1DeleteRecords(reader, datum) : processV2DeleteRecords(reader, datum);

	MemoryContextSwitchTo(oldContext);
}

static void
processQueuedBlocks(HudiMergedLogfileRecordReader *reader)
{
	while (reader->queuedBlocks != NIL)
	{
		HudiLogFileBlock *logBlock = (HudiLogFileBlock *) linitial(reader->queuedBlocks);

		reader->queuedBlocks = list_delete_first(reader->queuedBlocks);

		switch(logBlock->type)
		{
			case AVRO_BLOCK:
				processDataBlock(reader, logBlock);
				break;
			case DELETE_BLOCK:
				processDeleteBlock(reader, logBlock);
				break;
			default:
				/* must be a corrupt block, free resources */
				Assert(logBlock->type == CORRUPTED_BLOCK);
				break;
		}

		freeLogBlock(logBlock);
	}
}

static void
rollbackBlocks(HudiMergedLogfileRecordReader *reader, const char *targetInstantTime)
{
	ListCell *lc;
	ListCell *nextlc;
	ListCell *prevlc;
	const char *instantTime;

	prevlc = NULL;
	for (lc = list_head(reader->queuedBlocks); lc != NULL; lc = nextlc)
	{
		HudiLogFileBlock *block = lfirst(lc);
		instantTime = logBlockGetInstantTime(block);

		// nextlc = lnext(lc);
		nextlc = lnext(reader->queuedBlocks, lc);
		if ((block->type == CORRUPTED_BLOCK) ||
			(instantTime && strcmp(targetInstantTime, instantTime) == 0))
		{
			freeLogBlock(block);
			// reader->queuedBlocks = list_delete_cell(reader->queuedBlocks, lc, prevlc);
			reader->queuedBlocks = list_delete_cell(reader->queuedBlocks, lc);
		}
		else
			prevlc = lc;
	}
}

static bool
scanBlock(HudiMergedLogfileRecordReader *reader, HudiLogFileBlock *block)
{
	const char *instantTime = logBlockGetInstantTime(block);

	/* hit a block with instant time greater than should be processed, stop processing further */
	if (block->type != CORRUPTED_BLOCK && strcmp(instantTime, reader->instantTime) > 0)
	{
		freeLogBlock(block);
		return false;
	}

	/* hit an uncommitted block possibly from a failed write, move to the next one and skip processing this one */
	if (block->type != CORRUPTED_BLOCK && block->type != COMMAND_BLOCK)
	{
		if (!containsOrBeforeInstantTime(reader->tableOptions->completedInstants,
										 reader->tableOptions->firstNonSavepointCommit,
										 instantTime) ||
			containsInstantTime(reader->tableOptions->inflightInstants, instantTime))
		{
			freeLogBlock(block);
			return true;
		}
	}

	switch (block->type)
	{
		case AVRO_BLOCK:
		case DELETE_BLOCK:
			elog(DEBUG1, "reading a \"%s\" block from file at instant %s", logBlockGetTypeName(block), instantTime);

			if (isNewInstantBlock(instantTime, reader->queuedBlocks))
				processQueuedBlocks(reader);

			reader->queuedBlocks = lappend(reader->queuedBlocks, block);
			break;
		case COMMAND_BLOCK:
			{
				int numBlocksRolledBack = list_length(reader->queuedBlocks);
				int queueSizeBeforeRollback = list_length(reader->queuedBlocks);
				const char *targetInstantTime = logBlockGetTargetInstantTime(block);

				rollbackBlocks(reader, targetInstantTime);

				numBlocksRolledBack = queueSizeBeforeRollback - list_length(reader->queuedBlocks);

				elog(DEBUG1, "number of applied rollback blocks %d by targetInstant %s",
							  numBlocksRolledBack, targetInstantTime);

				freeLogBlock(block);
			}
			break;
		case CORRUPTED_BLOCK:
			reader->queuedBlocks = lappend(reader->queuedBlocks, block);
			break;
		default:
			elog(ERROR, "block type %d is not supported yet", block->type);
	}

	return true;
}

bool
mergedLogfileRecordReaderNext(HudiMergedLogfileRecordReader *reader, InternalRecord *record)
{
	return reader->mergeProvider->Next(reader->mergeProvider, record);
}

bool
mergedLogfileContains(HudiMergedLogfileRecordReader *reader,
					  InternalRecord *record,
					  InternalRecord **newRecord,
					  bool *isDeleted)
{
	return reader->mergeProvider->Contains(reader->mergeProvider, record, newRecord, isDeleted);
}
