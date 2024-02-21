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

#include "src/provider/common/kryo.h"
#include "src/provider/common/kryo_input.h"

typedef struct LogRecordHashEntry {
	InternalRecordWrapper *recordKey;
	InternalRecordWrapper *recordValue;
} LogRecordHashEntry;

static void processQueuedBlocks(HudiMergedLogfileRecordReader *reader);
static bool scanBlock(HudiMergedLogfileRecordReader *reader, HudiLogFileBlock *block);

static InternalRecordDesc *
createInternalRecordDesc(MemoryContext mcxt,
						 List *columnDesc,
						 List *recordKeyFields,
						 char *preCombineField,
						 int *preCombineFieldIndex,
						 Oid *preCombineFieldType)
{
	int i;
	ListCell *lc;
	HASHCTL  hashCtl;
	bool found = false;

	InternalRecordDesc *result = palloc0(sizeof(InternalRecordDesc));

	result->nColumns = list_length(columnDesc);
	result->nKeys = list_length(recordKeyFields);
	result->keyIndexes = createRecordKeyIndexes(recordKeyFields, columnDesc);
	result->attrType = palloc(sizeof(Oid) * list_length(columnDesc));

	foreach_with_count(lc, columnDesc, i)
	{
		FieldDescription *entry = (FieldDescription *) lfirst(lc);
		result->attrType[i] = entry->typeOid;

		if (preCombineField && !found && pg_strcasecmp(preCombineField, entry->name) == 0)
		{
			*preCombineFieldIndex = i;
			*preCombineFieldType = entry->typeOid;
			found = true;
		}
	}

	MemSet(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(InternalRecordWrapper *);
	hashCtl.entrysize = sizeof(LogRecordHashEntry);
	hashCtl.hash = recordHash;
	hashCtl.match = recordMatch;
	hashCtl.hcxt = mcxt;
	result->hashTab = hash_create("HudiMergeLogTable", 10000 * 30, &hashCtl,
								   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	return result;
}

static void
destroyInternalRecordDesc(InternalRecordDesc *recordDesc)
{
	if (recordDesc->keyIndexes)
		pfree(recordDesc->keyIndexes);

	if (recordDesc->attrType)
		pfree(recordDesc->attrType);

	if (recordDesc->hashTab)
		hash_destroy(recordDesc->hashTab);

	pfree(recordDesc);
}

HudiMergedLogfileRecordReader *
createMergedLogfileRecordReader(MemoryContext readerMcxt,
								List *columnDesc,
								bool *attrUsed,
								const char *instantTime,
								gopherFS gopherFilesystem,
								List *logfiles,
								ExternalTableMetadata *tableOptions)
{
	HudiMergedLogfileRecordReader *reader = palloc0(sizeof(HudiMergedLogfileRecordReader));

	reader->logfiles = logfiles;
	reader->columnDesc = columnDesc;
	reader->attrUsed = attrUsed;
	reader->instantTime = instantTime;
	reader->gopherFilesystem = gopherFilesystem;
	reader->readerMcxt = readerMcxt;
	reader->preCombineFieldType = InvalidOid;
	reader->preCombineFieldIndex = -1;

	reader->mergerMcxt = AllocSetContextCreate(CurrentMemoryContext,
											   "MergedLogRecordReaderContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	reader->deleteMcxt = AllocSetContextCreate(CurrentMemoryContext,
											   "DeleteLogRecordReaderContext",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);
	reader->tableOptions = tableOptions;
	reader->recordDesc = createInternalRecordDesc(reader->mergerMcxt,
												  columnDesc,
												  tableOptions->recordKeyFields,
												  tableOptions->preCombineField,
												  &reader->preCombineFieldIndex,
												  &reader->preCombineFieldType);

	performScan(reader);

	hash_seq_init(&reader->hashTabSeqStatus, reader->recordDesc->hashTab);
	reader->isRegistered = true;

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
	if (reader->isRegistered)
		hash_seq_term(&reader->hashTabSeqStatus);

	destroyInternalRecordDesc(reader->recordDesc);
	MemoryContextDelete(reader->mergerMcxt);
	MemoryContextDelete(reader->deleteMcxt);
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
combineAndUpdateValue(HTAB *hashTab, InternalRecordWrapper *recordWrapper)
{
	bool found;
	LogRecordHashEntry *entry;

	entry = (LogRecordHashEntry *) hash_search(hashTab, &recordWrapper, HASH_FIND, NULL);
	if (entry)
	{
		hash_search(hashTab, &recordWrapper, HASH_REMOVE, &found);
		Assert(found);
	}

	entry = (LogRecordHashEntry *) hash_search(hashTab, &recordWrapper, HASH_ENTER, &found);
	Assert(!found);

	entry->recordValue = recordWrapper;
}

static void
processDataBlock(HudiMergedLogfileRecordReader *reader, HudiLogFileBlock *block)
{
	Reader *blockReader;
	FileFragment *stream;
	char *schema;
	MemoryContext oldMcxt;
	InternalRecordWrapper *recordWrapper;
	HudiLogFileDataBlock *dataBlock = (HudiLogFileDataBlock *) block;

	oldMcxt = MemoryContextSwitchTo(reader->mergerMcxt);

	stream = palloc0(sizeof(FileFragment));
	stream->filePath = dataBlock->content;
	stream->format = AVRO_FILE_BLOCK;

	schema = (char *) logBlockGetSchema(block);
	blockReader = (Reader *) createFileReader(reader->readerMcxt, reader->columnDesc, reader->attrUsed,
											  false, stream, schema, dataBlock->length, strlen(schema));

	while (true)
	{
		recordWrapper = createInternalRecordWrapper(reader->recordDesc, list_length(reader->columnDesc));
		if (blockReader->Next(blockReader, (InternalRecord *) recordWrapper))
		{
			combineAndUpdateValue(reader->recordDesc->hashTab, recordWrapper);
			deepCopyRecord(recordWrapper);
		}
		else
		{
			destroyInternalRecordWrapper(recordWrapper);
			break;
		}
	}

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

static InternalRecordWrapper *
createDeleteRecord(HudiMergedLogfileRecordReader *reader, List *recordKeyFields, KryoDatum orderingVal)
{
	int i;
	int colIndex;
	ListCell *lc;
	MemoryContext oldMcxt;
	InternalRecordDesc *recordDesc = reader->recordDesc;

	oldMcxt = MemoryContextSwitchTo(reader->mergerMcxt);

	InternalRecordWrapper *recordWrapper = createInternalRecordWrapper(reader->recordDesc,
																	   list_length(reader->columnDesc));
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

	if (reader->preCombineFieldIndex >= 0)
	{
		recordWrapper->record.values[reader->preCombineFieldIndex] =
						orderingVal ? createDatumByValue(reader->preCombineFieldType, orderingVal) : DatumGetInt64(0);
		recordWrapper->record.nulls[reader->preCombineFieldIndex] = false;
	}

	SET_RECORD_IS_DELETED(recordWrapper);
	MemoryContextSwitchTo(oldMcxt);

	return recordWrapper;
}

static bool
greaterThan(Oid type, Datum datum1, Datum datum2)
{
	/* TODO compare operator */
	if (type != TEXTOID)
		return DatumGetInt64(datum1) > DatumGetInt64(datum2);

	return DatumGetBool(DirectFunctionCall2Coll(text_gt,
												DEFAULT_COLLATION_OID,
												PointerGetDatum(datum1),
												PointerGetDatum(datum2)));
}

static void
processDeleteRecord(HudiMergedLogfileRecordReader *reader,
					KryoDatum recordKey,
					KryoDatum partitionPath,
					KryoDatum orderingVal)
{
	bool found;
	List *recordKeyFields;
	Datum deleteOrderingVal;
	Datum curOrderingVal;
	LogRecordHashEntry *entry;
	InternalRecordWrapper *recordWrapper;
	KryoStringDatum *strRecordKey = kryoDatumToString(recordKey);

	recordKeyFields = extractRecordKeyValues(strRecordKey->s, strRecordKey->size, reader->recordDesc->nKeys);
	recordWrapper = createDeleteRecord(reader, recordKeyFields, orderingVal);
	freeKeyValueList(recordKeyFields);

	entry = hash_search(reader->recordDesc->hashTab, &recordWrapper, HASH_FIND, NULL);
	if (entry != NULL)
	{
		bool choosePrev;

		if (reader->preCombineFieldIndex >= 0)
		{
			deleteOrderingVal = recordWrapper->record.values[reader->preCombineFieldIndex];
			curOrderingVal = entry->recordValue->record.values[reader->preCombineFieldIndex];
		}

		choosePrev = reader->preCombineFieldIndex >= 0 && 
					 DatumGetInt64(deleteOrderingVal) != 0 &&
					 greaterThan(reader->preCombineFieldType, curOrderingVal, deleteOrderingVal);

		if (choosePrev)
		{
			/* the DELETE message is obsolete if the old message has greater orderingVal */
			return;
		}

		hash_search(reader->recordDesc->hashTab, &recordWrapper, HASH_REMOVE, &found);
		Assert(found);
	}

	entry = hash_search(reader->recordDesc->hashTab, &recordWrapper, HASH_ENTER, &found);
	Assert(!found);
	entry->recordValue = recordWrapper;
}

static void
processV1DeleteRecords(HudiMergedLogfileRecordReader *reader, KryoDatum hoodieKeys)
{
	int i;

	for (i = 0; i < kryoDatumToArray(hoodieKeys)->size; i++)
	{
		KryoDatum hoodieKey = kryoDatumToArray(hoodieKeys)->elements[i];
		KryoDatum recordKey = kryoDatumToArray(hoodieKey)->elements[1];
		KryoDatum partitionPath = kryoDatumToArray(hoodieKey)->elements[0];

		processDeleteRecord(reader, recordKey, partitionPath, NULL);
	}
}

static void
processV2DeleteRecords(HudiMergedLogfileRecordReader *reader, KryoDatum deleteRecords)
{
	int i;

	for (i = 0; i < kryoDatumToArray(deleteRecords)->size; i++)
	{
		KryoDatum deleteRecord = kryoDatumToArray(deleteRecords)->elements[i];
		KryoDatum hoodieKey = kryoDatumToArray(deleteRecord)->elements[0];

		KryoDatum recordKey = kryoDatumToArray(hoodieKey)->elements[1];
		KryoDatum partitionPath = kryoDatumToArray(hoodieKey)->elements[0];

		KryoDatum orderingVal = kryoDatumToArray(deleteRecord)->elements[1];
		processDeleteRecord(reader, recordKey, partitionPath, orderingVal);
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

	MemoryContextReset(reader->deleteMcxt);
	oldContext = MemoryContextSwitchTo(reader->deleteMcxt);

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
	LogRecordHashEntry *entry;
	InternalRecordDesc *recordDesc = reader->recordDesc;

again:
	entry = (LogRecordHashEntry *) hash_seq_search(&reader->hashTabSeqStatus);
	if (entry)
	{
		int i;

		if (RECORD_IS_DELETED(entry->recordValue))
			goto again;

		if (RECORD_IS_SKIPPED(entry->recordValue))
			goto again;

		for (i = 0; i < recordDesc->nColumns; i++)
		{
			record->values[i] = entry->recordValue->record.values[i];
			record->nulls[i] = entry->recordValue->record.nulls[i];
		}

		return true;
	}

	Assert(reader->isRegistered);
	reader->isRegistered = false;

	return false;
}

bool
mergedLogfileContains(HudiMergedLogfileRecordReader *reader,
					  InternalRecord *record,
					  InternalRecord **newRecord,
					  bool *isDeleted)
{
	LogRecordHashEntry    *entry;
	InternalRecordWrapper  recordWrapper;
	InternalRecordWrapper *pRecordWrapper = &recordWrapper;

	*isDeleted = false;
	recordWrapper.record = *record;
	recordWrapper.recordDesc = reader->recordDesc;
	entry = (LogRecordHashEntry *) hash_search(reader->recordDesc->hashTab, &pRecordWrapper, HASH_FIND, NULL);
	if (entry == NULL)
		return false;

	if (RECORD_IS_DELETED(entry->recordValue))
	{
		*isDeleted = true;
		return true;
	}

	*newRecord = &entry->recordValue->record;
	SET_RECORD_IS_SKIPPED(entry->recordValue);
	return true;
}
