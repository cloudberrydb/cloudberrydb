#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "gopher/gopher.h"
#include "src/provider/common/file_reader.h"
#include "iceberg_equality_filter.h"

typedef struct EqualityHashEntry {
	InternalRecordWrapper *recordKey;
	InternalRecordWrapper *recordValue;
} EqualityHashEntry;

static bool equalityFilterNext(Reader *filter, InternalRecord *record);
static void equalityFilterClose(Reader *filter);
static List *readEqualityDeletes(MemoryContext filterMcxt,
								 MemoryContext readerMcxt,
								 List *datafileDesc,
								 gopherFS gopherFilesystem,
								 List *deletes);
static bool deletesSetsContains(EqualityFilter *filter, InternalRecord *record);

static Reader methods = {
	NULL,
	equalityFilterNext,
	equalityFilterClose,
};

EqualityFilter *
createEqualityFilter(MemoryContext readerMcxt,
					 List *datafileDesc,
					 Reader *dataReader,
					 gopherFS gopherFilesystem,
					 List *deletes)
{
	EqualityFilter *filter = palloc0(sizeof(EqualityFilter));

	elog(DEBUG1, "create iceberg equality filter");

	filter->base = methods;
	filter->dataReader = dataReader;
	filter->mcxt = AllocSetContextCreate(CurrentMemoryContext,
										 "EqualityFilterContext",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	filter->deletesSets = readEqualityDeletes(filter->mcxt, readerMcxt, datafileDesc, gopherFilesystem, deletes);
	return filter;
}

static bool
equalityFilterNext(Reader *filter, InternalRecord *record)
{
	bool isDeleted;
	EqualityFilter *equalityFilter = (EqualityFilter *) filter;

	while(equalityFilter->dataReader->Next(equalityFilter->dataReader, record))
	{
		isDeleted = deletesSetsContains(equalityFilter, record);
		if (!isDeleted)
			return true;
	}

	return false;
}

static void
equalityFilterClose(Reader *filter)
{
	EqualityFilter *equalityFilter = (EqualityFilter *) filter;

	equalityFilter->dataReader->Close(equalityFilter->dataReader);
	MemoryContextDelete(equalityFilter->mcxt);
	pfree(equalityFilter);

	elog(DEBUG1, "close iceberg equality filter");
}

static bool
deletesSetsContains(EqualityFilter *filter, InternalRecord *record)
{
	bool           found = false;
	ListCell      *lc;
	MemoryContext  oldMcxt;
	InternalRecordWrapper  recordWrapper;
	InternalRecordWrapper *pRecordWrapper = &recordWrapper;
	EqualityHashEntry *entry;

	recordWrapper.record = *record;

	oldMcxt = MemoryContextSwitchTo(filter->mcxt);

	foreach(lc, filter->deletesSets)
	{
		InternalRecordDesc *deletesSet = (InternalRecordDesc *) lfirst(lc);
		recordWrapper.recordDesc = deletesSet;

		entry = (EqualityHashEntry *) hash_search(deletesSet->hashTab, &pRecordWrapper, HASH_FIND, NULL);
		if (entry)
		{
			found = true;
			break;
		}
	}

	MemoryContextSwitchTo(oldMcxt);

	return found;
}

static HTAB *
createHashTable(MemoryContext mcxt, int64 totalRecords)
{
	HTAB    *hashTab;
	HASHCTL  hashCtl;

	MemSet(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(InternalRecordWrapper *);
	hashCtl.entrysize = sizeof(EqualityHashEntry);
	hashCtl.hash = recordHash;
	hashCtl.match = recordMatch;
	hashCtl.hcxt = mcxt;
	hashTab = hash_create("EqualityFilterTable", totalRecords, &hashCtl,
						   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	return hashTab;
}

static void
createDeletesReaderResources(MemoryContext mcxt,
							 List *datafileDesc,
							 FileFragment **deleteFile,
							 InternalRecordDesc **deletesSet,
							 List *deletes)
{
	int i;
	ListCell *lc;

	*deletesSet = palloc(sizeof(InternalRecordDesc));

	(*deleteFile) = list_nth(deletes, 0);

	(*deletesSet)->hashTab = createHashTable(mcxt, (*deleteFile)->recordCount);
	(*deletesSet)->nColumns = list_length(datafileDesc);
	(*deletesSet)->nKeys = list_length((*deleteFile)->eqColumnNames);
	(*deletesSet)->keyIndexes = createRecordKeyIndexes((*deleteFile)->eqColumnNames, datafileDesc);
	(*deletesSet)->attrType = palloc0(sizeof(Oid) * list_length(datafileDesc));
	(*deletesSet)->attrUsed = createDeleteProjectionColumns((*deleteFile)->eqColumnNames, datafileDesc);

	foreach_with_count(lc, datafileDesc, i)
	{
		FieldDescription *entry = (FieldDescription *) lfirst(lc);
		(*deletesSet)->attrType[i] = entry->typeOid;
	}

	list_free_deep((*deleteFile)->eqColumnNames);
}

static void
destroyDeletesReaderResource(List **deletes)
{
	*deletes = list_delete_first(*deletes);
}

static List *
readEqualityDeletes(MemoryContext filterMcxt,
					MemoryContext readerMcxt,
					List *datafileDesc,
					gopherFS gopherFilesystem,
					List *deletes)
{
	Reader         *curReader;
	FileFragment   *deleteFile;
	List           *result = NIL;
	InternalRecordDesc *deletesSet;
	MemoryContext   oldMcxt;
	InternalRecordWrapper *recordWrapper;

	oldMcxt = MemoryContextSwitchTo(filterMcxt);

	createDeletesReaderResources(filterMcxt, datafileDesc, &deleteFile, &deletesSet, deletes);
	elog(DEBUG1, "scanning equalityDeletes file %s", deleteFile->filePath);
	curReader = (Reader *) createFileReader(readerMcxt, datafileDesc, deletesSet->attrUsed,
											true, deleteFile, gopherFilesystem, -1, -1);
	destroyDeletesReaderResource(&deletes);

	result = lappend(result, deletesSet);

	while (true)
	{
		recordWrapper = createInternalRecordWrapper(deletesSet, deletesSet->nColumns);
		if (curReader->Next(curReader, (InternalRecord *) recordWrapper))
		{
			bool found;
			EqualityHashEntry *hentry;
			hentry = (EqualityHashEntry *) hash_search(deletesSet->hashTab, &recordWrapper, HASH_ENTER, &found);
			if (!found)
				deepCopyRecord(recordWrapper);
		}
		else if (list_length(deletes) > 0)
		{
			destroyInternalRecordWrapper(recordWrapper);
			curReader->Close(curReader);

			createDeletesReaderResources(filterMcxt, datafileDesc, &deleteFile, &deletesSet, deletes);

			elog(DEBUG1, "scanning equalityDeletes file %s", deleteFile->filePath);
			curReader = (Reader *) createFileReader(readerMcxt, datafileDesc, deletesSet->attrUsed,
													true, deleteFile, gopherFilesystem, -1, -1);
			destroyDeletesReaderResource(&deletes);

			result = lappend(result, deletesSet);
		}
		else
		{
			destroyInternalRecordWrapper(recordWrapper);
			curReader->Close(curReader);
			break;
		}
	}

	MemoryContextSwitchTo(oldMcxt);
	return result;
}
