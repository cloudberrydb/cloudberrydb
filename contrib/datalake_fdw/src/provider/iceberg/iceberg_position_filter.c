#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "gopher/gopher.h"
#include "src/provider/common/file_reader.h"
#include "iceberg_position_filter.h"
#include "src/provider/common/delete_bitmap_c.h"
#include "src/provider/common/sorted_merge_c.h"

extern int icebergPostionDeletesThreshold;

static bool positionFilterNext(Reader *filter, InternalRecord *record);
static void positionFilterClose(Reader *filter);
static void *readPositionDeletes(MemoryContext mcxt,
								 List *schema,
								 gopherFS gopherFilesystem,
								 char *dataFilePath,
								 List *deletes);

static Reader methods = {
	NULL,
	positionFilterNext,
	positionFilterClose,
};

PositionFilter *
createPositionFilter(MemoryContext readerMcxt,
					 Reader *dataReader,
					 gopherFS gopherFilesystem,
					 char *dataFilePath,
					 List *deletes)
{
	ListCell *lc;
	int64 totalRecords;
	Reader *reader;
	List *readers = NIL;
	List *posDeletesSchema = createPositionDeletesDescription();
	PositionFilter *filter = palloc0(sizeof(PositionFilter));

	filter->base = methods;
	filter->dataReader = dataReader;

	totalRecords = getFileRecordCount(deletes);
	if (totalRecords < icebergPostionDeletesThreshold)
	{
		elog(DEBUG1, "create in-memory position filter");
		filter->deletesSet = readPositionDeletes(readerMcxt, posDeletesSchema, gopherFilesystem, dataFilePath, deletes);
		if (filter->deletesSet == NULL)
			filter->isEmptySet = true;

		list_free_deep(posDeletesSchema);
		return filter;
	}

	elog(DEBUG1, "create iceberg streaming position filter");

	filter->mcxt = AllocSetContextCreate(CurrentMemoryContext,
										 "PositionFilterContext",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
	/* create streaming filter */
	foreach(lc, deletes)
	{
		bool attrUsed[2] = {true, true};
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);

		elog(DEBUG1, "scanning position file %s", deleteFile->filePath);
		reader = (Reader *) createFileReader(filter->mcxt, posDeletesSchema, attrUsed, true,
											 deleteFile, gopherFilesystem, -1, -1);

		readers = lappend(readers, reader);
	}

	filter->sortedMerge = createSortedMerge(dataFilePath, readers);
	if (!sortedMergeNext(filter->sortedMerge, &filter->nextPosition))
		filter->isEmptyStream = true;

	list_free_deep(posDeletesSchema);
	list_free(readers);

	return filter;
}

static bool
inMemoryFilterNext(PositionFilter *filter, InternalRecord *record)
{
	bool isDeleted;

	while(filter->dataReader->Next(filter->dataReader, record))
	{
		isDeleted = bitmapContains(filter->deletesSet, (uint64) record->position);
		if (!isDeleted)
			return true;
	}

	return false;
}

static bool
streamingFilterNext(PositionFilter *filter, InternalRecord *record)
{
	int64 curPosition;

	if (filter->isEmptyStream)
		return filter->dataReader->Next(filter->dataReader, record);

	while(filter->dataReader->Next(filter->dataReader, record))
	{
		curPosition = record->position;

		if (curPosition < filter->nextPosition)
			return true;

		/* consume delete positions until the next is past the current position */
		bool keep = curPosition != filter->nextPosition;
		while (sortedMergeNext(filter->sortedMerge, &filter->nextPosition))
		{
			/* if any delete position matches the current position, discard */
			if (keep && curPosition == filter->nextPosition)
				keep = false;

			if (filter->nextPosition > curPosition)
				break;
		}

		if (keep)
			return true;
	}

	return false;
}

static bool
positionFilterNext(Reader *filter, InternalRecord *record)
{
	PositionFilter *positionFilter = (PositionFilter *) filter;

	if (positionFilter->deletesSet != NULL)
		return inMemoryFilterNext(positionFilter, record);
	else if (positionFilter->isEmptySet)
		return positionFilter->dataReader->Next(positionFilter->dataReader, record);
	else
		return streamingFilterNext(positionFilter, record);
}

static void
positionFilterClose(Reader *filter)
{
	PositionFilter *positionFilter = (PositionFilter *) filter;

	if (positionFilter->deletesSet != NULL)
		destroyBitmap(positionFilter->deletesSet);

	if (positionFilter->sortedMerge != NULL)
		sortedMergeClose(positionFilter->sortedMerge);

	positionFilter->dataReader->Close(positionFilter->dataReader);

	if (positionFilter->mcxt != NULL)
		MemoryContextDelete(positionFilter->mcxt);

	pfree(positionFilter);

	elog(DEBUG1, "close iceberg position filter");
}

static void
locationFilter(void **bitmap, char *dataFilePath, int dataFilePathLen, Datum filePathField, Datum positionField)
{
	int filePathSize = VARSIZE_ANY_EXHDR(filePathField);
	char *filePathName = VARDATA_ANY(filePathField);
	int64 position = DatumGetInt64(positionField);

	if (charSeqEquals(filePathName, filePathSize, dataFilePath, dataFilePathLen))
	{
		if (*bitmap == NULL)
			*bitmap = createBitmap();

		bitmapAdd(*bitmap, (uint64) position);
	}

	pfree(DatumGetPointer(filePathField));
}

static void *
readPositionDeletes(MemoryContext mcxt,
					List *posDeletesSchema,
					gopherFS gopherFilesystem,
					char *dataFilePath,
					List *deletes)
{
	bool nulls[2] = {false, false};
	Datum values[2] = {0, 0};
	Reader *curReader;
	void *result = NULL;
	bool attrUsed[2] = {true, true};
	int dataFilePathLen = strlen(dataFilePath);
	InternalRecord record = {values, nulls, 0};
	FileFragment *positionFile = list_nth(deletes, 0);

	elog(DEBUG1, "scanning position file %s", positionFile->filePath);
	curReader = (Reader *) createFileReader(mcxt, posDeletesSchema, attrUsed, true,
											positionFile, gopherFilesystem, -1, -1);
	deletes = list_delete_first(deletes);

	while (true)
	{
		if (curReader->Next(curReader, &record))
		{
			locationFilter(&result, dataFilePath, dataFilePathLen, values[0], values[1]);
		}
		else if (list_length(deletes) > 0)
		{
			curReader->Close(curReader);
			positionFile = list_nth(deletes, 0);
			elog(DEBUG1, "scanning position file %s", positionFile->filePath);
			curReader = (Reader *) createFileReader(mcxt, posDeletesSchema, attrUsed, true,
													positionFile, gopherFilesystem, -1, -1);
			deletes = list_delete_first(deletes);
		}
		else
		{
			curReader->Close(curReader);
			break;
		}
	}

	return result;
}
