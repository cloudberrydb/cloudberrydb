#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "gopher/gopher.h"
#include "src/provider/common/file_reader.h"
#include "iceberg_position_filter.h"
#include "iceberg_equality_filter.h"
#include "iceberg_task_reader.h"

static void printDebugLog(int taskId,
						  const char *filePath,
						  int64 recordCount,
						  int64 startOffset,
						  int64 length,
						  List *posDeletes,
						  List *eqDeletes);
static void reOrganizeDeleteFiles(List *deletes, List **posDeletes, List **eqDeletes);
static void projectRequiredColumns(List *datafileTupleDesc, bool *attrUsed, List *deletes);

Reader *
createIcebergTaskReader(void *args)
{
	Reader *filter;
	List *posDeletes = NIL;
	List *eqDeletes = NIL;
	ReaderInitInfo *info = (ReaderInitInfo *) args;

	elog(DEBUG1, "create iceberg reader task [%d]", info->taskId);

	IcebergTaskReader *reader = palloc0(sizeof(IcebergTaskReader));
	reader->fileScanTask = info->fileScanTask;
	reader->taskId = info->taskId;

	reOrganizeDeleteFiles(info->fileScanTask->deletes, &posDeletes, &eqDeletes);

	printDebugLog(info->taskId,
				  info->fileScanTask->dataFile->filePath,
				  info->fileScanTask->dataFile->recordCount,
				  info->fileScanTask->start,
				  info->fileScanTask->length,
				  posDeletes,
				  eqDeletes);

	if (list_length(eqDeletes) > 0)
		projectRequiredColumns(info->datafileDesc, info->attrUsed, eqDeletes);

	filter = (Reader *) createFileReader(info->mcxt, info->datafileDesc, info->attrUsed, true,
										 info->fileScanTask->dataFile, info->gopherFilesystem,
										 info->fileScanTask->start,
										 info->fileScanTask->start + info->fileScanTask->length);

	list_free(info->fileScanTask->deletes);

	if (list_length(posDeletes) > 0)
		filter = (Reader *) createPositionFilter(info->mcxt, filter, info->gopherFilesystem,
												 info->fileScanTask->dataFile->filePath, posDeletes);

	if (list_length(eqDeletes) > 0)
		filter = (Reader *) createEqualityFilter(info->mcxt, info->datafileDesc,
												 filter, info->gopherFilesystem, eqDeletes);

	reader->dataReader = filter;
	return (Reader *) reader;
}

bool
icebergTaskReaderNext(Reader *reader, InternalRecord *record)
{
	IcebergTaskReader *icebergReader = (IcebergTaskReader *) reader;

	if (icebergReader == NULL)
		return false;

	return icebergReader->dataReader->Next(icebergReader->dataReader, record);
}

void
icebergTaskReaderClose(Reader *reader)
{
	IcebergTaskReader *icebergReader = (IcebergTaskReader *) reader;

	if (icebergReader == NULL)
		return;

	icebergReader->dataReader->Close(icebergReader->dataReader);
	elog(DEBUG1, "close iceberg reader task [%d]", icebergReader->taskId);
	pfree(icebergReader->fileScanTask);
	pfree(icebergReader);
}

static void
reOrganizeDeleteFiles(List *deletes, List **posDeletes, List **eqDeletes)
{
	ListCell *lc;

	foreach(lc, deletes)
	{
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);

		switch (deleteFile->content)
		{
			case POSITION_DELETES:
				*posDeletes = lappend(*posDeletes, deleteFile);
				break;
			case EQUALITY_DELETES:
				*eqDeletes = lappend(*eqDeletes, deleteFile);
				break;
			default:
				elog(ERROR, "unknown delete file type: %d", deleteFile->content);
		}
	}
}

static void
projectRequiredColumns(List *datafileTupleDesc, bool *attrUsed, List *deletes)
{
	int i = 0;
	ListCell *lco;
	ListCell *lci;
	FileFragment *deleteFile;
	List *eqColumnNames = NIL;

	foreach(lco, deletes)
	{
		deleteFile = (FileFragment *) lfirst(lco);
		eqColumnNames = list_concat_unique(eqColumnNames, deleteFile->eqColumnNames);
	}

	foreach(lco, eqColumnNames)
	{
		char *columnName = strVal(lfirst(lco));

		foreach_with_count(lci, datafileTupleDesc, i)
		{
			FieldDescription *fieldDesc = (FieldDescription *) lfirst(lci);

			if (attrUsed[i] == true)
				break;

			if (pg_strcasecmp(fieldDesc->name, columnName) == 0)
			{
				attrUsed[i] = true;
				break;
			}
		}
	}

	list_free(eqColumnNames);
}

static void
printDebugLog(int taskId,
			  const char *filePath,
			  int64 recordCount,
			  int64 startOffset,
			  int64 length,
			  List *posDeletes,
			  List *eqDeletes)
{
	ListCell *lc;

	elog(DEBUG1, "[%d] datafile \"%s\" [%ld %ld] records %ld",
			taskId, filePath, startOffset, startOffset + length, recordCount);

	foreach(lc, posDeletes)
	{
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);
		elog(DEBUG1, "[%d] position file \"%s\" records %ld", taskId, deleteFile->filePath, deleteFile->recordCount);
	}

	foreach(lc, eqDeletes)
	{
		FileFragment *deleteFile = (FileFragment *) lfirst(lc);
		elog(DEBUG1, "[%d] equality file \"%s\" records %ld", taskId, deleteFile->filePath, deleteFile->recordCount);
	}
}
