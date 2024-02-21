#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "gopher/gopher.h"
#include "src/provider/common/file_reader.h"
#include "hudi_merged_logfile_record_reader.h"
#include "hudi_deltalog_filter.h"
#include "hudi_task_reader.h"

typedef struct PartitionValueDatum
{
	int     index;
	Datum   value;
} PartitionValueDatum;

static void printDebugLog(int taskId, FileScanTask *task, ExternalTableMetadata *tableOptions);
static void projectRequiredColumns(List *recordKeyFields,
								   const char *preCombineField,
								   List *datafileTupleDesc,
								   bool *attrUsed);

static Reader *
createLogFilter(MemoryContext mcxt,
				List *datafileDesc,
				bool *attrUsed,
				gopherFS gopherFilesystem,
				int64 datafileStart,
				int64 dataFileLength,
				FileFragment *dataFile,
				List *deltaLogs,
				const char *instantTime,
				ExternalTableMetadata *tableOptions)
{
	Reader *filter = NULL;

	/* log files only */
	if (dataFile == NULL)
	{
		filter = (Reader *) createDeltaLogFilter(mcxt,
												 datafileDesc,
												 attrUsed,
												 filter,
												 gopherFilesystem,
												 deltaLogs,
												 instantTime,
												 tableOptions);
		return filter;
	}

	/* data files only */
	filter = (Reader *) createFileReader(mcxt, datafileDesc, attrUsed, true,
										 dataFile, gopherFilesystem, datafileStart,
										 datafileStart + dataFileLength);

	if (list_length(deltaLogs) == 0 || list_length(tableOptions->recordKeyFields) == 0)
	{
		elog(DEBUG1, "create hudi base file only reader");
		return filter;
	}

	return (Reader *) createDeltaLogFilter(mcxt,
										   datafileDesc,
										   attrUsed,
										   filter,
										   gopherFilesystem,
										   deltaLogs,
										   instantTime,
										   tableOptions);
}

static List *
createPartitionValueDatum(ExternalTableMetadata *tableOptions,
						  List *columnDesc,
						  bool *attrUsed,
						  FileFragment *dataFile,
						  List *deltaLogs)
{
	int i;
	ListCell *lco;
	ListCell *lci;
	char *filePath;
	List *partitionSpecs;
	List *result = NIL;

	if (!tableOptions->isTablePartitioned ||
		!tableOptions->extractPartitionValueFromPath)
		return result;

	if (list_length(tableOptions->partitionKeyFields) == 0)
		return result;

	if (dataFile != NULL)
	{
		filePath = dataFile->filePath;
	}
	else
	{
		FileFragment *logFile = list_nth(deltaLogs, 0);
		filePath = logFile->filePath;
	}

	partitionSpecs = extractPartitionKeyValues(filePath,
											   tableOptions->partitionKeyFields,
											   tableOptions->hiveStylePartitioningEnabled);

	foreach(lco, partitionSpecs)
	{
		KeyValue *keyValue = (KeyValue *) lfirst(lco);
		foreach_with_count(lci, columnDesc, i)
		{
			FieldDescription *field = (FieldDescription *) lfirst(lci);

			if (pg_strcasecmp(keyValue->key, field->name) == 0 && attrUsed[i])
			{
				PartitionValueDatum *valueDatum = palloc(sizeof(PartitionValueDatum));

				valueDatum->index = i;
				valueDatum->value = createDatumByText(field->typeOid, keyValue->value);

				result = lappend(result, valueDatum);
			}
		}
	}

	freeKeyValueList(partitionSpecs);

	return result;
}

Reader *
createHudiTaskReader(void *args)
{
	Reader *filter;
	ReaderInitInfo *info = (ReaderInitInfo *) args;

	HudiTaskReader *reader = palloc0(sizeof(HudiTaskReader));
	reader->fileScanTask = info->fileScanTask;
	reader->taskId = info->taskId;
	reader->tableOptions = info->tableOptions;

	printDebugLog(info->taskId, info->fileScanTask, reader->tableOptions);

	if (list_length(info->fileScanTask->deletes) > 0)
		projectRequiredColumns(info->tableOptions->recordKeyFields,
							   info->tableOptions->preCombineField,
							   info->datafileDesc,
							   info->attrUsed);

	filter = createLogFilter(info->mcxt, info->datafileDesc, info->attrUsed, info->gopherFilesystem,
						  info->fileScanTask->start, info->fileScanTask->length,
						  info->fileScanTask->dataFile, info->fileScanTask->deletes,
						  info->fileScanTask->instantTime, reader->tableOptions);
	reader->dataReader = filter;
	reader->attrUsed = info->attrUsed;

	reader->partitionDatums = createPartitionValueDatum(reader->tableOptions,
														info->datafileDesc,
														info->attrUsed,
														info->fileScanTask->dataFile,
														info->fileScanTask->deletes);

	return (Reader *) reader;
}

static void
projectPartitionColumnValue(HudiTaskReader *reader, InternalRecord *record)
{
	ListCell *lc;

	foreach(lc, reader->partitionDatums)
	{
		PartitionValueDatum *valueDatum = (PartitionValueDatum *) lfirst(lc);

		if (reader->attrUsed[valueDatum->index])
		{
			record->values[valueDatum->index] = valueDatum->value;
			record->nulls[valueDatum->index] = false;
		}
	}
}

bool
hudiTaskReaderNext(Reader *reader, InternalRecord *record)
{
	bool hasNext;
	HudiTaskReader *hudiReader = (HudiTaskReader *) reader;

	if (hudiReader == NULL)
		return false;

	hasNext = hudiReader->dataReader->Next(hudiReader->dataReader, record);
	if (!hasNext)
		return false;

	projectPartitionColumnValue(hudiReader, record);

	return true;
}

void
hudiTaskReaderClose(Reader *reader)
{
	HudiTaskReader *hudiReader = (HudiTaskReader *) reader;

	if (hudiReader == NULL)
		return;

	hudiReader->dataReader->Close(hudiReader->dataReader);
	elog(DEBUG1, "close hudi reader task [%d]", hudiReader->taskId);

	pfree(hudiReader->fileScanTask);
	pfree(hudiReader);
}

static void
projectRequiredColumn(List *datafileTupleDesc, bool *attrUsed, const char *colName)
{
	int i;
	ListCell *lc;

	foreach_with_count(lc, datafileTupleDesc, i)
	{
		FieldDescription *fieldDesc = (FieldDescription *) lfirst(lc);

		if (attrUsed[i] == true)
			continue;

		if (pg_strcasecmp(fieldDesc->name, colName) == 0)
		{
			attrUsed[i] = true;
			break;
		}
	}
}

static void
projectRequiredColumns(List *recordKeyFields,
					   const char *preCombineField,
					   List *datafileTupleDesc,
					   bool *attrUsed)
{
	ListCell *lc;

	foreach(lc, recordKeyFields)
	{
		char *colName = strVal(lfirst(lc));

		projectRequiredColumn(datafileTupleDesc, attrUsed, colName);
	}

	if (preCombineField != NULL)
		projectRequiredColumn(datafileTupleDesc, attrUsed, preCombineField);
}

static void
printDebugLog(int taskId, FileScanTask *task, ExternalTableMetadata *tableOptions)
{
	ListCell *lc;

	if (task->dataFile != NULL)
		elog(DEBUG1, "[%d] datafile \"%s\" [%ld %ld]",
				taskId, task->dataFile->filePath, task->start, task->start + task->length);

	foreach(lc, tableOptions->recordKeyFields)
	{
		char *keyName = strVal(lfirst(lc));
		elog(DEBUG1, "record key \"%s\"", keyName);
	}

	elog(DEBUG1, "isTablePartitioned \"%d\" extractPartitionValueFromPath \"%d\" hiveStylePartitioningEnabled \"%d\"",
			tableOptions->isTablePartitioned,
			tableOptions->extractPartitionValueFromPath,
			tableOptions->hiveStylePartitioningEnabled);

	foreach(lc, tableOptions->partitionKeyFields)
	{
		char *keyName = strVal(lfirst(lc));
		elog(DEBUG1, "partition key \"%s\"", keyName);
	}

	foreach(lc, task->deletes)
	{
		FileFragment *deltaLogFile = (FileFragment *) lfirst(lc);
		elog(DEBUG1, "[%d] hudi delta logfile \"%s\"", taskId, deltaLogFile->filePath);
	}
}
