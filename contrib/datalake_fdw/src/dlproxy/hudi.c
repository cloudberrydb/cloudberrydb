#include "postgres.h"

#include <jansson.h>
#include "datalake.h"
#include "nodes/nodes.h"
#include "headers.h"
#include "protocol.h"

#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"
#include "uriparser.h"

static FileFormat
convertFileFormat(const char *fileFormat)
{
	FileFormat result;

	if (pg_strcasecmp(fileFormat, "ORC") == 0)
		result = ORC;
	else if (pg_strcasecmp(fileFormat, "PARQUET") == 0)
		result = PARQUET;
	else if (pg_strcasecmp(fileFormat, "HFILE") == 0)
		result = HFILE;
	else if (pg_strcasecmp(fileFormat, "HLOG") == 0)
		result = HLOG;
	else
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported hudi file format \"%s\"", fileFormat)));

	return result;
}

static FileContent
convertFileContent(const char *fileContent)
{
	FileContent result;

	if (pg_strcasecmp(fileContent, "DATA") == 0)
		result = DATA;
	else if (pg_strcasecmp(fileContent, "DELTA_LOG") == 0)
		result = DELTA_LOG;
	else
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported hudi file content \"%s\"", fileContent)));

	return result;
}

static List *
parseStringArray(json_t *jobject)
{
	int			i;
	List	   *result = NIL;

	if (jobject == NULL)
		return result;

	for (i = 0; i < json_array_size(jobject); i++)
	{
		const char *value = json_string_value(json_array_get(jobject, i));
		result = lappend(result, makeString(pstrdup(value)));
	}

	return result;
}

static ExternalTableMetadata *
parseTableOptions(json_t *joptions)
{
	json_t *jobject;
	ExternalTableMetadata *result = makeNode(ExternalTableMetadata);

	result->isTablePartitioned = json_boolean_value(json_object_get(joptions, "tablePartitioned"));

	jobject = json_object_get(joptions, "preCombineField");
	if (jobject)
		result->preCombineField = pstrdup(json_string_value(jobject));

	jobject = json_object_get(joptions, "recordMergerStrategy");
	if (jobject)
		result->recordMergerStrategy = pstrdup(json_string_value(jobject));

	jobject = json_object_get(joptions, "firstNonSavepointCommit");
	if (jobject)
		result->firstNonSavepointCommit = pstrdup(json_string_value(jobject));

	result->recordKeyFields = parseStringArray(json_object_get(joptions, "recordKeyFields"));
	result->partitionKeyFields = parseStringArray(json_object_get(joptions, "partitionKeyFields"));
	result->completedInstants = parseStringArray(json_object_get(joptions, "completedInstants"));
	result->inflightInstants = parseStringArray(json_object_get(joptions, "inflightInstants"));
	result->extractPartitionValueFromPath = json_boolean_value(json_object_get(joptions, "extractPartitionValueFromPath"));
	result->hiveStylePartitioningEnabled = json_boolean_value(json_object_get(joptions, "hiveStylePartitioningEnabled"));
	result->isMorTable = json_boolean_value(json_object_get(joptions, "morTable"));

	return result;
}

static List *
parseDeltaLogs(json_t *jdeltaLogs)
{
	int   i;
	List *deletes = NIL;

	for (i = 0; i < json_array_size(jdeltaLogs); i++)
	{
		json_t *jdata = json_array_get(jdeltaLogs, i);
		json_t *jmetadata = json_object_get(jdata, "metadata");

		FileFragment *fileFragment = palloc0(sizeof(FileFragment));

		fileFragment->type = T_FileFragment;
		fileFragment->filePath = pstrdup(json_string_value(json_object_get(jdata, "sourceName")));
		fileFragment->content = convertFileContent(json_string_value(json_object_get(jmetadata, "fileContent")));
		fileFragment->format = convertFileFormat(json_string_value(json_object_get(jmetadata, "fileFormat")));

		deletes = lappend(deletes, fileFragment);
	}

	return deletes;
}

static List *
parseFragmentResponse(char *buffer, size_t buffer_size)
{
	List         *result = NIL;
	json_t       *jroot;
	json_t       *joptions;
	json_t       *jcombinedTasks;
	json_error_t  jerror;
	int           i;
	int           j;
	ExternalTableMetadata *tableOptions = NULL;

	jroot = json_loadb(buffer, buffer_size, 0, &jerror);
	if (jroot == NULL)
		elog(ERROR, "failed to decode message:\"%s\", errMessage: %s line: %d",
					pnstrdup(buffer, buffer_size), jerror.text, jerror.line);

	joptions = json_object_get(jroot, "tableOptions");
	if (joptions != NULL)
		tableOptions = parseTableOptions(joptions);

	/* set the first element of the list as tableMetadata */
	result = lappend(result, tableOptions);

	jcombinedTasks = json_object_get(jroot, "combinedTasks");
	for (i = 0; i < json_array_size(jcombinedTasks); i++)
	{
		List *combinedTask = NIL;
		json_t *jtasks = json_object_get(json_array_get(jcombinedTasks, i), "tasks");

		for (j = 0; j < json_array_size(jtasks); j++)
		{
			json_t       *jinstantTime;
			json_t       *jtask = json_array_get(jtasks, j);
			json_t       *jdata = json_object_get(jtask, "data");
			json_t       *jdeletes = json_object_get(jtask, "deletes");
			json_t       *jmetadata = jdata ? json_object_get(jdata, "metadata") : NULL;
			FileScanTask *fileScanTask = palloc0(sizeof(FileScanTask));
			FileFragment *dataFragment;

			fileScanTask->start = json_integer_value(json_object_get(jtask, "start"));
			fileScanTask->length = json_integer_value(json_object_get(jtask, "length"));

			jinstantTime = json_object_get(jtask, "instantTime");
			if (jinstantTime)
				fileScanTask->instantTime = pstrdup(json_string_value(jinstantTime));

			if (jdata != NULL)
			{
				dataFragment = palloc0(sizeof(FileFragment));
				dataFragment->type = T_FileFragment;
				dataFragment->filePath = pstrdup(json_string_value(json_object_get(jdata, "sourceName")));
				dataFragment->content = convertFileContent(json_string_value(json_object_get(jmetadata, "fileContent")));
				dataFragment->format = convertFileFormat(json_string_value(json_object_get(jmetadata, "fileFormat")));
				fileScanTask->dataFile = dataFragment;
			}

			fileScanTask->deletes = parseDeltaLogs(jdeletes);
			fileScanTask->type = T_FileScanTask;

			combinedTask = lappend(combinedTask, fileScanTask);
		}

		result = lappend(result, combinedTask);
	}

	json_decref(jroot);

	return result;
}

List *
hudi_get_external_fragments(Oid relid,
							Index relno,
							List *restrictInfo,
							List *targetList,
							List *locations)
{
	return internal_get_external_fragments("hudi",
											relid,
											relno,
											restrictInfo,
											targetList,
											locations,
											parseFragmentResponse);
}
