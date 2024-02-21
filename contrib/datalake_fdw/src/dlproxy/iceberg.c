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
	else if (pg_strcasecmp(fileFormat, "AVRO") == 0)
		result = AVRO;
	else
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported iceberg file format \"%s\"", fileFormat)));

	return result;
}

static FileContent
convertFileContent(const char *fileContent)
{
	FileContent result;

	if (pg_strcasecmp(fileContent, "DATA") == 0)
		result = DATA;
	else if (pg_strcasecmp(fileContent, "POSITION_DELETES") == 0)
		result = POSITION_DELETES;
	else if (pg_strcasecmp(fileContent, "EQUALITY_DELETES") == 0)
		result = EQUALITY_DELETES;
	else
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported iceberg file content \"%s\"", fileContent)));

	return result;
}

static List *
parseDeletes(json_t *jdeletes)
{
	int i;
	int j;
	List *deletes = NIL;

	for (i = 0; i < json_array_size(jdeletes); i++)
	{
		json_t *jEqColumnNames;
		json_t *jdata = json_array_get(jdeletes, i);
		json_t *jmetadata = json_object_get(jdata, "metadata");

		FileFragment *fileFragment = palloc0(sizeof(FileFragment));

		fileFragment->filePath = pstrdup(json_string_value(json_object_get(jdata, "sourceName")));
		fileFragment->content = convertFileContent(json_string_value(json_object_get(jmetadata, "fileContent")));
		fileFragment->format = convertFileFormat(json_string_value(json_object_get(jmetadata, "fileFormat")));
		fileFragment->recordCount = json_integer_value(json_object_get(jmetadata, "recordCount"));

		jEqColumnNames = json_object_get(jmetadata, "eqColumnNames");
		if (jEqColumnNames)
		{
			List *eqColumnNames = NIL;
			for (j = 0; j < json_array_size(jEqColumnNames); j++)
			{
				const char *value = json_string_value(json_array_get(jEqColumnNames, j));
				eqColumnNames = lappend(eqColumnNames, makeString(pstrdup(value)));
			}

			fileFragment->eqColumnNames = eqColumnNames;
		}

		fileFragment->type = T_FileFragment;

		deletes = lappend(deletes, fileFragment);
	}

	return deletes;
}

static List *
parseFragmentResponse(char *buffer, size_t buffer_size)
{
	List         *result = NIL;
	json_t       *jroot;
	json_t       *jcombinedTasks;
	json_error_t  jerror;
	int           i;
	int           j;

	jroot = json_loadb(buffer, buffer_size, 0, &jerror);
	if (jroot == NULL)
		elog(ERROR, "failed to decode message:\"%s\", errMessage: %s line: %d",
					pnstrdup(buffer, buffer_size), jerror.text, jerror.line);

	/* set the first element of the list as tableMetadata */
	// result = lappend(result, NULL);
	ExternalTableMetadata *tableMetadata = makeNode(ExternalTableMetadata);
	result = lappend(result, tableMetadata);

	jcombinedTasks = json_object_get(jroot, "combinedTasks");
	for (i = 0; i < json_array_size(jcombinedTasks); i++)
	{
		List *combinedTask = NIL;
		json_t *jtasks = json_object_get(json_array_get(jcombinedTasks, i), "tasks");

		for (j = 0; j < json_array_size(jtasks); j++)
		{
			json_t       *jtask = json_array_get(jtasks, j);
			json_t       *jdata = json_object_get(jtask, "data");
			json_t       *jdeletes = json_object_get(jtask, "deletes");
			json_t       *jmetadata = json_object_get(jdata, "metadata");
			FileScanTask *fileScanTask = palloc0(sizeof(FileScanTask));
			FileFragment *dataFragment = palloc0(sizeof(FileFragment));

			fileScanTask->start = json_integer_value(json_object_get(jtask, "start"));
			fileScanTask->length = json_integer_value(json_object_get(jtask, "length"));

			dataFragment->type = T_FileFragment;
			dataFragment->filePath = pstrdup(json_string_value(json_object_get(jdata, "sourceName")));
			dataFragment->content = convertFileContent(json_string_value(json_object_get(jmetadata, "fileContent")));
			dataFragment->format = convertFileFormat(json_string_value(json_object_get(jmetadata, "fileFormat")));
			dataFragment->recordCount = json_integer_value(json_object_get(jmetadata, "recordCount"));

			fileScanTask->dataFile = dataFragment;
			fileScanTask->deletes = parseDeletes(jdeletes);
			fileScanTask->type = T_FileScanTask;

			combinedTask = lappend(combinedTask, fileScanTask);
		}

		result = lappend(result, combinedTask);
	}

	json_decref(jroot);

	return result;
}

List *
iceberg_get_external_fragments(Oid relid,
							   Index relno,
							   List *restrictInfo,
							   List *targetList,
							   List *locations)
{
	return internal_get_external_fragments("iceberg",
											relid,
											relno,
											restrictInfo,
											targetList,
											locations,
											parseFragmentResponse);
}
