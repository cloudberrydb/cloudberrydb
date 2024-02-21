#include "datalake_fragment.h"
#include "common/fileSystemWrapper.h"
#include "common/partition_selector.h"
#include "dlproxy/datalake.h"
#include <curl/curl.h>


static List* SerializeFragmentList(gopherFileInfo* lists, int count, int64_t *totalSize);
static List *GetFragmentList(dataLakeOptions *options, int64_t *totalSize);
static List *get_partition_values(Relation relation, dataLakeOptions *options);
static List *getNextPartitionFragmentList(dataLakeOptions *options, ossFileStream stream, int64_t *totalSize);
static List *GetPartitionFragmentList(Relation relation, List *quals, dataLakeOptions *options, int64_t *totalSize);
static List *convert_iceberg_hudi_options(dataLakeOptions *options);

static List*
SerializeFragmentList(gopherFileInfo* lists, int count, int64_t *totalSize)
{
	List	   *serializedFragment = NIL;
	for (int i = 0; i < count; i++)
	{
		if (lists[i].mLength > 0)
		{
			List *fragment = NIL;
			fragment = lappend(fragment, makeString(pstrdup(lists[i].mPath)));
			char buf[20] = {0};
			sprintf(buf, "%ld", lists[i].mLength);
			fragment = lappend(fragment, makeString(pstrdup(buf)));
			serializedFragment = lappend(serializedFragment, fragment);
			if (totalSize)
				*totalSize += lists[i].mLength;
		}
	}
	return serializedFragment;
}

static List *
GetFragmentList(dataLakeOptions *options, int64_t *totalSize)
{
	List *fragment = NIL;
	gopherConfig* conf = createGopherConfig((void*) options->gopher);
	ossFileStream stream = createFileSystem(conf);
	int count = 0;
	gopherFileInfo* lists = listDir(stream, options->prefix, &count, true);
	fragment = lappend(fragment, SerializeFragmentList(lists, count, totalSize));
	freeListDir(stream, lists, count);
	gopherDestroyHandle(stream);
	freeGopherConfig(conf);

	return fragment;
}

static List *
get_partition_values(Relation relation, dataLakeOptions *options)
{
	List *locations = NIL;
	ListCell   *cell;
	StringInfoData partitionkey;
	int index = 1;
	int count = list_length(options->hiveOption->hivePartitionKey);

	initStringInfo(&partitionkey);
	foreach(cell, options->hiveOption->hivePartitionKey)
	{
		char *def = (char *) lfirst(cell);
		appendStringInfo(&partitionkey, "%s%s",
							def,
							(index == count) ? "" : ",");
		index++;
	}

	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "gphdfs:/%s hive_cluster_name=%s datasource=%s hdfs_cluster_name=%s" \
				"cache=%s transactional=%s partition_keys=%s",
		options->filePath,
		options->hive_cluster_name,
		options->hiveOption->datasource,
		options->hdfs_cluster_name,
		(options->gopher->enableCache) ? "true" : "false",
		(options->hiveOption->transactional) ? "true" : "false",
		partitionkey.data);

	locations = lappend(locations, makeString(pstrdup(buf.data)));

	return get_external_fragments(RelationGetRelid(relation), 0, NIL, NIL,
								  locations, options->format, false);
}

static List *
getNextPartitionFragmentList(dataLakeOptions *options, ossFileStream stream, int64_t *totalSize)
{
	ListCell *lckey;
	ListCell *lcvalue;
	List *serializedFragment = NIL;
	List *partitionKeys;
	List *partitionValues;
	PartitionConstraint *pc;

	pc = (PartitionConstraint*) list_nth(options->hiveOption->hivePartitionConstraints,
		options->hiveOption->curPartition);
	partitionValues = pc->partitionValues;
	partitionKeys = options->hiveOption->hivePartitionKey;


	StringInfoData prefix;
	initStringInfo(&prefix);
	appendStringInfoString(&prefix, options->filePath);

	int len = strlen(prefix.data) - 1;
	if (prefix.data[len] != '/')
	{
		appendStringInfoString(&prefix, "/");
	}

	forboth(lckey, partitionKeys, lcvalue, partitionValues)
	{
		char *partKey = (char *) lfirst(lckey);
		char *partValue = (char *) lfirst(lcvalue);
		char *escapedValue = curl_escape(partValue, strlen(partValue));

		appendStringInfo(&prefix, "%s=%s/", partKey, partValue);
		curl_free(escapedValue);
	}

	int count = 0;
	gopherFileInfo* lists = listDir(stream, prefix.data, &count, true);
	for (int i = 0; i < count; i++)
	{
		if (lists[i].mLength > 0)
		{
			List *fragment = NIL;
			fragment = lappend(fragment, makeString(pstrdup(lists[i].mPath)));
			char buf[64] = {0};
			sprintf(buf, "%ld", lists[i].mLength);
			fragment = lappend(fragment, makeString(pstrdup(buf)));
			serializedFragment = lappend(serializedFragment, fragment);

			if (totalSize)
				totalSize += lists[i].mLength;
		}
	}
	freeListDir(stream, lists, count);
	return serializedFragment;
}

static List *
GetPartitionFragmentList(Relation relation, List *quals, dataLakeOptions *options, int64_t *totalSize)
{
	List *serializedFragment = NIL;
	List *serializedFragmentCell = NIL;
	List *partitionValues;
	gopherConfig* conf = createGopherConfig((void*) options->gopher);
	ossFileStream stream = createFileSystem(conf);

	partitionValues = get_partition_values(relation, options);
	options->hiveOption->hivePartitionValues = transfromHMSPartitions(partitionValues);
	initializeConstraints(options, quals, relation->rd_att);

	serializedFragment = lappend(serializedFragment, partitionValues);

	int nPartitions = list_length(options->hiveOption->hivePartitionConstraints);
	for (int i = 0; i < nPartitions; i++)
	{
		serializedFragmentCell = getNextPartitionFragmentList(options, stream, totalSize);
		serializedFragment = lappend(serializedFragment, serializedFragmentCell);
		options->hiveOption->curPartition += 1;
	}

	/* set curPartition to zero for external table to read from begin */
	options->hiveOption->curPartition = 0;
	gopherDestroyHandle(stream);
	freeGopherConfig(conf);
	return serializedFragment;
}

List *
GetExternalFragmentList(Relation relation, List *quals, dataLakeOptions *options, int64_t *totalSize)
{
	if (FORMAT_IS_ICEBERG(options->format) || FORMAT_IS_HUDI(options->format))
	{
		List *locations = convert_iceberg_hudi_options(options);
		return get_external_fragments(RelationGetRelid(relation), 0, NIL, NIL,
									  locations, options->format, false);
	}

	if (SUPPORT_PARTITION_TABLE(options->gopher->gopherType,
								options->format,
								options->hiveOption->hivePartitionKey,
								options->hiveOption->datasource))
	{
		return GetPartitionFragmentList(relation, quals, options, totalSize);
	}
	else
	{
		options->hiveOption->partitiontable = false;
		return GetFragmentList(options, totalSize);
	}
}

static List *
convert_iceberg_hudi_options(dataLakeOptions *options)
{
	List			*result = NIL;
	StringInfoData	buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "datalake://%s catalog_type=%s server_name=%s",
					 options->filePath, options->catalog_type,
					 options->server_name);
	if (options->table_identifier)
		appendStringInfo(&buf, " table_identifier=%s", options->table_identifier);
	if (options->split_size > 0)
		appendStringInfo(&buf, " split_size=%d", options->split_size);
	if (options->cache_enabled)
		appendStringInfo(&buf, " cache_enabled=%s", options->cache_enabled);
	if (options->query_type)
		appendStringInfo(&buf, " query_type=%s", options->query_type);
	if (options->metadata_table_enable)
		appendStringInfo(&buf, " metadata_table_enable=%s", options->metadata_table_enable);

	result = lappend(result, makeString(pstrdup(buf.data)));
	return result;
}

List *
deserializeExternalFragmentList(Relation relation, List *quals, dataLakeOptions *options, List *fragmentInfo)
{
	List *fragmentData = NIL;

	if (FORMAT_IS_ICEBERG(options->format) || FORMAT_IS_HUDI(options->format))
	{
		fragmentData = list_delete_first_n(fragmentInfo, FdwScanPrivateFragmentList);
		return fragmentData;
	}

	if (SUPPORT_PARTITION_TABLE(options->gopher->gopherType,
								options->format,
								options->hiveOption->hivePartitionKey,
								options->hiveOption->datasource))
	{
		List *partitionValues = list_nth(fragmentInfo, PrivatePartitionData);

		/* transfrom partition values */
		options->hiveOption->hivePartitionValues = transfromHMSPartitions(partitionValues);
		initializeConstraints(options, quals, relation->rd_att);
		/* get fragment lists */
		for (int i = PrivatePartitionFragmentLists; i < list_length(fragmentInfo); i++)
		{
			List* serializedFragmentList = (List *) list_nth(fragmentInfo, i);
			fragmentData = lappend(fragmentData, serializedFragmentList);
		}
	}
	else
	{
		fragmentData = (List *) list_nth(fragmentInfo, FdwScanPrivateFragmentList);
	}

	return fragmentData;
}
