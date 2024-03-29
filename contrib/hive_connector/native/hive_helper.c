#include "postgres.h"

#include "access/xact.h"
#include "access/hash.h"
#include "commands/explain.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "cdb/cdbvars.h"
#include "executor/spi.h"

#include "libhms/src/hmsclient.h"
#include "config_parser.h"
#include "hive_helper.h"
#include "strings_util.h"

typedef struct PartNameEntry
{
	Oid  nameID;
	char name[NAMEDATALEN];
} PartNameEntry;

bool hiveEnableCacheFile;

bool
serverHiveConfExists(List *serverConf, const char *serverName)
{
	ListCell *cell;

	foreach(cell, serverConf)
	{
		ConfigItem *item = (ConfigItem *) lfirst(cell);
		if (!strcmp(serverName, item->name))
			return true;
	}

	return false;
}

bool
serverHdfsConfExists(List *serverConf, const char *serverName)
{
	ListCell *cell;

	foreach(cell, serverConf)
	{
		HdfsConfigItem *item = (HdfsConfigItem *) lfirst(cell);
		if (!strcmp(serverName, item->name))
			return true;
	}
	return false;
}

ConfigItem *
getHiveServerConf(List *serverConf, const char *serverName)
{
	ListCell *cell;

	foreach(cell, serverConf)
	{
		ConfigItem *item = (ConfigItem *) lfirst(cell);
		if (!strcmp(serverName, item->name))
		{
			hiveConfCheck(item);
			return item;
		}
	}
	return NULL;
}

HdfsConfigItem *
getHdfsServerConf(List *serverConf, const char *serverName)
{
	ListCell *cell;

	foreach(cell, serverConf)
	{
		HdfsConfigItem *item = (HdfsConfigItem *) lfirst(cell);
		if (!strcmp(serverName, item->name))
		{
			hdfsConfCheck(item);
			return item;
		}
	}
	return NULL;
}

void
spiExec(const char *query)
{
	int spiResult;
	volatile bool connected = false;

	PG_TRY();
	{
		if (SPI_connect() !=  SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect() failed");

		connected = true;
		spiResult = SPI_execute(query, false, 0);
		if (spiResult != SPI_OK_UTILITY)
			elog(ERROR, "SPI_execute() returned %d", spiResult);
		connected = false;
		SPI_finish();
	}
	PG_CATCH();
	{
		if (connected)
			SPI_finish();

		PG_RE_THROW();
	}
	PG_END_TRY();
}

char *
extractPathFromLocation(const char *location)
{
	char *bhPos;
	char *bpPos;

	bhPos = strstr(location, "://");
	if (!bhPos)
		elog(ERROR, "failed to parse location: \"%s\": missing schema name", location);

	bpPos = strstr(bhPos + 3, "/");
	if (!bpPos)
		elog(ERROR, "failed to parse location: \"%s\": missing leading slash", location);

	return bpPos;
}

static const char *
getHdfsVal(const HdfsConfigItem *hdfsConf, const char *keyName)
{
	ListCell *node;
	foreach(node, hdfsConf->haEntries)
	{
		HdfsHAConfEntry *entry = (HdfsHAConfEntry*)lfirst(node);
		if (!strcmp(entry->key, keyName))
		{
			return entry->value;
		}
	}
	return NULL;
}

const char *
extractServiceName(const HdfsConfigItem *hdfsConf)
{
	ListCell *node;
	const char *res = getHdfsVal(hdfsConf, "dfs.nameservices");
	if (!res)
	{
		elog(ERROR, "no nameservice");
	}
	return res;
}

const char *
extractNameNodes(const HdfsConfigItem *hdfsConf, const char *serviceName)
{
	ListCell *node;
	StringInfoData keyBuf;
	initStringInfo(&keyBuf);
	appendStringInfo(&keyBuf, "dfs.ha.namenodes.%s", serviceName);
	const char *res = getHdfsVal(hdfsConf, keyBuf.data);
	pfree(keyBuf.data);
	if (!res)
	{
		elog(ERROR, "no namenodes");
	}
	return res;
}

char *
extractRpcAddr(const HdfsConfigItem *hdfsConf, const char *service, const char *nodeNameStr)
{
	const char *rpcAddrBase = "dfs.namenode.rpc-address";
	StringInfoData rpcBuf;
	StringInfoData keyBuf;
	ListCell *node;
	initStringInfo(&rpcBuf);
	initStringInfo(&keyBuf);

	List *nameNodes = splitString_(nodeNameStr, ',', '\0');
	
	foreach(node, nameNodes)
	{
		char *name = (char*)lfirst(node);
		resetStringInfo(&keyBuf);
		appendStringInfo(&keyBuf, "%s.%s.%s", rpcAddrBase, service, name);
		const char *res = getHdfsVal(hdfsConf, keyBuf.data);
		if (!res)
		{
			pfree(keyBuf.data);
			list_free_deep(nameNodes);
			elog(ERROR, "no rpc addr for %s", keyBuf.data);
		}
		else
		{
			if (rpcBuf.len > 0) appendStringInfoChar(&rpcBuf, ',');
			appendStringInfoString(&rpcBuf, res);
		}
	}
	pfree(keyBuf.data);
	list_free_deep(nameNodes);
	return rpcBuf.data;
}

static const char *
extractFailoverProvider(const HdfsConfigItem *hdfsConf, const char *service)
{
	StringInfoData keyBuf;
	ListCell *node;
	initStringInfo(&keyBuf);
	appendStringInfo(&keyBuf, "dfs.client.failover.proxy.provider.%s", service);
	const char *res = getHdfsVal(hdfsConf, keyBuf.data);
	pfree(keyBuf.data);
	return res;
}

char *
formServerCreateStmt(const char *serverName, const char *dataWrapperName, const HdfsConfigItem *hdfsConf)
{
	const char *servFmt 	= "CREATE SERVER %s FOREIGN DATA WRAPPER %s OPTIONS (hdfs_namenodes '%s', hdfs_port '%s', protocol 'hdfs', hdfs_auth_method '%s'";
	const char *servHAFmt 	= "CREATE SERVER %s FOREIGN DATA WRAPPER %s OPTIONS (hdfs_namenodes '%s', protocol 'hdfs', hdfs_auth_method '%s'";
	const char *krbAuthFmt 	= ", krb_principal '%s', krb_principal_keytab '%s'";
	const char *hdfsHAFmt 	= ", is_ha_supported 'true', dfs_nameservices '%s', dfs_ha_namenodes '%s', dfs_namenode_rpc_address '%s'";
	const char *failOverFmt = ", dfs_client_failover_proxy_provider '%s'";

	StringInfoData sqlBuf;
	initStringInfo(&sqlBuf);

	if (!strcmp(hdfsConf->enableHA, "true"))
	{
		appendStringInfo(&sqlBuf, servHAFmt, serverName, dataWrapperName, hdfsConf->host, hdfsConf->authMethod);
	}
	else
	{
		appendStringInfo(&sqlBuf, servFmt, serverName, dataWrapperName, hdfsConf->host, hdfsConf->port, hdfsConf->authMethod);
	}

	// add hadoop_rpc_protection to server if exists.
	if (hdfsConf->hadoopRpcProtection)
	{
		appendStringInfo(&sqlBuf, ", hadoop_rpc_protection '%s'", hdfsConf->hadoopRpcProtection);
	}

	// add kerberos config to server if auth_method is kerberos.
	if (!strcmp(hdfsConf->authMethod, "kerberos"))
	{
		appendStringInfo(&sqlBuf, krbAuthFmt, hdfsConf->krbPrincipal, hdfsConf->krbKeytab);
	}

	// add hdfs-ha config to server if is_ha_supported is true.
	if (!strcmp(hdfsConf->enableHA, "true"))
	{
		const char *serviceName 		= extractServiceName(hdfsConf);
		const char *nodeNames 		 	= extractNameNodes(hdfsConf, serviceName);
		const char *failoverProvider 	= extractFailoverProvider(hdfsConf, serviceName);
			  char *rpcAddrs 			= extractRpcAddr(hdfsConf, serviceName, nodeNames);
		appendStringInfo(&sqlBuf,
						hdfsHAFmt,
						serviceName,
						nodeNames,
						rpcAddrs);
		if (!failoverProvider)
		{
			appendStringInfo(&sqlBuf, failOverFmt, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailover");
		}
		else
		{
			appendStringInfo(&sqlBuf, failOverFmt, failoverProvider);
		}
		pfree(rpcAddrs);
	}
	appendStringInfoString(&sqlBuf, ")");
	return sqlBuf.data;
}

char *
formUserStmt(const char* userMapName, const char *serverName)
{
	const char *userSql = "CREATE USER MAPPING FOR %s SERVER %s OPTIONS (user '%s')";
	StringInfoData sqlBuf;
	initStringInfo(&sqlBuf);
	appendStringInfo(&sqlBuf, userSql, userMapName, serverName, userMapName);
	return sqlBuf.data;
}

static char*
formFormatOpts(const char *optStr)
{
	StringInfoData optBuf;
	char *stripStr = pnstrdup(optStr + 1, strlen(optStr) - 2);
	List *opts = splitString_(stripStr, ' ', '\0');
	int i;

	initStringInfo(&optBuf);
	for (i = 0; i < opts->length; i += 2)
	{
		char *it = lfirst(list_nth_cell(opts, i));
		for (; *it != '\0'; ++it)
		{
			*it = pg_ascii_tolower(*it);
		}
		if (i)
		{
			appendStringInfoString(&optBuf, ", ");
		}
		appendStringInfoString(&optBuf, (char*)lfirst(list_nth_cell(opts, i)));
		appendStringInfo(&optBuf, " %s", (char*)lfirst(list_nth_cell(opts, i + 1)));
	}
	list_free_deep(opts);
	return optBuf.data;
}

char*
formCreateStmt(HmsHandle *hms,
			const char *destTableName,
			const char *serverName,
			const char *location,
			const char *fields,
			const char *hdfsClusterName,
			const char *format)
{
	size_t idx 			= 0;
	const char *sqlFmt 	= "CREATE FOREIGN TABLE %s(%s) SERVER %s OPTIONS (filePath '%s', hdfs_cluster_name '%s', enablecache '%s', transactional '%s', format '%s'";
	char *hdfsPath;
	StringInfoData sqlBuf;
	
	initStringInfo(&sqlBuf);
	hdfsPath = extractPathFromLocation(location);

	appendStringInfo(&sqlBuf,
					sqlFmt,
					destTableName,
					fields,
					serverName,
					hdfsPath,
					hdfsClusterName,
					hiveEnableCacheFile ? "true" : "false",
					HmsTableIsTransactionalTable(hms) ? "true" : "false",
					format);

	if (pg_strcasecmp(format, "orc") && pg_strcasecmp(format, "parquet") && pg_strcasecmp(format, "avro"))
	{
		char *formatOpts = HmsTableGetParameters(hms);
		char *formatStr = formFormatOpts(formatOpts);
		appendStringInfo(&sqlBuf, ", %s", formatStr);
		pfree(formatOpts);
		pfree(formatStr);
	}

	appendStringInfoChar(&sqlBuf, ')');

	return sqlBuf.data;
}

bool
validateMetaData(HmsHandle *hms,
				 const char *hiveDbName,
				 const char *hiveTableName,
				 char ***partKeys,
				 char ***partKeyTypes,
				 char ***partLocations,
				 char ***fields,
				 char **field)
{
	*partKeys = HmsPartTableGetKeys(hms);
	if (!(*partKeys))
	{
		elog(WARNING, "failed to get partition key for table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));
		return false;
	}

	if (partKeyTypes)
	{
		*partKeyTypes = HmsPartTableGetKeyTypes(hms);
		if (!(*partKeyTypes))
		{
			elog(WARNING, "failed to get type of partition key for table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));
			return false;
		}
	}

	if (partLocations)
	{
		*partLocations = HmsTableGetLocations(hms);
		if (!(*partLocations))
		{
			elog(WARNING, "failed to get locations of partition for table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));
			return false;
		}


		if ((*partLocations)[0] == NULL)
		{
			elog(WARNING, "hive partition table: \"%s.%s\" was ignored: no partition found",
					hiveDbName, hiveTableName);
			return false;
		}
	}

	if (fields)
	{
		*fields = HmsPartTableGetFields(hms);
		if (!(*fields))
		{
			elog(WARNING, "failed to get columns for partition table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));
			return false;
		}
	}

	*field = HmsTableGetField(hms);
	if (!(*field) || !strlen(*field))
	{
		elog(WARNING, "failed to get columns for partition table: \"%s.%s\": %s",
					hiveDbName, hiveTableName, HmsGetError(hms));
		return false;
	}

	return true;
}

void
freeArray(char **strArray)
{
	int i;

	for (i = 0; strArray[i] != NULL; i++)
		pfree(strArray[i]);

	pfree(strArray);
}

const char *
tableFormatConversion(HmsHandle *hms)
{
	char *format;
	char *serialLib;

	format = HmsTableGetFormat(hms);
	if (pg_strcasecmp(format, "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat") == 0)
	{
		pfree(format);
		return "orc";
	}
	if (pg_strcasecmp(format, "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") == 0)
	{
		pfree(format);
		return "parquet";
	}

	if (pg_strcasecmp(format, "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat") == 0)
	{
		pfree(format);
		return "avro";
	}
	pfree(format);

	serialLib = HmsTableGetSerialLib(hms);
	if (pg_strcasecmp(serialLib, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") == 0)
	{
		pfree(serialLib);
		return "text";
	}

	Assert(!pg_strcasecmp(serialLib, "org.apache.hadoop.hive.serde2.OpenCSVSerde"));

	pfree(serialLib);
	return "csv";
}

void
dropTable(const char *table, bool isExternal)
{
	StringInfoData  cmdBuf;

	initStringInfo(&cmdBuf);

	if (isExternal)
		appendStringInfo(&cmdBuf, "drop external table if exists %s", table);
	else
		appendStringInfo(&cmdBuf, "drop table if exists %s", table);

	spiExec(cmdBuf.data);
	pfree(cmdBuf.data);
}

char *
formCreateStmt2(HmsHandle *hms,
				const char *destTableName,
				const char *serverName,
				const char *location,
				const char *field,
				const char *hdfsClusterName,
				const char *format,
				const char *hiveClusterName,
				const char *hiveDbName,
				const char *hiveTableName,
				char **partKeys
				)
{
	const char *tableFmt = "CREATE FOREIGN TABLE %s(%s) SERVER %s OPTIONS (filePath '%s', hive_cluster_name '%s', datasource '%s.%s', hdfs_cluster_name '%s', enableCache '%s', transactional '%s', partitionkeys '%s', format '%s'";
	char *keyBuf;
	char *hdfsPath;
	StringInfoData sqlBuf;

	initStringInfo(&sqlBuf);
	keyBuf = joinString(partKeys, ',', '/');
	hdfsPath = extractPathFromLocation(location);

	appendStringInfo(&sqlBuf,
					tableFmt,
					destTableName,
					field,
					serverName,
					hdfsPath,
					hiveClusterName,
					hiveDbName,
					hiveTableName,
					hdfsClusterName,
					hiveEnableCacheFile ? "true" : "false",
					HmsTableIsTransactionalTable(hms) ? "true" : "false",
					keyBuf,
					format);

	if (pg_strcasecmp(format, "orc") && pg_strcasecmp(format, "parquet") && pg_strcasecmp(format, "avro"))
	{
		char *formatOpts = HmsTableGetParameters(hms);
		char *formatStr = formFormatOpts(formatOpts);
		appendStringInfo(&sqlBuf, ", %s", formatStr);
		pfree(formatOpts);
		pfree(formatStr);
	}

	appendStringInfoChar(&sqlBuf, ')');
	pfree(keyBuf);
	return sqlBuf.data;
}
