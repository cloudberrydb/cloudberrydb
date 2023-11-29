/*
 * dataLake_option.c
 *		  Foreign-data wrapper option handling for DATA LAKE (Platform Extension Framework)
 *
 */
#include "datalake_option.h"
#include "common/partition_selector.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "foreign/foreign.h"
#include "access/table.h"
#include "postmaster/postmaster.h"


#define DATALAKE_GOPHERMETA_FOLDER "gophermeta"

struct datalakeFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

static const struct datalakeFdwOption valid_oss_server_options[] = {
	{DATALAKE_OPTION_PROTOCOL, ForeignServerRelationId},
	{DATALAKE_OPTION_HOST, ForeignServerRelationId},
	{DATALAKE_OPTION_PORT, ForeignServerRelationId},
	{DATALAKE_OPTION_ISVIRTUAL, ForeignServerRelationId},
	{DATALAKE_OPTION_ISHTTPS, ForeignServerRelationId},
	{NULL, InvalidOid}
};

static const struct datalakeFdwOption valid_hdfs_server_options[] = {
	{DATALAKE_OPTION_PROTOCOL, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_NAMENODE, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_PORT, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_HDFS_AUTH_METHOD, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_KRB_PRINCIPAL, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_KRP_PRINCIPAL_KEYTAB, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_HADOOP_RPC_PROTECTION, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTOCOL, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_IS_HA_SUPPORTED, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DFS_NAME_SERVICES, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DFS_HA_NAMENODE, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DFS_NAMENODE_RPC_ADDR, ForeignServerRelationId},
	{DATALAKE_OPTION_HDFS_DFS_CLIENT_FAILOVER, ForeignServerRelationId},
	{NULL, InvalidOid}
};

static const struct datalakeFdwOption valid_foreign_options[] = {
	{DATALAKE_OPTION_COMPRESS, ForeignTableRelationId},
	{DATALAKE_OPTION_FILEPATH, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_TEXT, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_CSV, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_ORC, ForeignTableRelationId},
	{DATALAKE_OPTION_FORMAT_PARQUET, ForeignTableRelationId},
	{DATALAKE_OPTION_FILE_SIZE_LIMIT, ForeignTableRelationId},
	{DATALAKE_OPTION_ENABLE_CACHE, ForeignTableRelationId},
	{DATALAKE_OPTION_HIVE_DATASOURCE, ForeignTableRelationId},
	{DATALAKE_OPTION_HIVE_PARTITIONKEY, ForeignTableRelationId},
	{DATALAKE_OPTION_HIVE_TRANSACTIONAL, ForeignTableRelationId},
	{DATALAKE_OPTION_HIVE_CLUSTER_NAME, ForeignTableRelationId},
	{DATALAKE_OPTION_HDFS_CLUSTER_NAME, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_FORMAT, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_HEADER, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_DELIMITER, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_QUOTE, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_ESCAPE, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_NULL, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_ENCODING, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_NEWLINE, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_FILL_MISSING_FIELDS, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_FORCE_NOT_NULL, ForeignTableRelationId},
	{DATALAKE_COPY_OPTION_FORCE_NULL, ForeignTableRelationId},
	{NULL, InvalidOid}
};

static const struct datalakeFdwOption valid_usermaping_options[] = {
	{DATALAKE_OPTION_ACCESKEY, UserMappingRelationId},
	{DATALAKE_OPTION_SECRETKEY, UserMappingRelationId},
	{DATALAKE_OPTION_USER, UserMappingRelationId},
	{NULL, InvalidOid}
};

extern Datum datalake_fdw_validator(PG_FUNCTION_ARGS);

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(datalake_fdw_validator);


/*
 * Helper functions
 */
bool IsValidForeignOption(const char *option, Oid context);

bool IsValidHdfsServerOption(const char *option, Oid context);

bool IsValidOSSServerOption(const char *option, Oid context);

bool IsValidUserMappingOption(const char *option, Oid context);

void check_foreign_option(List *options_list, Oid catalog);

bool check_protocol_values(const char* values);

void check_server_option(List *options_list, Oid catalog);

void checkHdfsCombin(List *options_list, Oid catalog);

void check_user_mapping_option(List *options_list, Oid catalog);

void DatalakeGetGopherSocketPath(char *dest);

void DatalakeGetGopherPlasmaSocketPath(char *dest);

void DatalakeGetGopherMetaPath(char *dest);

void checkValidRecordBatchOpt(dataLakeOptions *options);

/*
 * Parser functions
 */
void parserUri(dataLakeOptions *opt);

void parserHdfsServerOption(dataLakeOptions *datalakeopt, List *options);

void parseForeignTableOptions(dataLakeOptions* opt, List *options);

void parseOssUserMappingOptions(dataLakeOptions* opt, List *options);

void parseOssServerOption(dataLakeOptions* opt, List *options);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 *
 */
Datum
datalake_fdw_validator(PG_FUNCTION_ARGS)
{
	Oid			catalog = PG_GETARG_OID(1);
	List		*options_list = untransformRelOptions(PG_GETARG_DATUM(0));

	check_server_option(options_list, catalog);
	check_foreign_option(options_list, catalog);
	check_user_mapping_option(options_list, catalog);
	PG_RETURN_VOID();
}


/*
 * Retrieve per-column generic options from pg_attribute and construct a list
 * of DefElems representing them.
 *
 * At the moment we only have "force_not_null", and "force_null",
 * which should each be combined into a single DefElem listing all such
 * columns, since that's what COPY expects.
 */
static List *
get_file_fdw_attribute_options(Oid relid)
{
	Relation	rel;
	TupleDesc	tupleDesc;
	AttrNumber	natts;
	AttrNumber	attnum;
	List	   *fnncolumns = NIL;
	List	   *fncolumns = NIL;

	List	   *options = NIL;

	rel = table_open(relid, AccessShareLock);
	tupleDesc = RelationGetDescr(rel);
	natts = tupleDesc->natts;

	/* Retrieve FDW options for all user-defined attributes. */
	for (attnum = 1; attnum <= natts; attnum++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, attnum - 1);
		List	   *options;
		ListCell   *lc;

		/* Skip dropped attributes. */
		if (attr->attisdropped)
			continue;

		options = GetForeignColumnOptions(relid, attnum);
		foreach(lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "force_not_null") == 0)
			{
				if (defGetBoolean(def))
				{
					char	   *attname = pstrdup(NameStr(attr->attname));

					fnncolumns = lappend(fnncolumns, makeString(attname));
				}
			}
			else if (strcmp(def->defname, "force_null") == 0)
			{
				if (defGetBoolean(def))
				{
					char	   *attname = pstrdup(NameStr(attr->attname));

					fncolumns = lappend(fncolumns, makeString(attname));
				}
			}
			/* maybe in future handle other options here */
		}
	}

	table_close(rel, AccessShareLock);

	/*
	 * Return DefElem only when some column(s) have force_not_null /
	 * force_null options set
	 */
	if (fnncolumns != NIL)
		options = lappend(options, makeDefElem("force_not_null", (Node *) fnncolumns, -1));

	if (fncolumns != NIL)
		options = lappend(options, makeDefElem("force_null", (Node *) fncolumns, -1));

	return options;
}

void parseOssServerOption(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		/* server options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HOST) == 0)
		{
			opt->gopher->host = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PORT) == 0)
		{
			opt->gopher->port = atoi(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_ISVIRTUAL) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->gopher->useVirtualHost = true;
			}
			else
			{
				opt->gopher->useVirtualHost = false;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PROTOCOL) == 0)
		{
			opt->gopher->gopherType = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_ISHTTPS) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->gopher->useHttps = true;
			}
			else
			{
				opt->gopher->useHttps = false;
			}
		}
	}
}

void parseOssUserMappingOptions(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		/* user mapping options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_ACCESKEY) == 0)
		{
			opt->gopher->accessKey = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_SECRETKEY) == 0)
		{
			opt->gopher->secretKey = pstrdup(defGetString(def));
		}
	}
}

void parseForeignTableOptions(dataLakeOptions* opt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		/* foreign table options */
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_FILEPATH) == 0)
		{
			opt->filePath = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_FORMAT) == 0)
		{
			opt->format = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_COMPRESS) == 0)
		{
			opt->compress = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_ENABLE_CACHE) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->gopher->enableCache = true;
			}
			else
			{
				opt->gopher->enableCache = false;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_FILE_SIZE_LIMIT) == 0)
		{
			opt->fileSizeLimit = atoi(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HIVE_DATASOURCE) == 0)
		{
			opt->hiveOption->datasource = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HIVE_PARTITIONKEY) == 0)
		{
			opt->hiveOption->hivePartitionKey = splitString2(defGetString(def), ',', '/');
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HIVE_TRANSACTIONAL) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				opt->hiveOption->transactional = true;
			}
			else
			{
				opt->hiveOption->transactional = false;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HIVE_CLUSTER_NAME) == 0)
		{
			opt->hive_cluster_name = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_CLUSTER_NAME) == 0)
		{
			opt->hdfs_cluster_name = pstrdup(defGetString(def));
		}
	}
}

dataLakeOptions *getOptions(Oid foreigntableid)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	List	   *options;
	UserMapping *user;
	ListCell   *lc;
	char*	protocol = NULL;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * file_fdw doesn't have any options that can be specified there.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);
	user = GetUserMapping(GetUserId(), server->serverid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);
	options = list_concat(options, get_file_fdw_attribute_options(foreigntableid));
	options = list_concat(options, user->options);

	dataLakeOptions *opt = (dataLakeOptions*)palloc0(sizeof(dataLakeOptions));
	opt->gopher = (gopherOptions*)palloc0(sizeof(gopherOptions));
	opt->hiveOption = (hiveOptions*)palloc0(sizeof(hiveOptions));

	char connect_path[1024] = {0};
	DatalakeGetGopherSocketPath(connect_path);
	opt->gopher->connect_path = pstrdup(connect_path);

	char connect_plasma_path[1024] = {0};
	DatalakeGetGopherPlasmaSocketPath(connect_plasma_path);
	opt->gopher->connect_plasma_path = pstrdup(connect_plasma_path);

	char connect_worker_path[1024] = {0};
	DatalakeGetGopherMetaPath(connect_worker_path);
	opt->gopher->worker_path = pstrdup(connect_worker_path);

	foreach(lc, server->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PROTOCOL) == 0)
		{
			protocol = defGetString(def);
			break;
		}
	}

	if (pg_strcasecmp(protocol, DATALAKE_HDFS_PROTOCOL) == 0)
	{
		parserHdfsServerOption(opt, server->options);
		parseForeignTableOptions(opt, table->options);
		opt->prefix = pstrdup(opt->filePath);
	}
	else
	{
		parseOssServerOption(opt, server->options);
		parseOssUserMappingOptions(opt, user->options);
		parseForeignTableOptions(opt, table->options);
		parserUri(opt);
	}

	return opt;
}

List* getCopyOptions(Oid foreigntableid)
{
	List *copyOptions = NIL;
	ForeignTable *table;
	table = GetForeignTable(foreigntableid);
	List *options = NIL;
	options = list_concat(options, table->options);
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_FORMAT) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_HEADER) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_DELIMITER) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_QUOTE) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_ESCAPE) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_NULL) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_ENCODING) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_NEWLINE) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_FILL_MISSING_FIELDS) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_FORCE_NOT_NULL) == 0 ||
			pg_strcasecmp(def->defname, DATALAKE_COPY_OPTION_FORCE_NULL) == 0)
		{
			copyOptions = lappend(copyOptions, def);
		}
	}
	return copyOptions;
}

void parserUri(dataLakeOptions *opt)
{
	if (opt->filePath)
	{
		/* get bucket */
		char* uri = opt->filePath + 1;
		char* delimit = pstrdup("/");
		char* bucket = strstr(uri, delimit);
		int len = bucket - uri;
		if (len <= 0)
		{
			ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("datalake filePath %s not found bucket or "
				"For the bucket the path needs '/' to end.", opt->filePath)));
			return;
		}
		char* buf = (char*)palloc0(len + 1);
		memcpy(buf, uri, len);
		opt->gopher->bucket = buf;

		/* get prefix */
		uri = bucket + 1;
		len = strlen(uri);
		if (len <= 0)
		{
			opt->prefix = NULL;
			return;
		}
		char* prefix = (char*)palloc0(len + 1);
		memcpy(prefix, uri, len);
		opt->prefix = prefix;

		pfree(delimit);
	}
}

void parserHdfsServerOption(dataLakeOptions *datalakeopt, List *options)
{
	ListCell   *lc;
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PROTOCOL) == 0)
		{
			datalakeopt->gopher->gopherType = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_NAMENODE) == 0)
		{
			datalakeopt->gopher->hdfs_namenode_host = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_PORT) == 0)
		{
			datalakeopt->gopher->hdfs_namenode_port = atoi(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_HDFS_AUTH_METHOD) == 0)
		{
			datalakeopt->gopher->hdfs_auth_method = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRB_PRINCIPAL) == 0)
		{
			datalakeopt->gopher->krb_principal = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRP_PRINCIPAL_KEYTAB) == 0)
		{
			datalakeopt->gopher->krb_principal_keytab = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_HADOOP_RPC_PROTECTION) == 0)
		{
			datalakeopt->gopher->hadoop_rpc_protection = pstrdup(defGetString(def));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTOCOL) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				datalakeopt->gopher->data_transfer_protocol = true;
			}
			else
			{
				datalakeopt->gopher->data_transfer_protocol = false;
			}

		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_IS_HA_SUPPORTED) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				datalakeopt->gopher->is_ha_supported = true;
			}
			else
			{
				datalakeopt->gopher->is_ha_supported = false;
			}
		}
	}

	/* parser hdfs ha config */
	if (datalakeopt->gopher->is_ha_supported)
	{
		foreach(lc, options)
		{
			DefElem *def = (DefElem *) lfirst(lc);

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_NAME_SERVICES) == 0)
			{
				datalakeopt->gopher->dfs_name_services = pstrdup(defGetString(def));
			}

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_HA_NAMENODE) == 0)
			{
				datalakeopt->gopher->dfs_ha_namenodes = pstrdup(defGetString(def));
			}

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_NAMENODE_RPC_ADDR) == 0)
			{
				datalakeopt->gopher->dfs_ha_namenode_rpc_addr = pstrdup(defGetString(def));
			}

			if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_CLIENT_FAILOVER) == 0)
			{
				datalakeopt->gopher->dfs_client_failover = pstrdup(defGetString(def));
			}
		}
	}
}

void freeDataLakeOptions(dataLakeOptions *options)
{
	if (options->gopher)
	{
		if (options->gopher->connect_path)
		{
			pfree(options->gopher->connect_path);
			options->gopher->connect_path = NULL;
		}

		if (options->gopher->gopherType)
		{
			pfree(options->gopher->gopherType);
			options->gopher->gopherType = NULL;
		}

		if (options->gopher->bucket)
		{
			pfree(options->gopher->bucket);
			options->gopher->bucket = NULL;
		}

		if (options->gopher->protocol)
		{
			pfree(options->gopher->protocol);
			options->gopher->protocol = NULL;
		}

		if (options->gopher->accessKey)
		{
			pfree(options->gopher->accessKey);
			options->gopher->accessKey = NULL;
		}

		if (options->gopher->secretKey)
		{
			pfree(options->gopher->secretKey);
			options->gopher->secretKey = NULL;
		}

		if (options->gopher->host)
		{
			pfree(options->gopher->host);
			options->gopher->host = NULL;
		}

		/* hdfs */
		if (options->gopher->hdfs_namenode_host)
		{
			pfree(options->gopher->hdfs_namenode_host);
			options->gopher->hdfs_namenode_host = NULL;
		}

		if (options->gopher->hdfs_auth_method)
		{
			pfree(options->gopher->hdfs_auth_method);
			options->gopher->hdfs_auth_method = NULL;
		}

		if (options->gopher->krb_principal)
		{
			pfree(options->gopher->krb_principal);
			options->gopher->krb_principal = NULL;
		}

		if (options->gopher->krb_principal_keytab)
		{
			pfree(options->gopher->krb_principal_keytab);
			options->gopher->krb_principal_keytab = NULL;
		}

		if (options->gopher->krb5_ccname)
		{
			pfree(options->gopher->krb5_ccname);
			options->gopher->krb5_ccname = NULL;
		}

		if (options->gopher->hadoop_rpc_protection)
		{
			pfree(options->gopher->hadoop_rpc_protection);
			options->gopher->hadoop_rpc_protection = NULL;
		}

		if (options->gopher->dfs_name_services)
		{
			pfree(options->gopher->dfs_name_services);
			options->gopher->dfs_name_services = NULL;
		}

		if (options->gopher->dfs_ha_namenodes)
		{
			pfree(options->gopher->dfs_ha_namenodes);
			options->gopher->dfs_ha_namenodes = NULL;
		}

		if (options->gopher->dfs_ha_namenode_rpc_addr)
		{
			pfree(options->gopher->dfs_ha_namenode_rpc_addr);
			options->gopher->dfs_ha_namenode_rpc_addr = NULL;
		}

		if (options->gopher->dfs_client_failover)
		{
			pfree(options->gopher->dfs_client_failover);
			options->gopher->dfs_client_failover = NULL;
		}

		pfree(options->gopher);
		options->gopher = NULL;
	}

	if (options->hiveOption)
	{
		if (options->hiveOption->hivePartitionKey)
		{
			ListCell *cell;
			foreach(cell, options->hiveOption->hivePartitionKey)
			{
				char *def = (char *) lfirst(cell);
				if (def)
				{
					pfree(def);
				}
			}
			options->hiveOption->hivePartitionKey = NIL;
		}

		if (options->hiveOption->hivePartitionValues)
		{
			ListCell *cell;
			foreach(cell, options->hiveOption->hivePartitionValues)
			{
				List *values = (List *) lfirst(cell);
				ListCell *cell2;
				foreach(cell2, values)
				{
					char *partValue = (char *) lfirst(cell2);
					if (partValue)
					{
						pfree(partValue);
					}
				}
			}
			options->hiveOption->hivePartitionValues = NIL;
		}

		if (options->hiveOption->hivePartitionConstraints)
		{
			list_free(options->hiveOption->hivePartitionConstraints);
			options->hiveOption->hivePartitionConstraints = NIL;
		}

		if (options->hiveOption->constraints)
		{
			list_free(options->hiveOption->constraints);
			options->hiveOption->constraints = NIL;
		}

		if (options->hiveOption->storePartitionInfo)
		{
			pfree(options->hiveOption->storePartitionInfo);
			options->hiveOption->storePartitionInfo = NULL;
		}

		if (options->hiveOption->orignPrefix)
		{
			pfree(options->hiveOption->orignPrefix);
			options->hiveOption->orignPrefix = NULL;
		}

		if (options->hiveOption->datasource)
		{
			pfree(options->hiveOption->datasource);
			options->hiveOption->datasource = NULL;
		}
	}

	if (options)
	{
		if (options->filePath)
		{
			pfree(options->filePath);
			options->filePath = NULL;
		}

		if (options->format)
		{
			pfree(options->format);
			options->format = NULL;
		}

		if (options->compress)
		{
			pfree(options->compress);
			options->compress = NULL;
		}

		if (options->prefix)
		{
			pfree(options->prefix);
			options->prefix = NULL;
		}

		if (options->hive_cluster_name)
		{
			pfree(options->hive_cluster_name);
			options->hive_cluster_name = NULL;
		}

		if (options->hdfs_cluster_name)
		{
			pfree(options->hdfs_cluster_name);
			options->hdfs_cluster_name = NULL;
		}
		pfree(options);
		options = NULL;
	}
}

void check_server_option(List *options_list, Oid catalog)
{
	char *protocol = NULL;
	ListCell *cell;

	if (catalog != ForeignServerRelationId)
		return;

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);
		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_PROTOCOL) == 0)
		{
			protocol = defGetString(def);
		}
	}

	if (pg_strcasecmp(protocol, DATALAKE_HDFS_PROTOCOL) == 0)
	{
		foreach(cell, options_list)
		{
			DefElem *def = (DefElem *) lfirst(cell);
			if (!IsValidHdfsServerOption(def->defname, catalog))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid hdfs option \"%s\".",
								def->defname)));
			}
		}
		checkHdfsCombin(options_list, catalog);
	}
	else
	{
		if (!check_protocol_values(protocol))
		{
			ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid protocol \"%s\". datalake support oss protocol"
						 " ali, cos, qs, s3, s3b, huawei, ks3.",
								protocol)));
		}

		foreach(cell, options_list)
		{
			DefElem *def = (DefElem *) lfirst(cell);
			if (!IsValidOSSServerOption(def->defname, catalog))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid oss option \"%s\".",
								def->defname)));
			}
		}

	}
	return;
}

bool check_protocol_values(const char* values)
{
	if (pg_strcasecmp(DATALAKE_OSS_PROTOCOL_ALI, values) == 0 ||
		pg_strcasecmp(DATALAKE_OSS_PROTOCOL_COS, values) == 0 ||
		pg_strcasecmp(DATALAKE_OSS_PROTOCOL_QINGSTORE, values) == 0 ||
		pg_strcasecmp(DATALAKE_OSS_PROTOCOL_S3, values) == 0 ||
		pg_strcasecmp(DATALAKE_OSS_PROTOCOL_S3B, values) == 0 ||
		pg_strcasecmp(DATALAKE_OSS_PROTOCOL_HUAWEI, values) == 0 ||
		pg_strcasecmp(DATALAKE_OSS_PROTOCOL_KS3, values) == 0)
	{
		return true;
	}
	return false;
}

void check_foreign_option(List *options_list, Oid catalog)
{
	ListCell *cell;
	char* format = NULL;
	char* compression = NULL;
	if (catalog != ForeignTableRelationId)
		return;

	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (!IsValidForeignOption(def->defname, catalog))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid foreign option \"%s\".",
							def->defname)));
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_FORMAT) == 0)
		{
			format = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_COMPRESS) == 0)
		{
			compression = defGetString(def);
		}
	}

	if (!(pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_TEXT) == 0 ||
		pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_CSV) == 0 ||
		pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_ORC) == 0 ||
		pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_PARQUET) == 0))
	{
		ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid foreign format option \"%s\". "
							"datalake support csv, text, parquet, orc.",
							format)));
	}

	if (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_TEXT) == 0 ||
		pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_CSV) == 0)
	{

	}

	if (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_ORC) == 0)
	{
		/* just not support orc compress */
	}

	if (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_PARQUET) == 0)
	{
		if (compression != NULL)
		{
			if (!(pg_strcasecmp(compression, DATALAKE_COMPRESS_SNAPPY) == 0 ||
				pg_strcasecmp(compression, DATALAKE_COMPRESS_GZIP) == 0 ||
				pg_strcasecmp(compression, DATALAKE_COMPRESS_ZSTD) == 0 ||
				pg_strcasecmp(compression, DATALAKE_COMPRESS_LZ4) == 0 ||
				pg_strcasecmp(compression, DATALAKE_COMPRESS_UNCOMPRESS) == 0))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid foreign compression options \"%s\". "
								"datalake parquet support snappy, gzip, zstd, lz4.",
								compression)));
			}
		}
	}
}

void check_user_mapping_option(List *options_list, Oid catalog)
{
	ListCell *cell;

	if (catalog != UserMappingRelationId)
		return;

	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (!IsValidUserMappingOption(def->defname, catalog))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("invalid user mapping option \"%s\".",
							def->defname)));
		}
	}
}

bool IsValidOSSServerOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_oss_server_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

bool IsValidHdfsServerOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_hdfs_server_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

bool IsValidForeignOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_foreign_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

bool IsValidUserMappingOption(const char *option, Oid context)
{
	const struct datalakeFdwOption *entry;
	for (entry = valid_usermaping_options; entry->optname; entry++)
	{
		if (context == entry->optcontext && strcmp(entry->optname, option) == 0)
			return true;
	}
	return false;
}

void checkHdfsCombin(List *options_list, Oid catalog)
{
	ListCell *cell;
	char* hdfs_namenodes = NULL;
	char* hdfs_port = NULL;
	char* hdfs_auth_method = NULL;
	char* krb_principal = NULL;
	char* krb_principal_keytab = NULL;
	char* hadoop_rpc_protection = NULL;
	char* data_transfer_protocol = NULL;
	bool is_ha_supported = false;
	char* dfs_nameservices = NULL;
	char* dfs_ha_namenodes = NULL;
	char* dfs_namenode_rpc_address = NULL;
	char* dfs_client_failover_proxy_provider = NULL;

	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_NAMENODE) == 0)
		{
			hdfs_namenodes = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_PORT) == 0)
		{
			hdfs_port = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_HDFS_AUTH_METHOD) == 0)
		{
			hdfs_auth_method = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRB_PRINCIPAL) == 0)
		{
			krb_principal = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_KRP_PRINCIPAL_KEYTAB) == 0)
		{
			krb_principal_keytab = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_HADOOP_RPC_PROTECTION) == 0)
		{
			hadoop_rpc_protection = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTOCOL) == 0)
		{
			data_transfer_protocol = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_IS_HA_SUPPORTED) == 0)
		{
			if (pg_strcasecmp(defGetString(def), "true") == 0)
			{
				is_ha_supported = true;
			}
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_NAME_SERVICES) == 0)
		{
			dfs_nameservices = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_HA_NAMENODE) == 0)
		{
			dfs_ha_namenodes = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_NAMENODE_RPC_ADDR) == 0)
		{
			dfs_namenode_rpc_address = defGetString(def);
		}

		if (pg_strcasecmp(def->defname, DATALAKE_OPTION_HDFS_DFS_CLIENT_FAILOVER) == 0)
		{
			dfs_client_failover_proxy_provider = defGetString(def);
		}
	}

	if (pg_strcasecmp(hdfs_auth_method, DATALAKE_OPTION_HDFS_AUTH_KERBEROS) == 0)
	{
		if ((krb_principal == NULL))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs kerberos options need specify \"krb_principal\".")));
		}
		if (krb_principal_keytab == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs kerberos options need specify \"krb_principal_keytab\".")));
		}
	}
	else if (pg_strcasecmp(hdfs_auth_method, DATALAKE_OPTION_HDFS_AUTH_SIMPLE) == 0)
	{
		if ((krb_principal != NULL))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs simple options no need specify \"krb_principal\".")));
		}
		if (krb_principal_keytab != NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs simple options no need specify \"krb_principal_keytab\".")));
		}
	}

	if (is_ha_supported)
	{
		if (hdfs_port != NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha no need specify options \"hdfs_port\". "
						"datalake visit hdfs cluster is "
						"through dfs_namenode_rpc_address.")));
		}
		if (dfs_nameservices == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha need specify options \"dfs_nameservices\".")));
		}
		if (dfs_ha_namenodes == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha need specify options \"dfs_ha_namenodes\".")));
		}
		if (dfs_namenode_rpc_address == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha need specify options \"dfs_namenode_rpc_address\".")));
		}
		if (dfs_client_failover_proxy_provider == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("hdfs-ha need specify options \"dfs_client_failover_proxy_provider\".")));
		}
	}
}

void DatalakeGetGopherSocketPath(char *dest)
{
	snprintf(dest, 1024, "/tmp/.s.gopher.%d", PostPortNumber);
}

void DatalakeGetGopherPlasmaSocketPath(char *dest)
{
	snprintf(dest, 1024, "/tmp/.s.gopher.plasma.%d", PostPortNumber);
}

void DatalakeGetGopherMetaPath(char *dest)
{
	sprintf(dest, "%s/%s", DataDir, DATALAKE_GOPHERMETA_FOLDER);
}

void checkValidRecordBatchOpt(dataLakeOptions *options)
{
	if (FORMAT_IS_ORC(options->format))
	{

		if (options->hiveOption->transactional)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("vectorization not support orc transactional table.")));
		}
	}
	else
	{
		ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						errmsg("vectorization only support format orc.")));
	}
}
