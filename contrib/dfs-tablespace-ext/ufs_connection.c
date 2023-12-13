#include "postgres.h"
#include "miscadmin.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "utils/memutils.h"
#include "utils/inval.h"
#include "foreign/foreign.h"

#include "dfs_utils.h"
#include "dfs_option.h"
#include "ufs_connection.h"

typedef struct ConnCacheEntry
{
	char      key[MAX_DFS_PATH_SIZE];
	gopherFS  connection;
	bool      invalidated;
	int32     hashvalue;
} ConnCacheEntry;

static HTAB *connectionHash = NULL;

static gopherConfig *
gopherCreateConfig(DfsOption *option, const char *path)
{
	int i;
	ListCell *lc;
	DfsHdfsOption *hdfsConf = (DfsHdfsOption *) option;
	gopherConfig *config = (gopherConfig *) palloc0(sizeof(gopherConfig));

	config->connect_path = (char *) GetGopherSockertPath();
	config->connect_plasma_path = (char *) GetGopherPlasmaSocketPath();
	config->worker_path = (char *) GetGopherMetaDataPath();
	config->cache_strategy = GOPHER_CACHE;
	config->ufs_type = OptionGetProtocolType(option);

	if (config->ufs_type != HDFS)
	{
		DfsOssOption *ossConf = (DfsOssOption *) option;

		splitPath(path, &config->bucket, &config->uriPrefix);
		config->access_key = ossConf->accessKey;
		config->secret_key = ossConf->secretKey;
		config->endpoint = ossConf->endpoint;
		config->region = ossConf->region;
		config->useHttps = ossConf->enableHttps;
		config->useVirtualHost = ossConf->enableVirtualHost;
		config->oss_min_delay_time = 80;
		config->logLevel = GOPHER_WARNING;
		config->liboss2LogSeverity = GOPHER_LIBOSS2_WARNING;

		return config;
	}

	config->uriPrefix = pstrdup(path);
	config->name_node = hdfsConf->nameNode;
	config->port = atoi(hdfsConf->port);
	config->auth_method = hdfsConf->authMethod;
	config->hadoop_rpc_protection = hdfsConf->hadoopRpcProtection;
	config->krb_principal = hdfsConf->krbPrincipal;
	config->krb_server_key_file = hdfsConf->krbPrincipalKeytab;
	config->krb5_ticket_cache_path = hdfsConf->krb5CCName;
	config->is_ha_supported = hdfsConf->enableHa;
	config->data_transfer_protocol = hdfsConf->dataTransferProtocol;
	config->hdfs_ha_configs_num = list_length(hdfsConf->dfsInfos);

	if (config->hdfs_ha_configs_num > 0)
	{
		config->hdfs_ha_configs =
			(HdfsHAConfig*) palloc0(sizeof(HdfsHAConfig) * config->hdfs_ha_configs_num);

		foreach_with_count(lc, hdfsConf->dfsInfos, i)
		{
			OptionKeyValue *entry = (OptionKeyValue *) lfirst(lc);

			config->hdfs_ha_configs[i].key = entry->key;
			config->hdfs_ha_configs[i].value = entry->value;
		}
	}

	return config;
}

static void
gopherDestroyConfig(gopherConfig *conf)
{
	int i;

	if (conf->uriPrefix != NULL)
		pfree(conf->uriPrefix);

	if (conf->name_node != NULL)
		pfree(conf->name_node);

	if (conf->auth_method != NULL)
		pfree(conf->auth_method);

	if (conf->hadoop_rpc_protection != NULL)
		pfree(conf->hadoop_rpc_protection);

	if (conf->krb_principal != NULL)
		pfree(conf->krb_principal);

	if (conf->krb_server_key_file != NULL)
		pfree(conf->krb_server_key_file);

	if (conf->krb5_ticket_cache_path != NULL)
		pfree(conf->krb5_ticket_cache_path);

	if (conf->bucket != NULL)
		pfree(conf->bucket);

	if (conf->access_key != NULL)
		pfree(conf->access_key);

	if (conf->secret_key != NULL)
		pfree(conf->secret_key);

	if (conf->endpoint != NULL)
		pfree(conf->endpoint);

	if (conf->region != NULL)
		pfree(conf->region);

	if (conf->hdfs_ha_configs_num > 0)
	{
		for (i = 0; i < conf->hdfs_ha_configs_num; i++)
		{
			if (conf->hdfs_ha_configs[i].key)
				pfree(conf->hdfs_ha_configs[i].key);
			if (conf->hdfs_ha_configs[i].value)
				pfree(conf->hdfs_ha_configs[i].value);
		}
	}

	if (conf->hdfs_ha_configs != NULL)
		pfree(conf->hdfs_ha_configs);

	pfree(conf);
}

static bool
checkInterrupt(void)
{
	if (!InterruptPending)
		return false;

	if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
		return false;

	return true;
}

static void
makeConnection(ConnCacheEntry *entry, const char *serverName, const char *path)
{
	UserMapping   *user;
	ForeignServer *server;
	List *options = NIL;
	DfsOption *option;
	gopherConfig *gopherConfig;

	server = GetForeignServerByName(serverName, false);
	user = GetUserMapping(GetUserId(), server->serverid);

	options = list_concat(options, server->options);
	options = list_concat(options, user->options);

	option = OptionParseOptions(options);

	entry->invalidated = false;
	entry->hashvalue = GetSysCacheHashValue1(USERMAPPINGOID, ObjectIdGetDatum(user->umid));

	gopherConfig = gopherCreateConfig(option, path);
	gopherUserCanceledCallBack(&checkInterrupt);

	entry->connection = gopherConnect(*gopherConfig);
	if (entry->connection == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to connect to gopher: %s", gopherGetLastError())));

	gopherDestroyConfig(gopherConfig);
	FreeOption(option);
}

static void
closeConnection(ConnCacheEntry *entry)
{
	if (entry->connection != NULL)
	{
		gopherDisconnect(entry->connection);
		entry->connection = NULL;
	}
}

static void
invalCallback(Datum arg, int cacheId, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheId == USERMAPPINGOID);

	hash_seq_init(&scan, connectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->connection == NULL)
			continue;

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
		   (cacheId == USERMAPPINGOID && entry->hashvalue == hashvalue))
			entry->invalidated = true;
	}
}

gopherFS
UfsGetConnection(const char *serverName, const char *path)
{
	bool found;
	ConnCacheEntry *entry;
	char searchKey[MAX_DFS_PATH_SIZE];

	if (connectionHash == NULL)
	{
		HASHCTL hashCtl;

		MemSet(&hashCtl, 0, sizeof(hashCtl));
		hashCtl.keysize = NAMEDATALEN;
		hashCtl.entrysize = sizeof(ConnCacheEntry);
		hashCtl.hcxt = CacheMemoryContext;
		connectionHash = hash_create("gopher connections", 8, &hashCtl,
									 HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

		CacheRegisterSyscacheCallback(USERMAPPINGOID, invalCallback, (Datum) 0);
	}

	snprintf(searchKey, MAX_DFS_PATH_SIZE, "%s.%s", serverName, path);

	entry = (ConnCacheEntry *) hash_search(connectionHash,
										   searchKey,
										   HASH_ENTER,
										   &found);
	if (!found)
		entry->connection = NULL;

	if (entry->connection != NULL &&
		entry->invalidated)
		closeConnection(entry);

	if (entry->connection == NULL)
		makeConnection(entry, serverName, path);

	return entry->connection;
}
