#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include <yaml.h>
#include <stdio.h>
#include "config.h"
// #include "datalake_extension.h"

#define GOPHERMETA_FOLDER "gophermeta"

extern bool disableCacheFile;

typedef struct
{
	const char *optname;
	int			offset;
} config_elt;

static const config_elt configElts[] = {
	{"hdfs_namenode_host", offsetof(HdfsConfigInfo, namenodeHost)},
	{"hdfs_namenode_port", offsetof(HdfsConfigInfo, namenodePort)},
	{"hdfs_auth_method", offsetof(HdfsConfigInfo, authMethod)},
	{"krb_principal", offsetof(HdfsConfigInfo, krbPrincipal)},
	{"krb_principal_keytab", offsetof(HdfsConfigInfo, krbPrincipalKeytab)},
	{"hadoop_rpc_protection", offsetof(HdfsConfigInfo, hadoopRpcProtection)},
	{"data_transfer_protocol", offsetof(HdfsConfigInfo, dataTransferProtocol)},
	{"is_ha_supported", offsetof(HdfsConfigInfo, enableHa)}
};

static int
lookupConfigEntry(const char *keyName)
{
	int  i;

	for (i = 0; i < lengthof(configElts); i++)
	{
		if (pg_strcasecmp(configElts[i].optname, keyName) == 0)
			return i;
	}

	return -1;
}

static void
GetGopherMetaPath(char *dest)
{
	sprintf(dest, "%s/%s", DataDir, GOPHERMETA_FOLDER);
}

void
formKrbCCName(HdfsConfigInfo *config)
{
	int i;
	int len;
	char gopherPath[MAXPGPATH];
	char ccName[MAXPGPATH];
	char *krb5ccStr;

	GetGopherMetaPath(gopherPath);
	snprintf(ccName, MAXPGPATH, "%s/krb5cc_%s", gopherPath, config->krbPrincipal);

	krb5ccStr = strstr(ccName, "krb5cc");
	len = strlen(krb5ccStr);
	for (i = 0; i < len; i++)
	{
		if (!isalnum(krb5ccStr[i]))
			krb5ccStr[i] = '_';
	}

	config->krb5CCName = pstrdup(ccName);
	config->gopherPath = pstrdup(gopherPath);
}

HdfsConfigInfo *
parseHdfsConfig(const char *configFile, const char *serverName)
{
	int               i;
	FILE             *fp;
	yaml_parser_t     parser;
	yaml_document_t   document;
	yaml_node_t      *root;
	yaml_node_t      *server;
	yaml_node_t      *node;
	yaml_node_pair_t *tnp;
	yaml_node_pair_t *nnp;
	HdfsConfigInfo   *result = NULL;
	char             *eltPos;

	if (!yaml_parser_initialize(&parser))
		elog(ERROR, "could not create yaml parser \"%s\": out of memory", configFile);

	fp = fopen(configFile, "rb");
	if (fp == NULL)
		elog(ERROR, "could not open file \"%s\": %m", configFile);

	yaml_parser_set_input_file(&parser, fp);
	if (!yaml_parser_load(&parser, &document))
	{
		fclose(fp);
		elog(ERROR, "failed to load yaml \"%s\": %s [%lu, %lu]",
				configFile,
				parser.problem,
				parser.problem_mark.line,
				parser.problem_mark.column + 1);
	}
	fclose(fp);

	root = yaml_document_get_root_node(&document);
	if (!root)
		elog(ERROR, "failed to parse \"%s\": no root node", configFile);

	if (root->type != YAML_MAPPING_NODE)
		elog(ERROR, "failed to parse \"%s\": root node must be mapping node", configFile);

	for (tnp = root->data.mapping.pairs.start; tnp < root->data.mapping.pairs.top; tnp++)
	{
		HdfsConfigInfo *hci;

		node = yaml_document_get_node(&document, tnp->key);
		if (pg_strcasecmp((const char *) node->data.scalar.value, serverName) != 0)
			continue;

		hci = palloc0(sizeof(HdfsConfigInfo));
		server = yaml_document_get_node(&document, tnp->value);
		if (server->type != YAML_MAPPING_NODE)
			elog(ERROR, "failed to parse \"%s\": server node of \"%s\" must be mapping node",
					configFile, (const char *) node->data.scalar.value);

		for (nnp = server->data.mapping.pairs.start; nnp < server->data.mapping.pairs.top; nnp++)
		{
			yaml_node_t *nodeKey;
			yaml_node_t *nodeValue;

			nodeKey = yaml_document_get_node(&document, nnp->key);
			nodeValue = yaml_document_get_node(&document, nnp->value);
			if (nodeValue->type != YAML_SCALAR_NODE)
				elog(ERROR, "failed to parse \"%s\": value of key \"%s\" must be scalar node",
						configFile, (const char *) nodeKey->data.scalar.value);

			i = lookupConfigEntry((const char *) nodeKey->data.scalar.value);
			if (i >= 0)
			{
				eltPos = ((char *) hci) + configElts[i].offset;
				*(char **) eltPos = pnstrdup((const char *) nodeValue->data.scalar.value, nodeValue->data.scalar.length);
			}
			else
			{
				HdfsHAConfEntry *ent = palloc0(sizeof(HdfsHAConfEntry));

				ent->key = pnstrdup((const char *) nodeKey->data.scalar.value, nodeKey->data.scalar.length);
				ent->value = pnstrdup((const char *) nodeValue->data.scalar.value, nodeValue->data.scalar.length);
				hci->haEntries = lappend(hci->haEntries, ent);
			}
		}

		result = hci;
		break;
	}

	yaml_document_delete(&document);
	yaml_parser_delete(&parser);

	return result;
}

gopherConfig *
gopherCreateConfig(HdfsConfigInfo *hdfsConf)
{
	gopherConfig *config = (gopherConfig *) palloc0(sizeof(gopherConfig));

	config->connect_path = hdfsConf->gopherPath;
	config->cache_strategy = GOPHER_CACHE;
	config->ufs_type = HDFS;
	config->name_node = hdfsConf->namenodeHost;
	config->port = atoi(hdfsConf->namenodePort);
	config->auth_method = hdfsConf->authMethod;
	config->krb_principal = hdfsConf->krbPrincipal;
	config->krb_server_key_file = hdfsConf->krbPrincipalKeytab;
	config->krb5_ticket_cache_path = hdfsConf->krb5CCName;
	config->hadoop_rpc_protection = hdfsConf->hadoopRpcProtection;

	if (disableCacheFile)
		config->cache_strategy = GOPHER_NOT_CACHE;

	if (hdfsConf->enableHa && pg_strcasecmp(hdfsConf->enableHa, "true") == 0)
		config->is_ha_supported = true;

	if (hdfsConf->dataTransferProtocol && pg_strcasecmp(hdfsConf->dataTransferProtocol, "true") == 0)
		config->data_transfer_protocol = true;
    
	config->hdfs_ha_configs_num = list_length(hdfsConf->haEntries);
	if (config->hdfs_ha_configs_num > 0)
	{
		int i = 0;
		ListCell *lc = NULL;

		config->hdfs_ha_configs =
				(HdfsHAConfig*) palloc0(sizeof(HdfsHAConfig) * config->hdfs_ha_configs_num);

		foreach_with_count(lc, hdfsConf->haEntries, i)
		{
			HdfsHAConfEntry *entry = (HdfsHAConfEntry *) lfirst(lc);

			config->hdfs_ha_configs[i].key = entry->key;
			config->hdfs_ha_configs[i].value = entry->value;
		}
	}

	return config;
}

void
gopherConfigDestroy(gopherConfig *conf)
{
	int i;

	pfree(conf->connect_path);

	if (conf->name_node != NULL)
		pfree(conf->name_node);

	if (conf->auth_method != NULL)
		pfree(conf->auth_method);

	if (conf->krb5_ticket_cache_path != NULL)
		pfree(conf->krb5_ticket_cache_path);

	if (conf->krb_server_key_file != NULL)
		pfree(conf->krb_server_key_file);

	if (conf->krb_principal != NULL)
		pfree(conf->krb_principal);

	if (conf->hadoop_rpc_protection != NULL)
		pfree(conf->hadoop_rpc_protection);

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
