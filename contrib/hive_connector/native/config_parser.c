#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include <stdio.h>
#include <yaml.h>
#include "config_parser.h"
#include "hive_helper.h"

typedef struct
{
	const char *optname;
	int			offset;
} config_elt;

static const config_elt configElts[] = {
	{"uris", offsetof(ConfigItem, uris)},
	{"auth_method", offsetof(ConfigItem, authMethod)},
	{"krb_service_principal", offsetof(ConfigItem, servicePrincipal)},
	{"krb_client_principal", offsetof(ConfigItem, clientPrincipal)},
	{"krb_client_keytab", offsetof(ConfigItem, clientKeytabFile)},
	{"debug", offsetof(ConfigItem, debug)}
};

static const config_elt hdfsConfigElts[] = {
	{"hdfs_namenode_host", offsetof(HdfsConfigItem, host)},
	{"hdfs_namenode_port", offsetof(HdfsConfigItem, port)},
	{"hdfs_auth_method", offsetof(HdfsConfigItem, authMethod)},
	{"krb_principal", offsetof(HdfsConfigItem, krbPrincipal)},
	{"krb_principal_keytab", offsetof(HdfsConfigItem, krbKeytab)},
	{"hadoop_rpc_protection", offsetof(HdfsConfigItem, hadoopRpcProtection)},
	{"is_ha_supported", offsetof(HdfsConfigItem, enableHA)},
};

void hiveConfCheck(ConfigItem *conf)
{
    if (!conf->authMethod)
    {
        conf->authMethod = pstrdup("simple");
    }
    if (!conf->uris)
    {
        elog(ERROR, "Missing option uris in config %s", conf->name);
    }
    if (!strcmp(conf->authMethod, "kerberos") && (!conf->servicePrincipal || !conf->clientPrincipal || !conf->clientKeytabFile))
    {
        elog(ERROR, "Missing option kerberos in config %s", conf->name);
    }
}

void hdfsConfCheck(HdfsConfigItem *conf)
{
    if (!conf->enableHA)
    {
        conf->enableHA = pstrdup("false");
    }
    if (!conf->authMethod)
    {
        conf->authMethod = pstrdup("simple");
    }
    if (!conf->host)
    {
        elog(ERROR, "Missing option namenode_host in config %s", conf->name);
    }
    if (!strcmp(conf->authMethod, "kerberos") && (!conf->krbKeytab || !conf->krbPrincipal))
    {
        elog(ERROR, "Missing option kerberos in config %s", conf->name);
    }
    if (!strcmp(conf->enableHA, "true"))
    {
        const char *serviceName         = extractServiceName(conf);
        const char *nodeNames           = extractNameNodes(conf, serviceName);
              char *rpcAddrs            = extractRpcAddr(conf, serviceName, nodeNames);
        pfree(rpcAddrs);
    }
	else if (!conf->port)
	{
		elog(ERROR, "Missing option namenode_port");
	}
}

List *
parseConf(const char *configFile, bool isFullMode)
{
	int               i;
	bool              found;
	char              key[1024];
	FILE              *fp;
	yaml_parser_t 	  parser;
	yaml_document_t   document;
	yaml_node_t      *root = NULL;
	yaml_node_t      *server;
	yaml_node_t      *node;
	yaml_node_pair_t *tnp;
	yaml_node_pair_t *nnp;
	List             *result = NIL;
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
		ConfigItem *hci = palloc0(sizeof(ConfigItem));

		node = yaml_document_get_node(&document, tnp->key);
		if (node->data.scalar.length <= 0)
			continue;

		hci->name = pnstrdup((const char *)node->data.scalar.value, node->data.scalar.length);

		server = yaml_document_get_node(&document, tnp->value);
		if (server->type != YAML_MAPPING_NODE)
			elog(ERROR, "failed to parse \"%s\": server node of \"%s\" must be mapping node",
					configFile, hci->name);

		if (!isFullMode)
		{
			result = lappend(result, hci);
			continue;
		}

		for (nnp = server->data.mapping.pairs.start; nnp < server->data.mapping.pairs.top; nnp++)
		{
			node = yaml_document_get_node(&document, nnp->key);
			if (node->data.scalar.length <= 0)
				continue;

			found = false;
			strncpy(key, (const char *)node->data.scalar.value, sizeof(key) -1);
			for (i = 0; i < lengthof(configElts); i++)
			{
				if (!strcmp(configElts[i].optname, (const char *)node->data.scalar.value))
				{
					found = true;
					break;
				}
			}

			if (!found)
				elog(ERROR, "failed to parse \"%s\": unrecognized configuration parameter \"%s\" for server \"%s\"",
							configFile, key, hci->name);

			node = yaml_document_get_node(&document, nnp->value);
			if (node->type != YAML_SCALAR_NODE)
				elog(ERROR, "failed to parse \"%s\": value of \"%s\" must be scalar node",
						configFile, key);

			eltPos = ((char *) hci) + configElts[i].offset;
			*(char **) eltPos = pnstrdup((const char *)node->data.scalar.value, node->data.scalar.length);
		}
		result = lappend(result, hci);
	}

	yaml_document_delete(&document);
	yaml_parser_delete(&parser);

	return result;
}

List *
parseHdfsConf(const char *configFile, bool isFullMode)
{
	int               i;
	bool              found;
	char              key[1024];
	FILE             *fp;
	yaml_parser_t 	  parser;
	yaml_document_t   document;
	yaml_node_t      *root = NULL;		
	yaml_node_t      *server;
	yaml_node_t      *node;
	yaml_node_pair_t *tnp;
	yaml_node_pair_t *nnp;
	List             *result = NIL;
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
		HdfsConfigItem *hci = palloc0(sizeof(HdfsConfigItem));

		node = yaml_document_get_node(&document, tnp->key);
		if (node->data.scalar.length <= 0)
			continue;

		hci->name = pnstrdup((const char *)node->data.scalar.value, node->data.scalar.length);

		server = yaml_document_get_node(&document, tnp->value);
		if (server->type != YAML_MAPPING_NODE)
			elog(ERROR, "failed to parse \"%s\": server node of \"%s\" must be mapping node",
					configFile, hci->name);

		if (!isFullMode)
		{
			result = lappend(result, hci);
			continue;
		}

		for (nnp = server->data.mapping.pairs.start; nnp < server->data.mapping.pairs.top; nnp++)
		{

			yaml_node_t   *nodeKey;
			yaml_node_t   *nodeVal;
			nodeKey = yaml_document_get_node(&document, nnp->key);
			nodeVal = yaml_document_get_node(&document, nnp->value);

			if (nodeKey->data.scalar.length <= 0)
				continue;

			if (nodeVal->type != YAML_SCALAR_NODE)
				elog(ERROR, "failed to parse \"%s\": value of \"%s\" must be scalar node",
						configFile, key);

			found = false;
			strncpy(key, (const char *)nodeKey->data.scalar.value, sizeof(key) -1);
			for (i = 0; i < lengthof(hdfsConfigElts); i++)
			{
				if (!strcmp(hdfsConfigElts[i].optname, (const char *)nodeKey->data.scalar.value))
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				HdfsHAConfEntry *ent = palloc0(sizeof(HdfsHAConfEntry));
				ent->key = pnstrdup((const char *) nodeKey->data.scalar.value, nodeKey->data.scalar.length);
				ent->value = pnstrdup((const char *) nodeVal->data.scalar.value, nodeVal->data.scalar.length);
				hci->haEntries = lappend(hci->haEntries, ent);
			}
			else
			{
				eltPos = ((char *) hci) + hdfsConfigElts[i].offset;
				*(char **) eltPos = pnstrdup((const char *)nodeVal->data.scalar.value, nodeVal->data.scalar.length);
			}
		}
		result = lappend(result, hci);
	}

	yaml_document_delete(&document);
	yaml_parser_delete(&parser);

	return result;
}