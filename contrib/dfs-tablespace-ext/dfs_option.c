#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "utils/builtins.h"
#include "utils/varlena.h"

#include <gopher/gopher.h>
#include "dfs_utils.h"
#include "dfs_option.h"

typedef struct ConfigOption
{
	const char  *name;
	relopt_type  kind;
	int          offset;
} ConfigOption;

typedef struct ProtocolConfig
{
	const char *name;
	UFS_TYPE    type;
} ProtocolConfig;

static const ConfigOption dfsOssConfig[] = {
	{DFS_SERVER_PROTOCOL, RELOPT_TYPE_STRING, offsetof(DfsOssOption, protocol)},
	{DFS_OSS_SERVER_ENDPOINT, RELOPT_TYPE_STRING, offsetof(DfsOssOption, endpoint)},
	{DFS_OSS_SERVER_REGION, RELOPT_TYPE_STRING, offsetof(DfsOssOption, region)},
	{DFS_OSS_USER_ACCESKEY, RELOPT_TYPE_STRING, offsetof(DfsOssOption, accessKey)},
	{DFS_OSS_USER_SECRETKEY, RELOPT_TYPE_STRING, offsetof(DfsOssOption, secretKey)},
	{DFS_OSS_SERVER_HTTPS, RELOPT_TYPE_BOOL, offsetof(DfsOssOption, enableHttps)},
	{DFS_OSS_SERVER_VIRTUAL_HOST, RELOPT_TYPE_BOOL, offsetof(DfsOssOption, enableVirtualHost)}
};

static const ConfigOption dfsHdfsConfig[] = {
	{DFS_SERVER_PROTOCOL, RELOPT_TYPE_STRING, offsetof(DfsHdfsOption, protocol)},
	{DFS_HDFS_SERVER_NAMENODE, RELOPT_TYPE_STRING, offsetof(DfsHdfsOption, nameNode)},
	{DFS_HDFS_SERVER_PORT, RELOPT_TYPE_STRING, offsetof(DfsHdfsOption, port)},
	{DFS_HDFS_SERVER_HADOOP_RPC_PROTECTION, RELOPT_TYPE_STRING, offsetof(DfsHdfsOption, hadoopRpcProtection)},
	{DFS_HDFS_SERVER_DATA_TRANSFER_PROTOCOL, RELOPT_TYPE_BOOL, offsetof(DfsHdfsOption, dataTransferProtocol)},
	{DFS_HDFS_USER_AUTH_METHOD, RELOPT_TYPE_STRING, offsetof(DfsHdfsOption, authMethod)},
	{DFS_HDFS_USER_KRB_PRINCIPAL, RELOPT_TYPE_STRING, offsetof(DfsHdfsOption, krbPrincipal)},
	{DFS_HDFS_USER_KRB_PRINCIPAL_KEYTAB, RELOPT_TYPE_STRING, offsetof(DfsHdfsOption, krbPrincipalKeytab)},
	{DFS_HDFS_SERVER_IS_HA_SUPPORTED, RELOPT_TYPE_BOOL, offsetof(DfsHdfsOption, enableHa)}
};

static const ProtocolConfig dfsProtocols[] = {
	{DFS_OSS_PROTOCOL_S3A, S3A},
	{DFS_OSS_PROTOCOL_QINGSTOR, QINGSTOR},
	{DFS_OSS_PROTOCOL_HUAWEI, HUAWEI},
	{DFS_OSS_PROTOCOL_S3AV2, S3AV2},
	{DFS_OSS_PROTOCOL_COS, QCLOUD},
	{DFS_OSS_PROTOCOL_OSS, OSS},
	{DFS_OSS_PROTOCOL_KSYUN, KSYUN},
	{DFS_OSS_PROTOCOL_QINIU, QINIU},
	{DFS_OSS_PROTOCOL_UCLOUD, UCLOUD},
	{DFS_OSS_PROTOCOL_SWIFT, SWIFT},
	{DFS_HDFS_PROTOCOL_HDFS, HDFS}
};

static const char *
getProtocolList(void)
{
	int i;
	StringInfoData protocols;

	initStringInfo(&protocols);

	for (i = 0; i < lengthof(dfsProtocols); i++)
	{
		appendStringInfo(&protocols, "%s,", dfsProtocols[i].name);
	}

	protocols.data[protocols.len - 1] = '\0';

	return protocols.data;
}

static bool
validateProtocol(const char *protocol)
{
	int i;

	for (i = 0; i < lengthof(dfsProtocols); i++)
	{
		if (pg_strcasecmp(dfsProtocols[i].name, protocol) == 0)
			return true;
	}

	return false;
}

static const char *
getOptionValue(List *options, const char *option)
{
	ListCell *lc;

	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (pg_strcasecmp(def->defname, option) == 0)
			return defGetString(def);
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("option \"%s\" not found", option)));
}

static void
setServerOptionValue(const char *optionName,
					 char *itemPos,
					 char *value,
					 relopt_type dataType)
{
	switch (dataType)
	{
		case RELOPT_TYPE_STRING:
			*((char **) itemPos) = pstrdup(value);
			break;
		case RELOPT_TYPE_BOOL:
			if (!parse_bool(value, (bool *) itemPos))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for boolean option \"%s\": %s", optionName, value)));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("data type \"%d\" of option \"%s\" is not supported", dataType, optionName)));
	}
}

static void
setServerOptions(const ConfigOption *dfsConfig,
				 int numDfsConfig,
				 DfsOption *option,
				 List *options,
				 List **otherOptions)
{
	int i;
	bool found;
	ListCell *lc;

	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		found = false;
		for (i = 0; i < numDfsConfig; i++)
		{
			if (pg_strcasecmp(dfsConfig[i].name, def->defname) == 0)
			{
				found = true;
				setServerOptionValue(dfsConfig[i].name,
									 ((char *) option) + dfsConfig[i].offset,
									 defGetString(def),
									 dfsConfig[i].kind);
				break;
			}
		}

		if (!found && otherOptions != NULL)
			*otherOptions = lappend(*otherOptions, def);
	}
}

static DfsOssOption *
getOssServerOptions(List *options)
{
	DfsOssOption *option = palloc0(sizeof(DfsOssOption));

	setServerOptions(dfsOssConfig, lengthof(dfsOssConfig), (DfsOption *) option, options, NULL);

	if (option->endpoint == NULL || option->endpoint[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" not found", DFS_OSS_SERVER_ENDPOINT)));

	if (option->accessKey == NULL || option->accessKey[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" not found", DFS_OSS_USER_ACCESKEY)));
	if (option->secretKey == NULL || option->secretKey[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" not found", DFS_OSS_USER_SECRETKEY)));

	return option;
}

static char *
formKrbCCName(const char *krbPrincipal)
{
	int i;
	int len;
	char ccName[MAXPGPATH];
	char *krb5ccStr;

	snprintf(ccName, MAXPGPATH, "%s/krb5cc_%s", GetGopherSockertPath(), krbPrincipal);

	krb5ccStr = strstr(ccName, "krb5cc");
	len = strlen(krb5ccStr);
	for (i = 0; i < len; i++)
	{
		if (!isalnum(krb5ccStr[i]))
			krb5ccStr[i] = '_';
	}

	return pstrdup(ccName);
}

static List *
addDfsItem(List *dfsInfos, const char *key, const char *value)
{
	OptionKeyValue *keyValue;

	keyValue = palloc0(sizeof(OptionKeyValue));
	keyValue->key = pstrdup(key);
	keyValue->value = pstrdup(value);

	return lappend(dfsInfos, keyValue);
}

static void
parseHdfsHaOptions(DfsHdfsOption *option, List *otherOptions)
{
	char keyBuffer[128] = {0};
	const char *nameService;
	const char *provider;
	char *tempNameNodes;
	char *tempAddresses;
	ListCell *lcNameNode;
	ListCell *lcAddress;
	List *nameNodeList;
	List *addressList;

	nameService = getOptionValue(otherOptions, DFS_HDFS_SERVER_NAMESERVICES);
	option->dfsInfos = addDfsItem(option->dfsInfos, "dfs.nameservices", nameService);

	snprintf(keyBuffer, sizeof(keyBuffer), "dfs.ha.namenodes.%s", nameService);
	tempNameNodes = pstrdup(getOptionValue(otherOptions, DFS_HDFS_SERVER_HA_NAMENODES));
	option->dfsInfos = addDfsItem(option->dfsInfos, keyBuffer, tempNameNodes);

	snprintf(keyBuffer, sizeof(keyBuffer), "dfs.client.failover.proxy.provider.%s", nameService);
	provider = getOptionValue(otherOptions, DFS_HDFS_SERVER_CLIENT_FAILOVER_PROVIDER);
	option->dfsInfos = addDfsItem(option->dfsInfos, keyBuffer, provider);

	if (!SplitIdentifierString(tempNameNodes, ',', &nameNodeList))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value \"%s\" for option \"%s\": value should be seperated by comma",
						tempNameNodes, DFS_HDFS_SERVER_HA_NAMENODES)));

	tempAddresses = pstrdup(getOptionValue(otherOptions, DFS_HDFS_SERVER_NAMENODE_RPC_ADDR));
	if (!SplitIdentifierString(tempAddresses, ',', &addressList))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value \"%s\" for option \"%s\": value should be seperated by comma",
						tempAddresses, DFS_HDFS_SERVER_NAMENODE_RPC_ADDR)));

	if (list_length(nameNodeList) != list_length(addressList))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("the number of elements in option \"%s\" and option \"%s\" should be equal",
						DFS_HDFS_SERVER_HA_NAMENODES, DFS_HDFS_SERVER_NAMENODE_RPC_ADDR)));

	forboth(lcNameNode, nameNodeList, lcAddress, addressList)
	{
		char *nameNode = (char *) lfirst(lcNameNode);
		char *address = (char *) lfirst(lcAddress);

		snprintf(keyBuffer, sizeof(keyBuffer), "dfs.namenode.rpc-address.%s.%s", nameService, nameNode);
		option->dfsInfos = addDfsItem(option->dfsInfos, keyBuffer, address);
	}

	pfree(tempNameNodes);
	list_free(nameNodeList);
	pfree(tempAddresses);
	list_free(addressList);
}

static DfsHdfsOption *
getHdfsServerOptions(List *options)
{
	List *otherOptions = NIL;
	DfsHdfsOption *option = palloc0(sizeof(DfsHdfsOption));

	setServerOptions(dfsHdfsConfig, lengthof(dfsHdfsConfig), (DfsOption *) option, options, &otherOptions);
	if (option->authMethod)
	{
		if (pg_strcasecmp(option->authMethod, DFS_HDFS_USER_AUTH_KERBEROS) == 0)
		{
			if (option->krbPrincipal == NULL || option->krbPrincipal[0] == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"%s\" not found", DFS_HDFS_USER_KRB_PRINCIPAL)));

			if (option->krbPrincipalKeytab == NULL || option->krbPrincipalKeytab[0] == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"%s\" not found", DFS_HDFS_USER_KRB_PRINCIPAL_KEYTAB)));

			option->krb5CCName = formKrbCCName(option->krbPrincipal);
		}
		else if (pg_strcasecmp(option->authMethod, DFS_HDFS_USER_AUTH_SIMPLE) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid value \"%s\" for option \"%s\": please use \"%s\" or \"%s\"",
							option->authMethod, DFS_HDFS_USER_AUTH_METHOD,
							DFS_HDFS_USER_AUTH_SIMPLE, DFS_HDFS_USER_AUTH_KERBEROS)));
		}
	}

	if (option->enableHa)
		parseHdfsHaOptions(option, otherOptions);

	if (option->nameNode == NULL || option->nameNode[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" not found", DFS_HDFS_SERVER_NAMENODE)));

	if (option->port == NULL || option->port[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" not found", DFS_HDFS_SERVER_PORT)));

	if (option->authMethod == NULL)
		option->authMethod = pstrdup(DFS_HDFS_USER_AUTH_SIMPLE);

	return option;
}

DfsOption *
OptionParseOptions(List *options)
{
	const char *protocol;

	protocol = getOptionValue(options, DFS_SERVER_PROTOCOL);
	if (!validateProtocol(protocol))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value \"%s\" for option \"%s\": please use \"%s\"",
						protocol, DFS_SERVER_PROTOCOL, getProtocolList())));
	
	if (pg_strcasecmp(protocol, DFS_HDFS_PROTOCOL_HDFS) == 0)
		return (DfsOption *) getHdfsServerOptions(options);

	return (DfsOption *) getOssServerOptions(options);
}

int
OptionGetProtocolType(DfsOption *option)
{
	int i;

	for (i = 0; i < lengthof(dfsProtocols); i++)
	{
		if (pg_strcasecmp(dfsProtocols[i].name, option->protocol) == 0)
			return dfsProtocols[i].type;
	}

	return UFS_UNKNOWN;
}

void FreeOption(DfsOption *option)
{
	/* NB: Other optins have been released in gopherConfig */
	pfree(option->protocol);
}
