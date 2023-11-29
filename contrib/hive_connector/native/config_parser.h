#ifndef CONFIG_PARSER_H
#define CONFIG_PARSER_H

#include "nodes/pg_list.h"

typedef struct ConfigItem
{
	char *name;
	char *uris;
	char *authMethod;
	char *servicePrincipal;
	char *clientPrincipal;
	char *clientKeytabFile;
	char *debug;
} ConfigItem;

typedef struct HdfsConfigItem
{
	char *name;
	char *host;
	int64_t port;
	char *authMethod;
	char *krbPrincipal;
	char *krbKeytab;
	char *hadoopRpcProtection;
	char *enableHA;
	List *haEntries;
} HdfsConfigItem;

typedef struct HdfsHAConfEntry
{
	char *key;
	char *value;
} HdfsHAConfEntry;

extern void hiveConfCheck(ConfigItem *conf);
extern void hdfsConfCheck(HdfsConfigItem *conf);
extern List *parseConf(const char *configFile, bool isFullMode);
extern List *parseHdfsConf(const char *configFile, bool isFullMode);
#endif // CONFIG_PARSER_H
