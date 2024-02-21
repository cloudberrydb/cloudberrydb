#ifndef GOPHER_CONFIG_H
#define GOPHER_CONFIG_H

#include "postgres.h"
#include "gopher/gopher.h"
#include "nodes/pg_list.h"

typedef struct HdfsHAConfEntry
{
	char *key;
	char *value;
} HdfsHAConfEntry;

typedef struct HdfsConfigInfo
{
	char *gopherPath;
	char *namenodeHost;
	char *namenodePort;
	char *authMethod;
	char *krbPrincipal;
	char *krbPrincipalKeytab;
	char *hadoopRpcProtection;
	char *dataTransferProtocol;
	char *krb5CCName;
	char *enableHa;
	List *haEntries;
} HdfsConfigInfo;

// HdfsConfigInfo *parseConf(const char *configFile, const char *serverName);
HdfsConfigInfo *parseHdfsConfig(const char *configFile, const char *serverName);
void formKrbCCName(HdfsConfigInfo *config);
gopherConfig *gopherCreateConfig(HdfsConfigInfo *hdfsConf);
void gopherConfigDestroy(gopherConfig *conf);

#endif // GOPHER_CONFIG_H
