#ifndef DFS_OPTION_H
#define DFS_OPTION_H

#include "postgres.h"
#include "nodes/pg_list.h"

#define DFS_SERVER_PROTOCOL         "protocol"
#define DFS_OSS_SERVER_ENDPOINT     "endpoint"
#define DFS_OSS_SERVER_REGION       "region"
#define DFS_OSS_SERVER_HTTPS        "https"
#define DFS_OSS_SERVER_VIRTUAL_HOST "virtual_host"

#define DFS_HDFS_SERVER_NAMENODE                 "namenode"
#define DFS_HDFS_SERVER_PORT                     "port"
#define DFS_HDFS_SERVER_HADOOP_RPC_PROTECTION    "hadoop_rpc_protection"
#define DFS_HDFS_SERVER_DATA_TRANSFER_PROTOCOL   "data_transfer_protocol"
#define DFS_HDFS_SERVER_IS_HA_SUPPORTED          "is_ha_supported"
#define DFS_HDFS_SERVER_NAMESERVICES             "dfs_nameservices"
#define DFS_HDFS_SERVER_HA_NAMENODES             "dfs_ha_namenodes"
#define DFS_HDFS_SERVER_NAMENODE_RPC_ADDR        "dfs_namenode_rpc_address"
#define DFS_HDFS_SERVER_CLIENT_FAILOVER_PROVIDER "dfs_client_failover_proxy_provider"

#define DFS_OSS_USER_ACCESKEY  "accesskey"
#define DFS_OSS_USER_SECRETKEY "secretkey"

#define DFS_HDFS_USER_AUTH_METHOD          "auth_method"
#define DFS_HDFS_USER_KRB_PRINCIPAL        "krb_principal"
#define DFS_HDFS_USER_KRB_PRINCIPAL_KEYTAB "krb_principal_keytab"

#define DFS_OSS_PROTOCOL_S3A       "s3a"
#define DFS_OSS_PROTOCOL_QINGSTOR  "qingstor"
#define DFS_OSS_PROTOCOL_HUAWEI    "huawei"
#define DFS_OSS_PROTOCOL_S3AV2     "s3av2"
#define DFS_OSS_PROTOCOL_COS       "cos"
#define DFS_OSS_PROTOCOL_OSS       "oss"
#define DFS_OSS_PROTOCOL_KSYUN     "ksyun"
#define DFS_OSS_PROTOCOL_QINIU     "qiniu"
#define DFS_OSS_PROTOCOL_UCLOUD    "ucloud"
#define DFS_OSS_PROTOCOL_SWIFT     "swift"
#define DFS_HDFS_PROTOCOL_HDFS     "hdfs"

#define DFS_HDFS_USER_AUTH_SIMPLE   "simple"
#define DFS_HDFS_USER_AUTH_KERBEROS "kerberos"

typedef struct OptionKeyValue
{
	char *key;
	char *value;
} OptionKeyValue;

typedef struct DfsOption {
	char *protocol;
} DfsOption;

typedef struct DfsOssOption
{
	char *protocol;
	char *endpoint;
	char *region;
	char *accessKey;
	char *secretKey;
	bool  enableHttps;
	bool  enableVirtualHost;
} DfsOssOption;

typedef struct DfsHdfsOption
{
	char *protocol;
	char *nameNode;
	char *port;
	char *hadoopRpcProtection;
	char *authMethod;
	char *krbPrincipal;
	char *krbPrincipalKeytab;
	char *krb5CCName;
	List *dfsInfos;
	bool  enableHa;
	bool  dataTransferProtocol;
} DfsHdfsOption;

extern DfsOption *OptionParseOptions(List *options);
extern void FreeOption(DfsOption *option);
extern int OptionGetProtocolType(DfsOption *option);

#endif  /* DFS_OPTION_H */
