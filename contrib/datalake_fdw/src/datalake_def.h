#ifndef DATALAKE_DEF_H
#define DATALAKE_DEF_H

#include "postgres.h"
#include "access/htup.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "nodes/bitmapset.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "commands/copy.h"


#include "src/provider/providerWrapper.h"

/* server protocl type */
#define DATALAKE_OPTION_PROTOCOL "protocol"
#define DATALAKE_OSS_PROTOCOL_ALI "ali"
#define DATALAKE_OSS_PROTOCOL_COS "cos"
#define DATALAKE_OSS_PROTOCOL_QINGSTORE "qs"
#define DATALAKE_OSS_PROTOCOL_S3 "s3"
#define DATALAKE_OSS_PROTOCOL_S3B "s3b"
#define DATALAKE_OSS_PROTOCOL_HUAWEI "huawei"
#define DATALAKE_OSS_PROTOCOL_KS3 "ks3"
#define DATALAKE_HDFS_PROTOCOL "hdfs"

/* server oss options */
#define DATALAKE_OPTION_HOST "host"
#define DATALAKE_OPTION_PORT "port"
#define DATALAKE_OPTION_ISVIRTUAL "isvirtual"
#define DATALAKE_OPTION_ISHTTPS "ishttps"

/* server hdfs options */
#define DATALAKE_OPTION_HDFS_NAMENODE "hdfs_namenodes"
#define DATALAKE_OPTION_HDFS_PORT "hdfs_port"
#define DATALAKE_OPTION_HDFS_HDFS_AUTH_METHOD "hdfs_auth_method"
#define DATALAKE_OPTION_HDFS_KRB_PRINCIPAL "krb_principal"
#define DATALAKE_OPTION_HDFS_KRP_PRINCIPAL_KEYTAB "krb_principal_keytab"
#define DATALAKE_OPTION_HDFS_HADOOP_RPC_PROTECTION "hadoop_rpc_protection"
#define DATALAKE_OPTION_HDFS_DATA_TRANSFER_PROTOCOL "data_transfer_protocol"
#define DATALAKE_OPTION_HDFS_IS_HA_SUPPORTED "is_ha_supported"
#define DATALAKE_OPTION_HDFS_DFS_NAME_SERVICES "dfs_nameservices"
#define DATALAKE_OPTION_HDFS_DFS_HA_NAMENODE "dfs_ha_namenodes"
#define DATALAKE_OPTION_HDFS_DFS_NAMENODE_RPC_ADDR "dfs_namenode_rpc_address"
#define DATALAKE_OPTION_HDFS_DFS_CLIENT_FAILOVER "dfs_client_failover_proxy_provider"

#define DATALAKE_OPTION_HDFS_AUTH_SIMPLE "simple"
#define DATALAKE_OPTION_HDFS_AUTH_KERBEROS "kerberos"

/* user mapping options */
#define DATALAKE_OPTION_ACCESKEY "accesskey"
#define DATALAKE_OPTION_SECRETKEY "secretkey"
#define DATALAKE_OPTION_USER "user"

/* foreign table options */
#define DATALAKE_OPTION_COMPRESS "compression"
#define DATALAKE_OPTION_FILEPATH "filepath"
#define DATALAKE_OPTION_FORMAT "format"
#define DATALAKE_OPTION_FORMAT_TEXT "text"
#define DATALAKE_OPTION_FORMAT_CSV "csv"
#define DATALAKE_OPTION_FORMAT_ORC "orc"
#define DATALAKE_OPTION_FORMAT_HUDI "hudi"
#define DATALAKE_OPTION_FORMAT_ICEBERG "iceberg"
#define DATALAKE_OPTION_FORMAT_PARQUET "parquet"
#define DATALAKE_OPTION_FILE_SIZE_LIMIT "filesizelimit"
#define DATALAKE_OPTION_ENABLE_CACHE "enablecache"
#define DATALAKE_OPTION_HIVE_DATASOURCE "datasource"
#define DATALAKE_OPTION_HIVE_PARTITIONKEY "partitionkeys"
#define DATALAKE_OPTION_HIVE_TRANSACTIONAL "transactional"
#define DATALAKE_OPTION_HIVE_CLUSTER_NAME "hive_cluster_name"
#define DATALAKE_OPTION_HDFS_CLUSTER_NAME "hdfs_cluster_name"

/* foreign table options compression */
#define DATALAKE_COMPRESS_UNCOMPRESS "none"
#define DATALAKE_COMPRESS_SNAPPY "snappy"
#define DATALAKE_COMPRESS_GZIP "gzip"
#define DATALAKE_COMPRESS_ZSTD "zstd"
#define DATALAKE_COMPRESS_LZ4 "lz4"

/* copy options */
#define DATALAKE_COPY_OPTION_FORMAT "format"
#define DATALAKE_COPY_OPTION_HEADER "header"
#define DATALAKE_COPY_OPTION_DELIMITER "delimiter"
#define DATALAKE_COPY_OPTION_QUOTE "quote"
#define DATALAKE_COPY_OPTION_ESCAPE "escape"
#define DATALAKE_COPY_OPTION_NULL "null"
#define DATALAKE_COPY_OPTION_ENCODING "encoding"
#define DATALAKE_COPY_OPTION_NEWLINE "newline"
#define DATALAKE_COPY_OPTION_FILL_MISSING_FIELDS "fill_missing_fields"
#define DATALAKE_COPY_OPTION_FORCE_NOT_NULL "force_not_null"
#define DATALAKE_COPY_OPTION_FORCE_NULL "force_null"

#define FORMAT_IS_CSV(format) (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_CSV) == 0)

#define FORMAT_IS_TEXT(format) (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_TEXT) == 0)

#define FORMAT_IS_ORC(format) (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_ORC) == 0)

#define FORMAT_IS_PARQUET(format) (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_PARQUET) == 0)

#define FORMAT_IS_HUDI(format) (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_HUDI) == 0)

#define FORMAT_IS_ICEBERG(format) (pg_strcasecmp(format, DATALAKE_OPTION_FORMAT_ICEBERG) == 0)

#define PROTOCOL_IS_HDFS(protocol) (pg_strcasecmp(protocol, DATALAKE_HDFS_PROTOCOL) == 0)

#define SUPPORT_PARTITION_TABLE(protocol, format, partitionkey, datasource) \
	PROTOCOL_IS_HDFS(protocol) && (FORMAT_IS_ORC(format) || \
	FORMAT_IS_PARQUET(format)) && (partitionkey != NULL) && (datasource != NULL) \



enum PrivatePartitionDataIndex
{
	PrivatePartionString = 0,
	PrivatePartionStringLength,
};

enum PrivatePartitionIndex
{
	PrivatePartitionData = 2,
	/* hive partition fragment lists */
	PrivatePartitionFragmentLists
};

enum FdwScanPrivateIndex
{
	FdwScanHdfsConfig,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* List of fragments to be processed by the segments */
	FdwScanPrivateFragmentList,
};

/*
 * Structure to store the datalake options */
typedef struct pg_HdfsHAConfig
{
    char *key;
    char *value;
}pg_HdfsHAConfig;

typedef struct gopherOptions
{
	/* Gopher config */
	char*	worker_path;
	char*	connect_path;
	char*	connect_plasma_path;
	char*	gopherType;
	char*	bucket;
	bool	useVirtualHost;
	bool	useHttps;
	char*	protocol;
	char*	accessKey;
	char*	secretKey;
	int		port;
	char*	host;
	bool	enableCache;

	/* hdfs config */
	char* 	hdfs_namenode_host;
	int 	hdfs_namenode_port;
	char*	hdfs_auth_method;
	char*	krb_principal;
	char*	krb_principal_keytab;
	char*	krb5_ccname;
	char*	hadoop_rpc_protection;
	bool	data_transfer_protocol;

	/* hdfs ha config  */
	bool	is_ha_supported;
	char*	dfs_name_services;
	char*	dfs_ha_namenodes;
	char*	dfs_ha_namenode_rpc_addr;
	char*	dfs_client_failover;
	int		hdfs_ha_configs_num;
}gopherOptions;

typedef struct hiveOptions
{
	/* hive partition table key */
	List	*hivePartitionKey;
	/* hive partition table values */
	List	*hivePartitionValues;
	/* store json partition values for segment, seg used it need parse json */
	char	*storePartitionInfo;
	/* store json partition values buffer size */
	int		storePartitionInfoLen;
	/* hive orc transaction table */
	bool	transactional;
	/* partition constraints */
	List 	*hivePartitionConstraints;
	List 	*attNums;
	List 	*constraints;
	int 	curPartition;
	char 	*orignPrefix;
	/* hive table name */
	char	*datasource;
	/* whether hive table is partition table */
	bool	partitiontable;
}hiveOptions;

typedef struct dataLakeOptions
{
	gopherOptions* gopher;
	/* sync hive options */
	hiveOptions* hiveOption;
	char		*hive_cluster_name;
	char		*hdfs_cluster_name;
	/* get FOREIGN TABLE option filePath */
	char		*filePath;
	char		*format;
	char		*compress;
	/* parser filePath get prefix */
	char		*prefix;
	bool		readFdw;
	int64_t 	fileSizeLimit;
	bool		vectorization;
} dataLakeOptions;

typedef struct dataLakeFdwPlanState
{
	List	   *retrieved_attrs;
	Bitmapset  *attrs_used;
}dataLakeFdwPlanState;

typedef struct dataLakeCopyState
{
#if (PG_VERSION_NUM < 140000)
	CopyState	cstate_scan;
	CopyState	cstate_modify;
#else
	CopyFromState	cstate_scan;
	CopyToState		cstate_modify;
#endif
}dataLakeCopyState;

/*
 * Execution state of a foreign scan using datalake_fdw.
 */
typedef struct dataLakeFdwScanState
{
	dataLakeOptions 	*options;
	providerWrapper 	provider;
	Relation			rel;
	List 				*fragments;
	Bitmapset  			*attrs_used;     /* attributes actually used in query */
	List				*retrieved_attrs;
	MemoryContext		rowcontext;
	MemoryContext		initcontext;
	List  				*quals;
	dataLakeCopyState	cstate;
} dataLakeFdwScanState;


typedef struct fragmentData
{
	char* filePath;
	int64_t length;
	bool directory;
}fragmentData;

#endif							/* DATALAKE_DEF_H */