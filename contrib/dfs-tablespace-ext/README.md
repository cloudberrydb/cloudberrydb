# DFS Tablespace

Dfs Tablespace is a tablespace that points to a distributed file system. User can create Dfs Tablespace, specify to use Dfs Tablespace when creating table. 

Developer can also use the ufs-api(ufs.h) from DFS Tablespace to access local or remote file system.

## Installation

Typical installation procedure may look like this:

```bash
 $ git clone https://code.hashdata.xyz/cloudberry/dfs-tablespace-ext
 $ cd dfs-tablespace-ext
 $ make USE_PGXS=1
 $ make USE_PGXS=1 install
 $ make USE_PGXS=1 installcheck
```

Before starting working with Dfs Tablespace, adding the following line
to `postgresql.conf` is required.  This change requires a restart of
the CBDB database server.

```
shared_preload_libraries = 'dfs_tablespace'
```

## Setup


 * Create the necessary database objects

```sql
-- OSS
CREATE FOREIGN DATA WRAPPER test_fdw;
CREATE SERVER test_server FOREIGN DATA WRAPPER test_fdw OPTIONS(protocol 'qingstor', endpoint 'pek3b.qingstor.com', https 'true', virtual_host 'false');
CREATE USER MAPPING FOR CURRENT_USER SERVER test_server OPTIONS (accesskey 'KGCPPHVCHRMZMFEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');
CREATE TABLESPACE oss_test location '/tbs-49560-0-mgq-multi/tablespace-oss' with(server='test_server');

-- HDFS
CREATE FOREIGN DATA WRAPPER test_fdw1;
CREATE SERVER test_server1 FOREIGN DATA WRAPPER test_fdw1 OPTIONS(protocol 'hdfs', namenode '192.168.48.201', port '9000');
CREATE USER MAPPING FOR CURRENT_USER SERVER test_server1 OPTIONS (auth_method 'simple');
CREATE TABLESPACE oss_test1 location '/dfs' with(server='test_server1');
create table t1 (val text) tablespace oss_test1;
insert into t1 values(1);
```

 * Implement a new Table Access Method by using ufs-api(ufs.h) from DFS Tablespace

 
 * ufsdemo_am(ufsdemo.c) is an example of using ufs-api to implement a Table Access Method


## Syntax reference

- **OSS server options:**

    `protocol` (required)
    
    
    Specify the protocol for accessing DFS
    
    | cloud platform | protocol |
    | ------ | ------ |
    |    Amazon used signature v4     |    s3a    |
    |   Qingyun object storage     |    qingstor    |
    |   Huawei object storage     |    huawei    |
    |   Amazon used signature v2     |   s3av2     |
    |   Tencent object storage     |    cos    |
    |   Ali object storage    |    oss    |
    |   Kingstor object storage     |    ksyun    |
    |   Qiniu object storage     |    qiniu    |
    |   UCloud object storage     |    ucloud    |
    |   Swift object storage     |    swift    |

    `endpoint` (required)
    
    Specify the access address of the DFS system

    `region` (optional)
    
    Specify the region name of the DFS system
    
     `https`  (optional)
    
    Specify whether to use https, default: false

     `virtual_host `  (optional)
    
    Specify whether to use virtualhost style to access DFS system, default: fase

- **HDFS server options:**

    `protocol` (required)
    
    
    Specify the protocol for accessing DFS
    
    | cloud platform | protocol |
    | ------ | ------ |
    |   HDFS storage     |    hdfs    |
    

    `namenode`  (required)
    
    Specify the address of the namenode

    `port`  (required)
    
    Specify the port of the namenode
    
    `hadoop_rpc_protection` (optional)
    
    Specify HDFS rpc encryption option: authentication, integrity, privacy. default: authentication

    `data_transfer_protocol` (optional)
	Specify whether to activate data encryption for data transfer protocol of DataNode. default: false
    
    Specify the address of the namenode
    
     `is_ha_supported`  (optional)
    
    Specify whether to use HA, default false

     `dfs_nameservices `  (depends on is\_ha_supported)
    
    Specify the HDFS Namesservice name when ha is enabled    

    `dfs_ha_namenodes`  (depends on is\_ha_supported)
    
    Specify the HDFS dfs_ha_namenodes name when ha is enabled
    
    `dfs_namenode_rpc_address` (depends on is\_ha_supported)
    
    Specify the HDFS dfs_namenode_rpc_address name when ha is enabled
    
    `dfs_client_failover_proxy_provider`  (depends on is\_ha_supported)
    
     Specify HDFS client failover provider

    
 - **OSS user options:**

	
    `accesskey`  (required)
    
    Specify the access key of OSS
    
    `secretkey` (optional)
    
    Specify the secret key of OSS
     
    
    
    
 - **HDFS user options:**

    `auth_method`  (required)
    
    Specify HDFS authentication method: simple, kerberos. default: simple
	
    `krb_principal`  (depends on auth_method)
    
    Specify the principal for kerberos authentication
    
    `krb_principal_keytab` (depends on auth_method)
    
    Specify the principal for kerberos authentication
