Hive Connector

#### 概述

Hive Connector通过HiveMetaStoreClient连接Hive Metastore Service（Thrift Service），获取Hive table的元数据，利用获取的元数据使用 CBDB 数据仓库gphdfs外部表来访问Hive数据仓库数据

Hive 普通表同步到 CBDB 数据仓库后成为 gphdfs 外部表

Hive 分区表同步到 CBDB 数据仓库后，成为普通的 gphdfs 外部表，由 gphdfs 外部表插件对 push down 下来的 filter 做 Partition Pruning

#### 支持的Hive文件格式

1. TEXT
2. ORC
3. PARQUET

#### 配置

Hive Connector 支持Hive 2.x, 3.x，使用如下内容在 CBDB 数据仓库主节点创建 gphive.conf 配置文件，将 expample.net:8083 替换成对应的 Hive Metastore Service 地址

```
hive-cluster-1: #connector name
    uris: thrift://example.net:8083
    auth_method: simple

```

#### 多Hive Cluster支持

在 gphive.conf 内新增一个connector name即可，例如如下内容表示新增了一个 hive-cluster-2 的 Hive 集群

```
hive-cluster-1: #simple auth
    uris: thrift://example1.net:9083
    auth_method: simple

hive-cluster-2: #kerberos auth
    uris: thrift://example2.net:9083
    auth_method: kerberos
    krb_service_principal: hive/hashdata@HASHDATA.CN
    krb_client_principal: user/hashdata@HASHDATA.CN
    krb_client_keytab: /home/gpadmin/user.keytab
    
hive-cluster-3: #kerberos auth(HMS HA)
    uris: thrift://hms-primary.example2.net:9083,thrift://hms-standby.example2.net:9083
    auth_method: kerberos
    krb_service_principal: hive/_HOST@HASHDATA.CN
    krb_client_principal: user/hashdata@HASHDATA.CN
    krb_client_keytab: /home/gpadmin/user.keytab
```

#### Hive Connector配置项

| Property Name | Description                                                  | Default |
| ------------- | ------------------------------------------------------------ | ------- |
| uris          | Hive Metastore Service监听地址(使用HMS的主机名)                               |         |
| auth_method   | Hive Metastore Service认证方法: simple或kerberos             | simple  |
| krb\_service_principal | Hive Metastore Service的kerberos认证需要的service principal：使用HMS HA功能时需要将principal内的instance写成_HOST，例如hive/\_HOST@HASHDATA.CN          |         |
| krb\_client_principal | Hive Metastore Service的kerberos认证需要的client principal          |         |
| krb\_client_keytab    | Hive Metastore Service的kerberos认证需要的client principal对应的keytab文件 |         |
| debug         | Hive Connector debug flag：true或false                       | false   |

#### Data Type Mappings

| Hive Data Type |  CBDB  Data Type |
| -------------- | ------------------ |
| binary         | bytea              |
| tinyint        | smallint           |
| smallint       | smallint           |
| int            | int                |
| bigint         | bigint             |
| float          | float4              |
| double         | double precision   |
| string         | text               |
| timestamp      | timestamp          |
| date           | date               |
| char           | char               |
| varchar        | varchar            |
| decimal        | decimal            |

#### 限制

1. 不支持同步 Hive External Table
2. 不支持同步 Hive Table统计信息

#### 函数

- sync_hive_table(hiveClusterName, hiveDatabaseName, hiveTableName, hdfsClusterName, destTableName, serverName)

  > 同步一个Hive表到 CBDB 

- sync_hive_table(hiveClusterName, hiveDatabaseName, hiveTableName, hdfsClusterName, destTableName, serverName, forceSync)

  > 强制同步一个Hive表到 CBDB 

- sync_hive_database(hiveClusterName, hiveDatabaseName, hdfsClusterName, destSchemaName, serverName)

  > 同步一个Hive数据库到 CBDB 

- sync_hive_database(hiveClusterName, hiveDatabaseName, hdfsClusterName, destSchemaName, serverName, forceSync)

  > 强制同步一个Hive数据库到 CBDB 

- create_foreign_server(serverName, userMapName, dataWrapName, hdfsClusterName)

  > 创建一个 server 和 user mapping


#### 使用HMS(Hive Metastore Service)接口说明

sync_hive_table调用HMS的thrift getTable接口

sync_hive_database调用HMS的thrift getTables,getTable接口



#### 例子

0. 创建 Server 和 User Mapping

-- 创建 Foreign Data Wrapper
```
CREATE EXTENSION datalake_fdw;

CREATE FOREIGN DATA WRAPPER datalake_fdw
HANDLER datalake_fdw_handler
VALIDATOR datalake_fdw_validator
OPTIONS (mpp_execute 'all segments');
```

-- 创建 Server 和 User Mapping
```
SELECT public.create_foreign_server('sync_server', 'gpadmin', 'datalake_fdw', 'hdfs-cluster-1');
```

1. 同步一个Hive text table

```
--beeline内创建Hive表
CREATE TABLE weblogs
    (
        client_ip           STRING,
        full_request_date   STRING,
        day                 STRING,
        month               STRING,
        month_num           INT,
        year                STRING,
        referrer            STRING,
        user_agent          STRING
 ) STORED AS TEXTFILE;
 
 --psql内同步Hive表
 select public.sync_hive_table('hive-cluster-1', 'mytestdb', 'weblogs', 'hdfs-cluster-1', 'mytestdb.weblogs', 'sync_server');

```



2. 同步一个Hive csv table

```
--beeline内创建Hive表
CREATE TABLE test_csv_default_option
(
        column_a string,
        column_b string,
        column_c string,
        column_d string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;

--psql内同步表
 select public.sync_hive_table('hive-cluster-1', 'mytestdb', 'test_csv_default_option', 'hdfs-cluster-1', 'mytestdb.test_csv_default_option', 'sync_server');
```



3. 同步一个Hive orc table

```
--beeline内创建Hive表
CREATE TABLE test_all_type
(
    column_a tinyint,
    column_b smallint,
    column_c int,
    column_d bigint,
    column_e float,
    column_f double,
    column_g string,
    column_h timestamp,
    column_i date,
    column_j char(20),
    column_k varchar(20),
    column_l decimal(20, 10)
) STORED AS ORC;

--psql内同步表
 select public.sync_hive_table('hive-cluster-1', 'mytestdb', 'test_all_type', 'hdfs-cluster-1', 'mytestdb.test_all_type', 'sync_server');
```



4. 同步一个hive orc partition table

```
--beeline内创建分区表
CREATE TABLE test_partition_1_int
(
    a tinyint,
    b smallint,
    c int,
    d bigint,
    e float,
    f double,
    g string,
    h timestamp,
    i date,
    j char(20),
    k varchar(20),
    l decimal(20, 10)
)
PARTITIONED BY
(
    m int
)
STORED AS ORC;
INSERT INTO test_partition_1_int VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1);
INSERT INTO test_partition_1_int VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2);
INSERT INTO test_partition_1_int VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3);
INSERT INTO test_partition_1_int VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4);
INSERT INTO test_partition_1_int VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5);

--psql将hive分区表同步成一个gphdfs外部表
gpadmin=# sselect public.sync_hive_table('hive-cluster-1', 'mytestdb', 'test_partition_1_int', 'hdfs-cluster-1', 'mytestdb.test_partition_1_int', 'sync_server');
 sync_hive_table
-----------------
 t
(1 row)

gpadmin=# \d mytestdb.test_partition_1_int;
                    Foreign table "mytestdb.test_partition_1_int"
 Column |            Type             | Collation | Nullable | Default | FDW options
--------+-----------------------------+-----------+----------+---------+-------------
 a      | smallint                    |           |          |         |
 b      | smallint                    |           |          |         |
 c      | integer                     |           |          |         |
 d      | bigint                      |           |          |         |
 e      | double precision            |           |          |         |
 f      | double precision            |           |          |         |
 g      | text                        |           |          |         |
 h      | timestamp without time zone |           |          |         |
 i      | date                        |           |          |         |
 j      | character(20)               |           |          |         |
 k      | character varying(20)       |           |          |         |
 l      | numeric(20,10)              |           |          |         |
 m      | integer                     |           |          |         |
Server: sync_server
FDW options: (filepath 'opt/hadoop/apache-hive-3.1.0-bin/user/hive/warehouse/mytestdb.db/test_partition_1_int', hive_cluster_name 'hive-cluster-1', datasource 'mytestdb.test_partition_1_int', hdfs_cluster_name 'hdfs-cluster-1', enablecache 'true', transactional 'false', partitionkeys 'm', format 'orc')

5. 同步一个Hive database

```
--psql内同步数据库
select public.sync_hive_database('hive-cluster-1', 'default', 'hdfs-cluster-1', 'mytestdb', 'sync_server');
 sync_hive_database
--------------------
 t
(1 row)

set search_path='mytestdb';
\d
                                List of relations
  Schema  |             Name              |       Type        |  Owner  | Storage
----------+-------------------------------+-------------------+---------+---------
 mytestdb | test_all_type                 | foreign table     | gpadmin |
 mytestdb | weblogs                       | foreign table     | gpadmin |
 mytestdb | test_csv_default_option       | foreign table     | gpadmin |
 mytestdb | test_partition_1_int          | foreign table     | gpadmin |
(4 rows)
```

#### 注意事项

sync_hive_database 可能对 Hive Metastore Service 和 CBDB 造成较大压力
