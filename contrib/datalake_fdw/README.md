# datalake-ext

datalake-ext is foreign data wrapper for PostgreSQL. support read/write oss and hdfs storage.

## Installation

datalake-ext requires `libgopher`, `libparquet`, `liborc`, `libarchive` installed in your system. To build datalake-ext run:
```
make install
```

After extension was successfully installed run in psql:
```
CREATE EXTENSION datalake_fdw;
```


## Basic usage

datalake-ext support visite oss and hdfs storage.

### visite oss storage
visite oss storage example SQL:

1. create a server and user mapping 
    ```
    DROP FOREIGN DATA WRAPPER datalake_fdw;
    CREATE FOREIGN DATA WRAPPER datalake_fdw
    HANDLER datalake_fdw_handler
    VALIDATOR datalake_fdw_validator 
    OPTIONS ( mpp_execute 'all segments' );


    DROP SERVER foreign_server;
    CREATE SERVER foreign_server
            FOREIGN DATA WRAPPER datalake_fdw
            OPTIONS (host 'xxx', protocol 's3b', isvirtual 'false',
            ishttps 'false');
            
    
    DROP USER MAPPING FOR CURRENT_USER SERVER foreign_server;
    CREATE USER MAPPING FOR gpadmin
            SERVER foreign_server
            OPTIONS (user 'gpadmin', accesskey 'xxx', secretkey 'xxx'); 
            
    ```

- **create foreign_server parameter:**  
    `host`  
    Address for accessing the host object storage

    `protocol`   
    Specify a different cloud platform. 
    | cloud platfomr | protocol |
    | ------ | ------ |
    |   Amazon used signature v2     |   s3b     |
    |    Amazon used signature v4     |    s3    |
    |   Ali object storage    |    ali    |
    |   Qingyun object storage     |    qs    |
    |   Tencent object storage     |    cos    |
    |   Huawei object storage     |    huawei    |
    |   Kingstor object storage     |    ks3    |

    `isvirtual`  
    Parse host is used **virutal-host-style** or **path-host-style**

    `ishttps`  
    used https

- **create user mapping parameter:**  

    `user`  
    Specify foreign_server create user

    `accesskey`  
    Specify a accesskey for cloud platform.

    `secretkey`   
    Specify a secretkey for cloud platform.


2. create fdw table
```
CREATE FOREIGN TABLE example (
        a text,
        b text
)
        SERVER foreign_server
        OPTIONS (filePath '/test/parquet/', compression 'none' , enableCache 'false', format 'parquet');
```
`filePath`  
the path of writing or reading foreign table

`compression`   
compressed format, none, snappy, gzip, zstd, lz4

| compress | file format |
| ------ | ------ |
|   none     |   csv,orc,text,parquet     |
|   snappy     |    csv,text,parquet    |
|   gzip     |    csv,text,parquet    |
|   zstd     |    parquet    |
|   lz4     |    parquet    |


`enableCache`  
whether to enable use gopher cache


`format`  
file format, csv, text, orc, parquet

| file format | support |
| ------ | ------ |
|   csv     |     yes   |
|   text     |    yes    |
|   orc     |    yes    |
|   parquet     |    yes    |


### visite hdfs storage
visite hdfs storage example SQL:
#### 1. create a data wrapper
```
DROP FOREIGN DATA WRAPPER datalake_fdw;
CREATE FOREIGN DATA WRAPPER datalake_fdw
  HANDLER datalake_fdw_handler
  VALIDATOR datalake_fdw_validator 
  OPTIONS ( mpp_execute 'all segments' );
```

#### 2. create a server  
##### a. hdfs simple
support hdfs simple  
```
DROP SERVER foreign_server;
CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (
        protocol 'hdfs',
        hdfs_namenodes '192.168.178.95:9000',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication');
```
##### b. hdfs-ha simple 
support hdfs-ha auth method is simple

```
DROP SERVER foreign_server;
CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (
        protocol 'hdfs',
        hdfs_namenodes 'mycluster',
        hdfs_auth_method 'simple',
        hadoop_rpc_protection 'authentication',
        is_ha_supported 'true',
        dfs_nameservices 'mycluster',
        dfs_ha_namenodes 'nn1,nn2,nn3',
        dfs_namenode_rpc_address '192.168.178.95:9000,192.168.178.160:9000,192.168.178.186:9000',
        dfs_client_failover_proxy_provider 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider');
```

##### c. hdfs kerberos
support hdfs method is kerberos
```
DROP SERVER foreign_server;
CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (hdfs_namenodes '192.168.3.32:9000'， 
        protocol 'hdfs', 
        auth_method 'kerberos', 
        krb_principal 'gpadmin/hdw-68212a9a-master0@GPADMINCLUSTER2.COM',
        krb_principal_keytab '/home/gpadmin/hadoop.keytab', 
        hadoop_rpc_protection 'privacy'
        );
```

##### d. hdfs-ha kerberos
support hdfs-ha method is kerberos
```
CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (hdfs_namenodes 'mycluster'， 
        protocol 'hdfs', 
        auth_method 'kerberos', 
        krb_principal 'gpadmin/hdw-68212a9a-master0@GPADMINCLUSTER2.COM',
        krb_principal_keytab '/home/gpadmin/hadoop.keytab', 
        hadoop_rpc_protection 'privacy',
        is_ha_supported 'true',
        dfs_nameservices 'mycluster',
        dfs_ha_namenodes 'nn1,nn2,nn3',
        dfs_namenode_rpc_address '192.168.178.95:9000,192.168.178.160:9000,192.168.178.186:9000',
        dfs_client_failover_proxy_provider 'org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
        );
```


##### hdfs simple, Description of the mode parameters  
`protocol`  
Need specify `hdfs` platform.
  
`hdfs_namenodes`  
Specify hdfs name node host. 
  
`hdfs_auth_method`    
hdfs auth method 
| auth method | 
| ------ | 
|   simple     |
|   kerberos     |
  

`hadoop_rpc_protection`    
hdfs rpc protection
| hdfs rpc protection | 
| ------ | 
|   authentication     |
|   integrity     |
|   privacy     |
  

##### hdfs-ha simple, Description of the mode parameters

Some of the parameters are the same as the simple. Here to say the remaining parameters  
  
`hdfs_namenodes`  
Need Specify hdfs-ha name
  
`is_ha_supported`  
Need set is_ha_supported is true. Tell the fdw to access the hdfs-ha. only is_ha_supported set true, code will read ha parameters.
  
`dfs_nameservices`  
hdfs-ha name
  
`dfs_ha_namenodes`  
hdfs-ha name nodes
  
`dfs_namenode_rpc_address`  
hdfs namenode rpc address
  
`dfs_client_failover_proxy_provider`  
the class name of the configured Failover proxy provider for the host
  

##### hdfs kerberos, Description of the mode parameters
  
`krb_principal`  
you keytab krb principal name
  
`krb_principal_keytab`  
keytab full path
  
##### hdfs-ha kerberos , Description of the mode parameters
is the same as simple-ha and hdfs kerberos.
  


#### 3. create user mapping
```
DROP USER MAPPING FOR CURRENT_USER SERVER foreign_server;
CREATE USER MAPPING FOR gpadmin
        SERVER foreign_server
        OPTIONS (user 'gpadmin'); 
```
`user`  
Specify foreign_server create user

#### 4. create foreign table
```
CREATE FOREIGN TABLE example (
        a text,
        b text
)
        SERVER foreign_server
        OPTIONS (filePath '/test/parquet/', compression 'none' , enableCache 'false', format 'parquet');

```
the parameters are the same as the oss storage.



### support hive-connector
Run a hive-connector sync SQL. An fdw foregin table will be created. Here only the description of hive-connector fdw foregin table parameters.

1. Run hive-connector sync SQL
  hive-connector will visite remote hive metadata. Get metadata and create fdw foregin table.
2. Sync SQL will create fdw foregin table
example:
```
CREATE FOREIGN TABLE example (
        id int,
        col text,
        col2 text,
        col3 text
)
        SERVER foreign_server
        OPTIONS (filePath '/opt/hadoop/apache-hive-3.1.0-bin/user/hive/warehouse/hive_test16', hive_cluster_name 'hive_cluster', datasource 'default.hive_test16', transactional 'false', partitionkeys 'id', hdfs_cluster_name 'paa_cluster', format 'orc');
```

`filepath`  
Specific file path of the hive table saved to hdfs

`hive_cluster_name`  
Specifies the cluster name of the gphive.conf. fdw table will used hive_cluster_name to visite hdfs.


`datasource`  
Sync hive table name

`transactional`  
whether hive table is transaction table  

`partitionkeys`  
hive table partition keys

`hdfs_cluster_name`  
Specifies the cluster name of the gphdfs.conf. fdw table will used hdfs_cluster_name to visite hdfs.


Reference document for more hive-connector instructions
https://code.hashdata.xyz/documents/dev-internals/-/blob/master/design_doc/storage/Hashdata2x3x-gphdfs%E4%BD%BF%E7%94%A8%E6%89%8B%E5%86%8C.md






