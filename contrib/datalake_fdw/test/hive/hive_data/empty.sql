USE mytestdb;
SET hive.support.concurrency = true;
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.exec.max.dynamic.partitions.pernode = 1000;

CREATE TABLE empty_orc_transactional
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
) CLUSTERED BY (a) INTO 5 BUCKETS
STORED AS ORC
TBLPROPERTIES ("transactional"="true");

CREATE TABLE empty_orc
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
) STORED AS ORC;

CREATE TABLE empty_orc_partition
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
    m int,
    n varchar(20),
    o date
)
STORED AS ORC;

CREATE TABLE empty_parquet
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
) STORED AS PARQUET;

CREATE TABLE empty_parquet_partition
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
    m int,
    n varchar(20),
    o date
)
STORED AS PARQUET;
