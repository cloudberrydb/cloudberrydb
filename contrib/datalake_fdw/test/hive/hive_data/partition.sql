USE mytestdb;
SET hive.support.concurrency = true;
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
-- SET hive.stats.autogather=false;
-- SET hive.exec.mode.local.auto=true;

CREATE TABLE transactional_orc
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
INSERT INTO transactional_orc VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01);

CREATE TABLE normal_orc
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
INSERT INTO normal_orc VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01);


CREATE TABLE partition_orc_1
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
INSERT INTO partition_orc_1 VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5);


CREATE TABLE partition_orc_2
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
    n varchar(20)
)
STORED AS ORC;
INSERT INTO partition_orc_2 VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1, '1'),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2, '2'),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3, '3'),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4, '4'),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5, '5');


CREATE TABLE partition_orc_3
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
INSERT INTO partition_orc_3 VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1, '1', '2020-01-01'),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2, '2', '2020-02-01'),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3, '3', '2020-03-01'),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4, '4', '2020-04-01'),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5, '5', '2020-05-01');

CREATE TABLE partition_orc_4
(
    a int,
    b decimal(10,5)
)
PARTITIONED BY
(
    c string,
    d string,
    e string,
    f string
)
STORED AS ORC;
INSERT INTO partition_orc_4 SELECT * FROM test_table1;

CREATE TABLE normal_parquet
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
INSERT INTO normal_parquet VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01);


CREATE TABLE partition_parquet_1
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
STORED AS PARQUET;
INSERT INTO partition_parquet_1 VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5);


CREATE TABLE partition_parquet_2
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
    n varchar(20)
)
STORED AS PARQUET;
INSERT INTO partition_parquet_2 VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1, '1'),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2, '2'),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3, '3'),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4, '4'),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5, '5');


CREATE TABLE partition_parquet_3
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
INSERT INTO partition_parquet_3 VALUES
(1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1, '1', '2020-01-01'),
(2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2, '2', '2020-02-01'),
(3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3, '3', '2020-03-01'),
(4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4, '4', '2020-04-01'),
(5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5, '5', '2020-05-01');
