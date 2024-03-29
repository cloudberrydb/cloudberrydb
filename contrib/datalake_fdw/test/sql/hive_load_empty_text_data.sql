CREATE DATABASE IF NOT EXISTS mytestdb;
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

CREATE TABLE empty_avro
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
) STORED AS AVRO;

CREATE TABLE empty_avro_partition
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
STORED AS AVRO;

USE mytestdb;

CREATE TABLE text_default
(
    a string,
    b string,
    c string,
    d string
)
STORED AS TEXTFILE;
INSERT INTO text_default VALUES
('1', '1', '1', '1'),
('2', '2', '2', '2'),
('3', '3', '3', '3'),
('4', '4', '4', '4'),
('5', '5', '5', '5');


CREATE TABLE text_custom
(
    a string,
    b string,
    c string,
    d string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
NULL DEFINED AS 'null'
STORED AS TEXTFILE;
INSERT INTO text_custom VALUES
('1', '1', '1', null),
('2', '2', null, '2'),
('3', '3', '3', '3'),
('4', null, '4', '4'),
('5', '5', '5', '5');


CREATE TABLE csv_default
(
    a string,
    b string,
    c string,
    d string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;
INSERT INTO csv_default VALUES
('1', '1', '1', '1'),
('2', '2', '2', null),
('3', '3', '3', '3'),
('4', null, '4', '4'),
('5', '5', '5', '5');


CREATE TABLE csv_custom
(
    a string,
    b string,
    c string,
    d string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "quoteChar"     = "\'",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE;
INSERT INTO csv_custom VALUES
('1', '1', '1', null),
('2', '2', '2', '2'),
('3', null, '3', '3'),
('4', '4', '4', '4'),
('5', '5', '5', '5');
