CREATE DATABASE IF NOT EXISTS hive_avro_load_data_test;

USE hive_avro_load_data_test;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.support.concurrency = true;
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;



-- hive partition tinyint
drop table if exists hive_type_test_1;
CREATE TABLE hive_type_test_1
(
    id  int,
    name string
)
PARTITIONED BY
(
    m tinyint
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_1 PARTITION(m=1) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=1) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=1) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=1) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=1) values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=2) values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=2) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=2) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=2) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=2) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(7, NULL);
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(8, NULL);
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(9, NULL);
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(NULL, NULL);
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_1 PARTITION(m=3) values(10, "aaaaabbbb");

-- hive partition smallint
drop table if exists hive_type_test_2;
CREATE TABLE hive_type_test_2
(
    id  int,
    name string
)
PARTITIONED BY
(
    m smallint
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_2 PARTITION(m=1) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=1) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=1) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=1) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=1) values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=2) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=2) values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=2) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=2) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=2) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(9, NULL);
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(NULL, NULL);
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_2 PARTITION(m=3) values(10, "aaaaabbbb");


-- hive partition int
drop table if exists hive_type_test_3;
CREATE TABLE hive_type_test_3
(
    id  int,
    name string
)
PARTITIONED BY
(
    m int
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_3 PARTITION(m=1) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=1) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=1) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=1) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=1) values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=2) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=2) values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=2) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=2) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=2) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(9, NULL);
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(NULL, NULL);
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_3 PARTITION(m=3) values(10, "aaaaabbbb");

-- hive partition bigint
drop table if exists hive_type_test_4;
CREATE TABLE hive_type_test_4
(
    id  int,
    name string
)
PARTITIONED BY
(
    m bigint
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_4 PARTITION(m=1) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=1) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=1) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=1) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=1) values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=2) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=2) values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=2) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=2) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=2) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(9, NULL);
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(NULL, NULL);
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_4 PARTITION(m=3) values(10, "aaaaabbbb");

-- hive partition float
drop table if exists hive_type_test_5;
CREATE TABLE hive_type_test_5
(
    id  int,
    name string
)
PARTITIONED BY
(
    m float
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_5 PARTITION(m=1.1) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=1.1) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=1.1) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=1.1) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=1.1) values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=2.1) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=2.1) values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=2.1) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=2.1) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=2.1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(9, NULL);
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(NULL, NULL);
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_5 PARTITION(m=3.1) values(10, "aaaaabbbb");


-- hive partition double
drop table if exists hive_type_test_6;
CREATE TABLE hive_type_test_6
(
    id  int,
    name string
)
PARTITIONED BY
(
    m double
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_6 PARTITION(m=1.1) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=1.1) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=1.1) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=1.1) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=1.1) values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=2.1) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=2.1) values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=2.1) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=2.1) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=2.1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(9, NULL);
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(NULL, NULL);
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_6 PARTITION(m=3.1) values(10, "aaaaabbbb");


-- hive partition string
drop table if exists hive_type_test_7;
CREATE TABLE hive_type_test_7
(
    id  int,
    name string
)
PARTITIONED BY
(
    m string
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_7 PARTITION(m="1.1") values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="1.1") values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="1.1") values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="1.1") values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="1.1") values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="2.1") values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="2.1") values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="2.1") values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="2.1") values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="2.1") values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(9, NULL);
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(NULL, NULL);
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_7 PARTITION(m="3.1") values(10, "aaaaabbbb");



-- hive partition date
drop table if exists hive_type_test_8;
CREATE TABLE hive_type_test_8
(
    id  int,
    name string
)
PARTITIONED BY
(
    m date
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_8 PARTITION(m='0009-01-01') values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='0010-01-01') values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1011-01-01') values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='0001-12-30') values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='0001-12-01') values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1969-12-01') values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1969-12-01') values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1969-12-30') values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1969-12-30') values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1970-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1970-01-01') values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='100-01-01') values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='100-01-01') values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1560-01-01') values(9, NULL);
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1560-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='99-01-01') values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='99-01-01') values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1968-01-01') values(NULL, NULL);
INSERT INTO TABLE hive_type_test_8 PARTITION(m='2999-12-30') values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='2999-12-31') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='0009-01-01') values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='0010-01-01') values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1011-01-01') values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='0001-12-30') values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='0001-12-01') values(5, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1969-12-01') values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1969-12-01') values(NULL, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1969-12-30') values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1969-12-30') values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1970-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1970-01-01') values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='100-01-01') values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='100-01-01') values(8, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1560-01-01') values(9, NULL);
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1560-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='99-01-01') values(6, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='99-01-01') values(7, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='1968-01-01') values(NULL, NULL);
INSERT INTO TABLE hive_type_test_8 PARTITION(m='2999-12-30') values(9, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_8 PARTITION(m='2999-12-31') values(10, "aaaaabbbb");

-- hive partition varchar
drop table if exists hive_type_test_9;
CREATE TABLE hive_type_test_9
(
    id  int,
    name string
)
PARTITIONED BY
(
    m varchar(20)
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aa") values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aa") values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aa") values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aa") values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aabb") values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aabb") values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aabb") values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aabb") values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aabbcc") values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aabbcc") values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aabbcc") values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_9 PARTITION(m="aabbcc") values(4, "aaaaabbbb");

-- hive partition decimal
drop table if exists hive_type_test_10;
CREATE TABLE hive_type_test_10
(
    id  int,
    name string
)
PARTITIONED BY
(
    m decimal(20, 10)
)
STORED AS AVRO;
INSERT INTO TABLE hive_type_test_10 PARTITION(m=12345.1) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=12345.1) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1234567890.1) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1234567890.1) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1.000000001) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1.000000001) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1.999999991) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=12345.12345) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=12345.12345) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=123456789.123456789) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=123456780.10000) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=123456780.10000) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=12345.1) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=12345.1) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1234567890.1) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1234567890.1) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1.000000001) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1.000000001) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=1.999999991) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=12345.12345) values(4, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=12345.12345) values(1, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=123456789.123456789) values(2, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=123456780.10000) values(3, "aaaaabbbb");
INSERT INTO TABLE hive_type_test_10 PARTITION(m=123456780.10000) values(4, "aaaaabbbb");

-- hive simple table
drop table if exists hive_test_1;
CREATE TABLE hive_test_1 (
  id  int,
  name               string,
  name2              string
) CLUSTERED BY (id) INTO 5 BUCKETS
STORED AS AVRO
TBLPROPERTIES ("transactional"="false");
INSERT INTO TABLE hive_test_1 values(1, "a", "a");
INSERT INTO TABLE hive_test_1 values(2, NULL, "a");
INSERT INTO TABLE hive_test_1 values(3, "a", "a");
INSERT INTO TABLE hive_test_1 values(4, "a", NULL);
INSERT INTO TABLE hive_test_1 values(5, "a", "a");
INSERT INTO TABLE hive_test_1 values(NULL, NULL, NULL);
INSERT INTO TABLE hive_test_1 values(7, NULL, NULL);
INSERT INTO TABLE hive_test_1 values(8, NULL, NULL);
INSERT INTO TABLE hive_test_1 values(9, NULL, NULL);
INSERT INTO TABLE hive_test_1 values(10, NULL, NULL);


-- hive partition and transaction table
drop table if exists hive_test_2;
CREATE TABLE hive_test_2 (
  id  int,
  name               string,
  name2              string
)
partitioned by (name3 string) CLUSTERED BY (id) INTO 5 BUCKETS STORED AS AVRO
TBLPROPERTIES ("transactional"="false",
  "compactor.mapreduce.map.memory.mb"="2048",
  "compactorthreshold.hive.compactor.delta.num.threshold"="4",  
  "compactorthreshold.hive.compactor.delta.pct.threshold"="0.5" 
);

alter table hive_test_2 add partition (name3="a");
alter table hive_test_2 add partition (name3="b");
alter table hive_test_2 add partition (name3="c");
alter table hive_test_2 add partition (name3="d");

INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(1, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(2, NULL, "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(3, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(4, "a", NULL);
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(5, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(6, NULL, "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(7, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(8, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(9, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='a') values(10, "a", "a");

INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(1, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(2, NULL, "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(3, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(4, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(5, "a", NULL);
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(6, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(7, NULL, "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(8, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(9, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='b') values(10, "a", "a");

INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(1, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(2, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(3, NULL, "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(4, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(5, "a", NULL);
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(6, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(7, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(8, NULL, "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(9, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='c') values(10, "a", "a");

INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(1, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(2, NULL, "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(3, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(4, "a", NULL);
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(5, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(6, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(7, NULL, "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(8, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(9, "a", "a");
INSERT INTO TABLE hive_test_2 PARTITION(name3='d') values(10, "a", NULL);


-- hive partiton key is int type
drop table if exists hive_test_3;
CREATE TABLE hive_test_3
(
    id  int,
    name string
)
PARTITIONED BY
(
    m int
)
STORED AS AVRO;
INSERT INTO TABLE hive_test_3 PARTITION(m=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_3 PARTITION(m=1) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_3 PARTITION(m=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_3 PARTITION(m=1) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_3 PARTITION(m=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_3 PARTITION(m=2) values(1, "fsdf");
INSERT INTO TABLE hive_test_3 PARTITION(m=2) values(2, "rewrwe");
INSERT INTO TABLE hive_test_3 PARTITION(m=2) values(3, "2345fsd");
INSERT INTO TABLE hive_test_3 PARTITION(m=2) values(4, "trewtaf");
INSERT INTO TABLE hive_test_3 PARTITION(m=2) values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_3 PARTITION(m=3) values(1, "23423");
INSERT INTO TABLE hive_test_3 PARTITION(m=3) values(2, "werwer");
INSERT INTO TABLE hive_test_3 PARTITION(m=3) values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_3 PARTITION(m=3) values(4, "46346");
INSERT INTO TABLE hive_test_3 PARTITION(m=3) values(5, "agasdg");

-- hive partiton key is int and string type
drop table if exists hive_test_4;
CREATE TABLE hive_test_4
(
    id  int,
    name string
)
PARTITIONED BY
(
    m int, n string
)
STORED AS AVRO;
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="bb") values(7, "fdsfsf");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="bb") values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="cc") values(1, "fsdf");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="cc") values(2, "rewrwe");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="dd") values(3, "2345fsd");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="ee") values(4, "trewtaf");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="ff") values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="aa") values(1, "23423");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="cc") values(2, "werwer");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="cc") values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="dd") values(4, "46346");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="ee") values(5, "agasdg");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="bb") values(7, "fdsfsf");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="bb") values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="cc") values(1, "fsdf");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="cc") values(2, "rewrwe");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="dd") values(3, "2345fsd");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="ee") values(4, "trewtaf");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="ff") values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="aa") values(1, "23423");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="cc") values(2, "werwer");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="cc") values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="dd") values(4, "46346");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="ee") values(5, "agasdg");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="bb") values(7, "fdsfsf");
INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="bb") values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="cc") values(1, "fsdf");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="cc") values(2, "rewrwe");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="dd") values(3, "2345fsd");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="ee") values(4, "trewtaf");
INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="ff") values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="aa") values(1, "23423");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="cc") values(2, "werwer");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="cc") values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="dd") values(4, "46346");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="ee") values(5, "agasdg");

-- hive partiton key is int and string and date type
drop table if exists hive_test_5;
CREATE TABLE hive_test_5
(
    id  int,
    name string
)
PARTITIONED BY
(
    m int, n string, o date
)
STORED AS AVRO;
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='1900-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='099-01-01') values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='0001-01-01') values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1970-01-01') values(7, "fdsfsf");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1969-12-30') values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='1900-01-01') values(1, "fsdf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='9999-09-09') values(2, "rewrwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="dd", o='0002-03-30') values(3, "2345fsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ee", o='2999-12-30') values(4, "trewtaf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ff", o='2000-01-01') values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="aa", o='1969-11-31') values(1, "23423");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1969-12-31') values(2, "werwer");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1970-01-01') values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="dd", o='1999-09-01') values(4, "46346");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="ee", o='1000-01-01') values(5, "agasdg");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='1900-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='099-01-01') values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='0001-01-01') values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1970-01-01') values(7, "fdsfsf");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1969-12-30') values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='1900-01-01') values(1, "fsdf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='9999-09-09') values(2, "rewrwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="dd", o='0002-03-30') values(3, "2345fsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ee", o='2999-12-30') values(4, "trewtaf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ff", o='2000-01-01') values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="aa", o='1969-11-31') values(1, "23423");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1969-12-31') values(2, "werwer");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1970-01-01') values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="dd", o='1999-09-01') values(4, "46346");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="ee", o='1000-01-01') values(5, "agasdg");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='1900-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='099-01-01') values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='0001-01-01') values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1970-01-01') values(7, "fdsfsf");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1969-12-30') values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='1900-01-01') values(1, "fsdf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='9999-09-09') values(2, "rewrwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="dd", o='0002-03-30') values(3, "2345fsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ee", o='2999-12-30') values(4, "trewtaf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ff", o='2000-01-01') values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="aa", o='1969-11-31') values(1, "23423");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1969-12-31') values(2, "werwer");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1970-01-01') values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="dd", o='1999-09-01') values(4, "46346");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="ee", o='1000-01-01') values(5, "agasdg");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='1900-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='099-01-01') values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='0001-01-01') values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1970-01-01') values(7, "fdsfsf");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1969-12-30') values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='1900-01-01') values(1, "fsdf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='9999-09-09') values(2, "rewrwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="dd", o='0002-03-30') values(3, "2345fsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ee", o='2999-12-30') values(4, "trewtaf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ff", o='2000-01-01') values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="aa", o='1969-11-31') values(1, "23423");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1969-12-31') values(2, "werwer");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1970-01-01') values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="dd", o='1999-09-01') values(4, "46346");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="ee", o='1000-01-01') values(5, "agasdg");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='1900-01-01') values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='099-01-01') values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="aa", o='0001-01-01') values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1970-01-01') values(7, "fdsfsf");
INSERT INTO TABLE hive_test_5 PARTITION(m=1, n="bb", o='1969-12-30') values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='1900-01-01') values(1, "fsdf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="cc", o='9999-09-09') values(2, "rewrwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="dd", o='0002-03-30') values(3, "2345fsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ee", o='2999-12-30') values(4, "trewtaf");
INSERT INTO TABLE hive_test_5 PARTITION(m=2, n="ff", o='2000-01-01') values(5, "fdsfsdfwe");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="aa", o='1969-11-31') values(1, "23423");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1969-12-31') values(2, "werwer");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="cc", o='1970-01-01') values(3, "2345gagefsd");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="dd", o='1999-09-01') values(4, "46346");
INSERT INTO TABLE hive_test_5 PARTITION(m=3, n="ee", o='1000-01-01') values(5, "agasdg");



-- hive partiton key is int and string and float type
drop table if exists hive_test_6;
CREATE TABLE hive_test_6
(
    id  int,
    name string
)
PARTITIONED BY
(
    m int, n string, o float
)
STORED AS AVRO;
INSERT INTO TABLE hive_test_6 PARTITION(m=1, n="aa", o=1.11) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_6 PARTITION(m=1, n="aa", o=200.11) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_6 PARTITION(m=1, n="aa", o=1.0001) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_6 PARTITION(m=1, n="bb", o=2.9999) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_6 PARTITION(m=1, n="bb", o=9999.9999) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_6 PARTITION(m=2, n="aa", o=1.11) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_6 PARTITION(m=2, n="aa", o=200.11) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_6 PARTITION(m=2, n="aa", o=1.0001) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_6 PARTITION(m=2, n="bb", o=2.9999) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_6 PARTITION(m=2, n="bb", o=9999.9999) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_6 PARTITION(m=3, n="aa", o=1.11) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_6 PARTITION(m=3, n="aa", o=200.11) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_6 PARTITION(m=3, n="aa", o=1.0001) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_6 PARTITION(m=3, n="bb", o=2.9999) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_6 PARTITION(m=3, n="bb", o=9999.9999) values(6, "rewrwr3r2");

-- hive partiton key is int and string and decimal and tinyint type
drop table if exists hive_test_7;
CREATE TABLE hive_test_7
(
    id  int,
    name string
)
PARTITIONED BY
(
    m int, n string, o decimal(20, 10), p tinyint
)
STORED AS AVRO;
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=1.0000000001, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=200.01, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=1.1, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=2.99, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=9999.999, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=1.1000000000, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=200.999999, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=1.99999, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="aa", o=1.0000000001, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="aa", o=200.01, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="aa", o=1.1, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="bb", o=2.99, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="bb", o=9999.999, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="aa", o=1.1000000000, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="aa", o=200.999999, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="aa", o=1.99999, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="aa", o=1.0000000001, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="aa", o=200.01, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="aa", o=1.1, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="bb", o=2.99, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="bb", o=9999.999, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="aa", o=1.1000000000, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="aa", o=200.999999, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="aa", o=1.99999, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="bb", o=1234567890.01, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="aa", o=1.0000000001, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="aa", o=200.01, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="aa", o=1.1, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="bb", o=2.99, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="bb", o=9999.999, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="aa", o=1.1000000000, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="aa", o=200.999999, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="aa", o=1.99999, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="bb", o=1234567890.01, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=1.0000000001, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=200.01, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=1.1, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=2.99, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=9999.999, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=1.1000000000, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=200.999999, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="aa", o=1.99999, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=1, n="bb", o=1234567890.01, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="aa", o=1.0000000001, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="aa", o=200.01, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="aa", o=1.1, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="bb", o=2.99, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=2, n="bb", o=9999.999, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="aa", o=1.1000000000, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="aa", o=200.999999, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="aa", o=1.99999, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=3, n="bb", o=1234567890.01, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="aa", o=1.0000000001, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="aa", o=200.01, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="aa", o=1.1, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="bb", o=2.99, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=4, n="bb", o=9999.999, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="aa", o=1.1000000000, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="aa", o=200.999999, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="aa", o=1.99999, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=5, n="bb", o=1234567890.01, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="aa", o=1.0000000001, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="aa", o=200.01, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="aa", o=1.1, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="bb", o=2.99, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=6, n="bb", o=9999.999, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="aa", o=1.1000000000, p=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="aa", o=200.999999, p=0) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="aa", o=1.99999, p=1) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="bb", o=1234567890.1234567891, p=1) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="bb", o=999999999.9999999, p=0) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_7 PARTITION(m=7, n="bb", o=1234567890.01, p=1) values(6, "rewrwr3r2");


-- hive partiton key is int and string and decimal and tinyint type
drop table if exists hive_test_8;
CREATE TABLE hive_test_8
(
    id  int,
    name string
)
PARTITIONED BY
(
    m int, n string, o decimal(20, 10), p tinyint, q smallint, s bigint
)
STORED AS AVRO;
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.1000000000, p=1, q=1, s=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=200.999999, p=0, q=2, s=3) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.99999, p=1, q=4, s=5) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=22, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.1234567891, p=1, q=2, s=3) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=20, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.01, p=1, q=2, s=13) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.1000000000, p=1, q=1, s=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=200.999999, p=0, q=2, s=3) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.99999, p=1, q=4, s=5) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=22, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.1234567891, p=1, q=2, s=3) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=20, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.01, p=1, q=2, s=13) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.1000000000, p=1, q=1, s=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=200.999999, p=0, q=2, s=3) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.99999, p=1, q=4, s=5) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=22, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.1234567891, p=1, q=2, s=3) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=20, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.01, p=1, q=2, s=13) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.1000000000, p=1, q=1, s=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=200.999999, p=0, q=2, s=3) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.99999, p=1, q=4, s=5) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=22, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.1234567891, p=1, q=2, s=3) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=20, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.01, p=1, q=2, s=13) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.1000000000, p=1, q=1, s=1) values(10, "aaaaabbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=200.999999, p=0, q=2, s=3) values(9, "bbbbcccc");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="aa", o=1.99999, p=1, q=4, s=5) values(8, "ddddbbbb");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=22, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.1234567891, p=1, q=2, s=3) values(6, "rewrwr3r2");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=999999999.9999999, p=0, q=20, s=3) values(7, "fdsfsf");
INSERT INTO TABLE hive_test_8 PARTITION(m=7, n="bb", o=1234567890.01, p=1, q=2, s=13) values(6, "rewrwr3r2");

