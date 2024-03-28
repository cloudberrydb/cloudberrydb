CREATE DATABASE IF NOT EXISTS hive_parquet_load_data_test;

USE hive_parquet_load_data_test;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.support.concurrency = true;
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.stats.autogather=false;
SET hive.exec.mode.local.auto=true;


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
STORED AS PARQUET;
INSERT INTO TABLE hive_type_test_1 values 
(1, "aaaaabbbb", 1),
(2, "aaaaabbbb", 1),
(3, "aaaaabbbb", 1),
(4, "aaaaabbbb", 1),
(5, "aaaaabbbb", 1),
(NULL, "aaaaabbbb", 2),
(7, "aaaaabbbb", 2),
(8, "aaaaabbbb", 2),
(9, "aaaaabbbb", 2),
(10, "aaaaabbbb", 2),
(6, "aaaaabbbb", 3),
(7, NULL, 3),
(8, NULL, 3),
(9, NULL, 3),
(NULL, NULL, 3),
(6, "aaaaabbbb", 3),
(7, "aaaaabbbb", 3),
(8, "aaaaabbbb", 3),
(9, "aaaaabbbb", 3),
(10, "aaaaabbbb", 3);





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
STORED AS PARQUET;


INSERT INTO TABLE hive_type_test_2 values 
(1, "aaaaabbbb", 1),
(2, "aaaaabbbb", 1),
(3, "aaaaabbbb", 1),
(4, "aaaaabbbb", 1),
(5, "aaaaabbbb", 1),
(6, "aaaaabbbb", 2),
(NULL, "aaaaabbbb", 2),
(8, "aaaaabbbb", 2),
(9, "aaaaabbbb", 2),
(10, "aaaaabbbb", 2),
(6, "aaaaabbbb", 3),
(7, "aaaaabbbb", 3),
(8, "aaaaabbbb", 3),
(9, NULL, 3),
(10, "aaaaabbbb", 3),
(6, "aaaaabbbb", 3),
(7, "aaaaabbbb", 3),
(NULL, NULL, 3),
(9, "aaaaabbbb", 3),
(10, "aaaaabbbb", 3);


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
STORED AS PARQUET;

INSERT INTO TABLE hive_type_test_3 values
(1, "aaaaabbbb", 1),
(2, "aaaaabbbb", 1),
(3, "aaaaabbbb", 1),
(4, "aaaaabbbb", 1),
(5, "aaaaabbbb", 1),
(6, "aaaaabbbb", 2),
(NULL, "aaaaabbbb", 2),
(8, "aaaaabbbb", 2),
(9, "aaaaabbbb", 2),
(10, "aaaaabbbb", 2),
(6, "aaaaabbbb", 3),
(7, "aaaaabbbb", 3),
(8, "aaaaabbbb", 3),
(9, NULL, 3),
(10, "aaaaabbbb", 3),
(6, "aaaaabbbb", 3),
(7, "aaaaabbbb", 3),
(NULL, NULL, 3),
(9, "aaaaabbbb", 3),
(10, "aaaaabbbb", 3);

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
STORED AS PARQUET;

INSERT INTO TABLE hive_type_test_4 values 
(1, "aaaaabbbb", 1),
(2, "aaaaabbbb", 1),
(3, "aaaaabbbb", 1),
(4, "aaaaabbbb", 1),
(5, "aaaaabbbb", 1),
(6, "aaaaabbbb", 2),
(NULL, "aaaaabbbb", 2),
(8, "aaaaabbbb", 2),
(9, "aaaaabbbb", 2),
(10, "aaaaabbbb", 2),
(6, "aaaaabbbb", 3),
(7, "aaaaabbbb", 3),
(8, "aaaaabbbb", 3),
(9, NULL, 3),
(10, "aaaaabbbb", 3),
(6, "aaaaabbbb", 3),
(7, "aaaaabbbb", 3),
(NULL, NULL, 3),
(9, "aaaaabbbb", 3),
(10, "aaaaabbbb", 3);


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
STORED AS PARQUET;

INSERT INTO TABLE hive_type_test_5 values
(1, "aaaaabbbb", 1.1),
(2, "aaaaabbbb", 1.1),
(3, "aaaaabbbb", 1.1),
(4, "aaaaabbbb", 1.1),
(5, "aaaaabbbb", 1.1),
(6, "aaaaabbbb", 2.1),
(NULL, "aaaaabbbb", 2.1),
(8, "aaaaabbbb", 2.1),
(9, "aaaaabbbb", 2.1),
(10, "aaaaabbbb", 2.1),
(6, "aaaaabbbb", 3.1),
(7, "aaaaabbbb", 3.1),
(8, "aaaaabbbb", 3.1),
(9, NULL, 3.1),
(10, "aaaaabbbb", 3.1),
(6, "aaaaabbbb", 3.1),
(7, "aaaaabbbb", 3.1),
(NULL, NULL, 3.1),
(9, "aaaaabbbb", 3.1),
(10, "aaaaabbbb", 3.1);

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
STORED AS PARQUET;
INSERT INTO TABLE hive_type_test_6 
values
(1, "aaaaabbbb", 1.1),
(2, "aaaaabbbb", 1.1),
(3, "aaaaabbbb", 1.1),
(4, "aaaaabbbb", 1.1),
(5, "aaaaabbbb", 1.1),
(6, "aaaaabbbb", 2.1),
(NULL, "aaaaabbbb", 2.1),
(8, "aaaaabbbb", 2.1),
(9, "aaaaabbbb", 2.1),
(10, "aaaaabbbb", 2.1),
(6, "aaaaabbbb", 3.1),
(7, "aaaaabbbb", 3.1),
(8, "aaaaabbbb", 3.1),
(9, NULL, 3.1),
(10, "aaaaabbbb", 3.1),
(6, "aaaaabbbb", 3.1),
(7, "aaaaabbbb", 3.1),
(NULL, NULL, 3.1),
(9, "aaaaabbbb", 3.1),
(10, "aaaaabbbb", 3.1);


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
STORED AS PARQUET;
INSERT INTO TABLE hive_type_test_7 
values
(1, "aaaaabbbb", "1.1"),
(2, "aaaaabbbb", "1.1"),
(3, "aaaaabbbb", "1.1"),
(4, "aaaaabbbb", "1.1"),
(5, "aaaaabbbb", "1.1"),
(6, "aaaaabbbb", "2.1"),
(NULL, "aaaaabbbb", "2.1"),
(8, "aaaaabbbb", "2.1"),
(9, "aaaaabbbb", "2.1"),
(10, "aaaaabbbb", "2.1"),
(6, "aaaaabbbb", "3.1"),
(7, "aaaaabbbb", "3.1"),
(8, "aaaaabbbb", "3.1"),
(9, NULL, "3.1"),
(10, "aaaaabbbb", "3.1"),
(6, "aaaaabbbb", "3.1"),
(7, "aaaaabbbb", "3.1"),
(NULL, NULL, "3.1"),
(9, "aaaaabbbb", "3.1"),
(10, "aaaaabbbb", "3.1");



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
STORED AS PARQUET;

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
STORED AS PARQUET;

INSERT INTO TABLE hive_type_test_9 
values
(1, "aaaaabbbb", "aa"),
(2, "aaaaabbbb", "aa"),
(3, "aaaaabbbb", "aa"),
(4, "aaaaabbbb", "aa"),
(1, "aaaaabbbb", "aabb"),
(2, "aaaaabbbb", "aabb"),
(3, "aaaaabbbb", "aabb"),
(4, "aaaaabbbb", "aabb"),
(1, "aaaaabbbb", "aabbcc"),
(2, "aaaaabbbb", "aabbcc"),
(3, "aaaaabbbb", "aabbcc"),
(4, "aaaaabbbb", "aabbcc");

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
STORED AS PARQUET;

INSERT INTO TABLE hive_type_test_10 
values
(1, "aaaaabbbb", 12345.1),
(2, "aaaaabbbb", 12345.1),
(3, "aaaaabbbb", 1234567890.1),
(4, "aaaaabbbb", 1234567890.1),
(1, "aaaaabbbb", 1.000000001),
(2, "aaaaabbbb", 1.000000001),
(3, "aaaaabbbb", 1.999999991),
(4, "aaaaabbbb", 12345.12345),
(1, "aaaaabbbb", 12345.12345),
(2, "aaaaabbbb", 123456789.123456789),
(3, "aaaaabbbb", 123456780.10000),
(4, "aaaaabbbb", 123456780.10000),
(1, "aaaaabbbb", 12345.1),
(2, "aaaaabbbb", 12345.1),
(3, "aaaaabbbb", 1234567890.1),
(4, "aaaaabbbb", 1234567890.1),
(1, "aaaaabbbb", 1.000000001),
(2, "aaaaabbbb", 1.000000001),
(3, "aaaaabbbb", 1.999999991),
(4, "aaaaabbbb", 12345.12345),
(1, "aaaaabbbb", 12345.12345),
(2, "aaaaabbbb", 123456789.123456789),
(3, "aaaaabbbb", 123456780.10000),
(4, "aaaaabbbb", 123456780.10000);


-- hive simple table
drop table if exists hive_test_1;
CREATE TABLE hive_test_1 (
  id  int,
  name               string,
  name2              string
) CLUSTERED BY (id) INTO 5 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

INSERT INTO TABLE hive_test_1 
VALUES
(1, "a", "a"),
(2, NULL, "a"),
(3, "a", "a"),
(4, "a", NULL),
(5, "a", "a"),
(NULL, NULL, NULL),
(7, NULL, NULL),
(8, NULL, NULL),
(9, NULL, NULL),
(10, NULL, NULL);

-- hive partition and transaction table
drop table if exists hive_test_2;
CREATE TABLE hive_test_2 (
  id  int,
  name               string,
  name2              string
)
partitioned by (name3 string) CLUSTERED BY (id) INTO 5 BUCKETS STORED AS PARQUET
TBLPROPERTIES ("transactional"="false",
  "compactor.mapreduce.map.memory.mb"="2048",
  "compactorthreshold.hive.compactor.delta.num.threshold"="4",  
  "compactorthreshold.hive.compactor.delta.pct.threshold"="0.5" 
);

alter table hive_test_2 add partition (name3="a");
alter table hive_test_2 add partition (name3="b");
alter table hive_test_2 add partition (name3="c");
alter table hive_test_2 add partition (name3="d");

INSERT INTO TABLE hive_test_2 
PARTITION(name3='a') 
VALUES
(1, "a", "a"),
(2, NULL, "a"),
(3, "a", "a"),
(4, "a", NULL),
(5, "a", "a"),
(6, NULL, "a"),
(7, "a", "a"),
(8, "a", "a"),
(9, "a", "a"),
(10, "a", "a");

INSERT INTO TABLE hive_test_2 
PARTITION(name3='b') 
VALUES
(1, "a", "a"),
(2, NULL, "a"),
(3, "a", "a"),
(4, "a", "a"),
(5, "a", NULL),
(6, "a", "a"),
(7, NULL, "a"),
(8, "a", "a"),
(9, "a", "a"),
(10, "a", "a");

INSERT INTO TABLE hive_test_2 
PARTITION(name3='c') 
VALUES
(1, "a", "a"),
(2, "a", "a"),
(3, NULL, "a"),
(4, "a", "a"),
(5, "a", NULL),
(6, "a", "a"),
(7, "a", "a"),
(8, NULL, "a"),
(9, "a", "a"),
(10, "a", "a");

INSERT INTO TABLE hive_test_2 
PARTITION(name3='d') 
VALUES
(1, "a", "a"),
(2, NULL, "a"),
(3, "a", "a"),
(4, "a", NULL),
(5, "a", "a"),
(6, "a", "a"),
(7, NULL, "a"),
(8, "a", "a"),
(9, "a", "a"),
(10, "a", NULL);

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
STORED AS PARQUET;

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
STORED AS PARQUET;

INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="aa") VALUES
  (10, "aaaaabbbb"),
  (9, "bbbbcccc"),
  (8, "ddddbbbb"),
  (10, "aaaaabbbb"),
  (9, "bbbbcccc"),
  (8, "ddddbbbb"),
  (10, "aaaaabbbb"),
  (9, "bbbbcccc"),
  (8, "ddddbbbb");

INSERT INTO TABLE hive_test_4 PARTITION(m=1, n="bb") VALUES
  (7, "fdsfsf"),
  (6, "rewrwr3r2"),
  (7, "fdsfsf"),
  (6, "rewrwr3r2"),
  (7, "fdsfsf"),
  (6, "rewrwr3r2");

INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="cc") VALUES
  (1, "fsdf"),
  (2, "rewrwe"),
  (1, "fsdf"),
  (2, "rewrwe"),
  (1, "fsdf"),
  (2, "rewrwe");

INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="dd") VALUES
  (3, "2345fsd"),
  (3, "2345fsd"),
  (3, "2345fsd");

INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="ee") VALUES
  (4, "trewtaf"),
  (4, "trewtaf"),
  (4, "trewtaf");

INSERT INTO TABLE hive_test_4 PARTITION(m=2, n="ff") VALUES
  (5, "fdsfsdfwe"),
  (5, "fdsfsdfwe"),
  (5, "fdsfsdfwe");

INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="aa") VALUES
  (1, "23423"),
  (1, "23423"),
  (1, "23423");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="cc") VALUES
  (2, "werwer"),
  (3, "2345gagefsd"),
  (2, "werwer"),
  (3, "2345gagefsd"),
  (2, "werwer"),
  (3, "2345gagefsd");
INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="dd") VALUES 
(4, "46346"),
(4, "46346"),
(4, "46346");

INSERT INTO TABLE hive_test_4 PARTITION(m=3, n="ee") VALUES 
(5, "agasdg"),
(5, "agasdg"),
(5, "agasdg");





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
STORED AS PARQUET;
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
STORED AS PARQUET;
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
STORED AS PARQUET;
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
STORED AS PARQUET;
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

