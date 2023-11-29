#!/bin/sh

beeline -u "jdbc:hive2://localhost:10000/newtestdb;principal=hive/hashdata@HADOOP.COM" -e "drop database if exists mytestdb cascade"
beeline -u "jdbc:hive2://localhost:10000/newtestdb;principal=hive/hashdata@HADOOP.COM" -e "create database mytestdb"

for i in {1..10000}
do
        beeline -u "jdbc:hive2://localhost:10000/mytestdb;principal=hive/hashdata@HADOOP.COM" -e "CREATE TABLE test_all_type_${i}
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
) STORED AS ORC"
done

for i in {1..5000}
do
        beeline -u "jdbc:hive2://localhost:10000/mytestdb;principal=hive/hashdata@HADOOP.COM" -e "CREATE TABLE test_partition_${i}
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
    n char(20)
)
STORED AS ORC"
done
