CREATE TABLE test_all_type
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
INSERT INTO test_all_type VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01);
INSERT INTO test_all_type VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01);
INSERT INTO test_all_type VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01);
INSERT INTO test_all_type VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01);
INSERT INTO test_all_type VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01);


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


CREATE TABLE test_partition_1_smallint
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
    m smallint
)
STORED AS ORC;
INSERT INTO test_partition_1_smallint VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1);
INSERT INTO test_partition_1_smallint VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2);
INSERT INTO test_partition_1_smallint VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3);
INSERT INTO test_partition_1_smallint VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4);
INSERT INTO test_partition_1_smallint VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5);


CREATE TABLE test_partition_1_bigint
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
    m bigint
)
STORED AS ORC;
INSERT INTO test_partition_1_bigint VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1);
INSERT INTO test_partition_1_bigint VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2);
INSERT INTO test_partition_1_bigint VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3);
INSERT INTO test_partition_1_bigint VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4);
INSERT INTO test_partition_1_bigint VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5);


CREATE TABLE test_partition_1_double
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
    m double
)
STORED AS ORC;
INSERT INTO test_partition_1_double VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1.01);
INSERT INTO test_partition_1_double VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2.01);
INSERT INTO test_partition_1_double VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3.01);
INSERT INTO test_partition_1_double VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4.01);
INSERT INTO test_partition_1_double VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5.01);


CREATE TABLE test_partition_1_decimal
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
    m decimal(20, 10)
)
STORED AS ORC;
INSERT INTO test_partition_1_decimal VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1.01);
INSERT INTO test_partition_1_decimal VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2.01);
INSERT INTO test_partition_1_decimal VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3.01);
INSERT INTO test_partition_1_decimal VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4.01);
INSERT INTO test_partition_1_decimal VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5.01);


CREATE TABLE test_partition_1_char
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
    m char(10)
)
STORED AS ORC;
INSERT INTO test_partition_1_char VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, '1');
INSERT INTO test_partition_1_char VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, '2');
INSERT INTO test_partition_1_char VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, '3');
INSERT INTO test_partition_1_char VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, '4');
INSERT INTO test_partition_1_char VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, '5');


CREATE TABLE test_partition_1_varchar
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
    m varchar(10)
)
STORED AS ORC;
INSERT INTO test_partition_1_varchar VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, '1');
INSERT INTO test_partition_1_varchar VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, '2');
INSERT INTO test_partition_1_varchar VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, '3');
INSERT INTO test_partition_1_varchar VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, '4');
INSERT INTO test_partition_1_varchar VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, '5');


CREATE TABLE test_partition_1_string
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
    m string
)
STORED AS ORC;
INSERT INTO test_partition_1_string VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, '1');
INSERT INTO test_partition_1_string VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, '2');
INSERT INTO test_partition_1_string VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, '3');
INSERT INTO test_partition_1_string VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, '4');
INSERT INTO test_partition_1_string VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, '5');


CREATE TABLE test_partition_1_float
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
    m float
)
STORED AS ORC;
INSERT INTO test_partition_1_float VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1.01);
INSERT INTO test_partition_1_float VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2.01);
INSERT INTO test_partition_1_float VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3.01);
INSERT INTO test_partition_1_float VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4.01);
INSERT INTO test_partition_1_float VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5.01);



CREATE TABLE test_partition_1_date
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
    m date
)
STORED AS ORC;
INSERT INTO test_partition_1_date VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, '2020-01-01');
INSERT INTO test_partition_1_date VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, '2020-02-01');
INSERT INTO test_partition_1_date VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, '2020-03-01');
INSERT INTO test_partition_1_date VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, '2020-04-01');
INSERT INTO test_partition_1_date VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, '2020-05-01');


CREATE TABLE test_partition_2
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
STORED AS ORC;
INSERT INTO test_partition_2 VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1, '1');
INSERT INTO test_partition_2 VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2, '2');
INSERT INTO test_partition_2 VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3, '3');
INSERT INTO test_partition_2 VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4, '4');
INSERT INTO test_partition_2 VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5, '5');


CREATE TABLE test_partition_3
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
    n char(20),
    o date
)
STORED AS ORC;
INSERT INTO test_partition_3 VALUES (1, 1, 1, 1, 1, 1, '1', '2020-01-01 01:01:01', '2020-01-01', '1', '1', 10.01, 1, '1', '2020-01-01');
INSERT INTO test_partition_3 VALUES (2, 2, 2, 2, 2, 2, '2', '2020-02-02 02:02:02', '2020-02-01', '2', '2', 11.01, 2, '2', '2020-02-01');
INSERT INTO test_partition_3 VALUES (3, 3, 3, 3, 3, 3, '3', '2020-03-03 03:03:03', '2020-03-01', '3', '3', 12.01, 3, '3', '2020-03-01');
INSERT INTO test_partition_3 VALUES (4, 4, 4, 4, 4, 4, '4', '2020-04-04 04:04:04', '2020-04-01', '4', '4', 13.01, 4, '4', '2020-04-01');
INSERT INTO test_partition_3 VALUES (5, 5, 5, 5, 5, 5, '5', '2020-05-05 05:05:05', '2020-05-01', '5', '5', 14.01, 5, '5', '2020-05-01');


CREATE TABLE test_textfile_default
(
	a string,
	b string,
	c string,
	d string
)
STORED AS TEXTFILE;
INSERT INTO test_textfile_default VALUES ('1', '1', '1', '1');
INSERT INTO test_textfile_default VALUES ('2', '2', '2', '2');
INSERT INTO test_textfile_default VALUES ('3', '3', '3', '3');
INSERT INTO test_textfile_default VALUES ('4', '4', '4', '4');
INSERT INTO test_textfile_default VALUES ('5', '5', '5', '5');


CREATE TABLE test_textfile_custom
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
INSERT INTO test_textfile_custom VALUES ('1', '1', '1', null);
INSERT INTO test_textfile_custom VALUES ('2', '2', null, '2');
INSERT INTO test_textfile_custom VALUES ('3', '3', '3', '3');
INSERT INTO test_textfile_custom VALUES ('4', null, '4', '4');
INSERT INTO test_textfile_custom VALUES ('5', '5', '5', '5');


CREATE TABLE test_csv_default
(
	a string,
	b string,
	c string,
	d string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE;
INSERT INTO test_csv_default VALUES ('1', '1', '1', '1');
INSERT INTO test_csv_default VALUES ('2', '2', '2', null);
INSERT INTO test_csv_default VALUES ('3', '3', '3', '3');
INSERT INTO test_csv_default VALUES ('4', null, '4', '4');
INSERT INTO test_csv_default VALUES ('5', '5', '5', '5');


CREATE TABLE test_csv_custom
(
	a string,
	b string,
	c string,
	d string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE;
INSERT INTO test_csv_custom VALUES ('1', '1', '1', null);
INSERT INTO test_csv_custom VALUES ('2', '2', '2', '2');
INSERT INTO test_csv_custom VALUES ('3', null, '3', '3');
INSERT INTO test_csv_custom VALUES ('4', '4', '4', '4');
INSERT INTO test_csv_custom VALUES ('5', '5', '5', '5');
