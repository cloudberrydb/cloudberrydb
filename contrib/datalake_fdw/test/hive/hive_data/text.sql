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
