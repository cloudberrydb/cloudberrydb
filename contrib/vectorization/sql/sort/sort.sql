-- test sort executor

-- set vectorization on

SET vector.enable_vectorization=ON;
SET vector.max_batch_size = 13;

-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b INT) DISTRIBUTED BY(a);

-- load data

INSERT INTO t1 SELECT 1000 - i, i * i - i  FROM GENERATE_SERIES(1, 100)i;

-- query

SELECT * FROM t1 ORDER BY a;
SELECT * FROM t1 ORDER BY b;

-- drop table
DROP TABLE t1;

-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b INT) DISTRIBUTED BY(a);

-- load data

INSERT INTO t1 SELECT i % 11, i * (i / 3) FROM GENERATE_SERIES(1, 100)i;

-- query

SELECT * FROM t1 ORDER BY a,b;

-- drop table
DROP TABLE t1;

CREATE TABLE IF NOT EXISTS t1 (a INT, b TEXT) WITH(APPENDONLY=true, ORIENTATION=column)  DISTRIBUTED BY(a);

-- load data
INSERT INTO t1 SELECT i, i::text FROM GENERATE_SERIES(110, 150)i;
INSERT INTO t1 SELECT i % 7, i::text FROM GENERATE_SERIES(1, 100)i;

-- query
SELECT * FROM t1 ORDER BY b;
SELECT * FROM t1 ORDER BY a,b;

-- drop table
DROP TABLE t1;

CREATE COLLATION c1 (LOCALE = 'en_US.UTF-8');
CREATE TABLE test_locale (a char(8) COLLATE "C", b char(8) COLLATE c1) using ao_column;
INSERT INTO test_locale VALUES ('XXXX', 'A');
INSERT INTO test_locale VALUES ('ABAB', 'b');
INSERT INTO test_locale VALUES ('ABAB', 'c');
INSERT INTO test_locale VALUES ('BBBB', 'D');
INSERT INTO test_locale VALUES ('BBBB', 'e');
INSERT INTO test_locale VALUES ('bbbb', 'F');
INSERT INTO test_locale VALUES ('cccc', 'g');
INSERT INTO test_locale VALUES ('cccc', 'h');
INSERT INTO test_locale VALUES ('CCCC', 'I');
INSERT INTO test_locale VALUES ('CCCC', 'j');
select a from test_locale order by a;
select b from test_locale order by b;
drop table test_locale;
drop COLLATION c1;