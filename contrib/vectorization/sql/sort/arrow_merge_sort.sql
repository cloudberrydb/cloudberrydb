-- test sort executor

-- set vectorization on

SET vector.enable_vectorization=ON;
SET vector.max_batch_size = 13;
SET hashdata_max_debug_sort_cnt = 1;

-- drop table 
DROP TABLE IF EXISTS t1;

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
