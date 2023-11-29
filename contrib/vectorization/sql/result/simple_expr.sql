-- test result executor

-- set vectorization on

SET vector.enable_vectorization=ON;

-- create table

CREATE TABLE if not exists t1 (a INT, b INT) DISTRIBUTED BY (a);


-- load

INSERT INTO t1 SELECT i, i * 2 FROM GENERATE_SERIES(1, 100)i;

-- query

SELECT a+1 FROM t1;
SELECT a-1 FROM t1;
SELECT a+1+2-1 FROM t1;

-- drop table

DROP TABLE t1;


-- create table

CREATE TABLE if not exists t1 (a INT, b INT) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED BY (a);


-- load

INSERT INTO t1 SELECT i, i * 2 FROM GENERATE_SERIES(1, 100)i;

-- query

SELECT a+1 FROM t1;
SELECT a-1 FROM t1;
SELECT a+1+2-1 FROM t1;

-- drop table

DROP TABLE t1;



