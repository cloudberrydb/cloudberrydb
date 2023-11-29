-- test cases collected from development

-- create table

CREATE TABLE t1 (a INT, b VARCHAR(100), c VARCHAR(100)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED BY(a);

-- set configuration
SET vector.enable_vectorization=off;

-- insert data

INSERT INTO t1 SELECT i % 100, 'adafrsdgrgdffbgfg', 'sscdscdsvfdbhfd' FROM generate_series(1, 100000)i;

-- set configuration
SET vector.enable_vectorization=on;
SET hashdata_use_late_materialize=on;

-- queries

SELECT a, count(b), count(c) FROM t1 WHERE a=10 GROUP BY a;
SELECT a, count(b), count(c) FROM t1 WHERE a<10 GROUP BY a;
SELECT a, count(b), count(c) FROM t1 WHERE b != 'adafrsdgrgdffbgfg' GROUP BY a;
SELECT a, count(b), count(c) FROM t1 WHERE a = 10 and b = 'adafrsdgrgdffbgfg' GROUP BY a ;
SELECT a, count(b), count(c) FROM t1 WHERE a is NULL GROUP BY a;
SELECT a, count(b), count(c) FROM t1 WHERE a is NOT NULL and b != 'adafrsdgrgdffbgfg' GROUP BY a ;

-- drop table

DROP TABLE t1;
