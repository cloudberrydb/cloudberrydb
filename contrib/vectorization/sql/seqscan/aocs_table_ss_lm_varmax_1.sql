-- test heap table sequence 

-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b INT, c TEXT) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED BY(a);

-- SET VECTORIZATION ON

SET vector.enable_vectorization=ON;
SET hashdata_use_late_materialize=ON;


-- SET MAX VERTORIZATION COUNT

SET vector.max_batch_size=128;

-- load data

SET vector.enable_vectorization=OFF;

INSERT INTO t1 SELECT i, i % 16, chr(65 + i % 26)  FROM GENERATE_SERIES(1, 2048) i;


-- start query

SET vector.enable_vectorization=ON;

SELECT * FROM t1 WHERE b = 1 ORDER BY a, c;

SELECT * FROM t1 WHERE b < 0;

SELECT * FROM t1 WHERE b > 15;

SELECT * FROM t1 WHERE b = 15 ORDER BY a, c;

SELECT * FROM t1 WHERE a = 1000 ORDER BY b, c;

SELECT * FROM t1 WHERE a < 1000 and a > 990;

SELECT * FROM t1 WHERE a > 2000 and b = 10;


-- drop table

DROP TABLE t1;





