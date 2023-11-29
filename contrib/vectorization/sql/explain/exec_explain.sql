-- query plan test for check whether is vectorization plan

-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b INT) DISTRIBUTED BY(a);
CREATE TABLE IF NOT EXISTS t2 (a INT, b INT) DISTRIBUTED RANDOMLY;
CREATE TABLE IF NOT EXISTS t3 (a INT, b INT) DISTRIBUTED REPLICATED;

-- SET VECTORIZATION ON

SET vector.enable_vectorization=ON;

-- load data
INSERT INTO t1 VALUES (1, 2), (2, 3),(3, 4);
INSERT INTO t2 VALUES (1, 2), (2, 3),(3, 4);
INSERT INTO t3 VALUES (1, 2), (2, 3),(3, 4);

-- analyze table
ANALYZE t1;
ANALYZE t2;
ANALYZE t3;

-- explain query

EXPLAIN (costs, verbose) SELECT * FROM t1;

EXPLAIN (costs, verbose) SELECT * FROM t2;

EXPLAIN (costs, verbose) SELECT * FROM t3;

EXPLAIN (costs, verbose) SELECT * FROM t1 WHERE a=1;

EXPLAIN (costs, verbose) SELECT * FROM t2 WHERE a=1;

EXPLAIN (costs, verbose) SELECT * FROM t3 WHERE a=1;

EXPLAIN (costs, verbose) SELECT * FROM t1 WHERE a IS NOT NULL;

EXPLAIN (costs, verbose) SELECT * FROM t2 WHERE a IS NOT NULL;

EXPLAIN (costs, verbose) SELECT * FROM t3 WHERE a IS NOT NULL;

EXPLAIN (costs, verbose) SELECT * FROM t1 WHERE a IS NULL;

EXPLAIN (costs, verbose) SELECT * FROM t2 WHERE a IS NULL;

EXPLAIN (costs, verbose) SELECT * FROM t3 WHERE a IS NULL;

EXPLAIN (costs, verbose) SELECT * FROM t1 ORDER BY a;

EXPLAIN (costs, verbose) SELECT * FROM t2 ORDER BY a;

EXPLAIN (costs, verbose) SELECT * FROM t3 ORDER BY a;

EXPLAIN (costs, verbose) SELECT * FROM t1 ORDER BY b;

EXPLAIN (costs, verbose) SELECT * FROM t2 ORDER BY b;

EXPLAIN (costs, verbose) SELECT * FROM t3 ORDER BY b;

EXPLAIN (costs, verbose) SELECT * FROM t1 ORDER BY a,b;

EXPLAIN (costs, verbose) SELECT * FROM t2 ORDER BY a,b;

EXPLAIN (costs, verbose) SELECT * FROM t3 ORDER BY a,b;

EXPLAIN (costs, verbose) SELECT a+1 FROM t1;

EXPLAIN (costs, verbose) SELECT a+1 FROM t2;

EXPLAIN (costs, verbose) SELECT a+1 FROM t3;

EXPLAIN (costs, verbose) SELECT a+b FROM t1;

EXPLAIN (costs, verbose) SELECT a+b FROM t2;

EXPLAIN (costs, verbose) SELECT a+b FROM t3;

EXPLAIN (costs, verbose) SELECT a*2 FROM t1;

EXPLAIN (costs, verbose) SELECT a*2 FROM t2;

EXPLAIN (costs, verbose) SELECT a*2 FROM t3;


-- drop table

DROP TABLE if EXISTS t1;
DROP TABLE if EXISTS t2;
DROP TABLE if EXISTS t3;


