-- test heap table sequence 

-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b date, c TIMESTAMP) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED BY(a);
CREATE TABLE IF NOT EXISTS t2 (a INT, b date, c TIMESTAMP) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED RANDOMLY;
CREATE TABLE IF NOT EXISTS t3 (a INT, b date, c timestamp) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED REPLICATED;

-- SET VECTORIZATION ON

SET vector.enable_vectorization=ON;

-- empty data query

INSERT INTO t1 VALUES (1, '1957-04-09', 'today'), (2, '1996-03-01', 'yesterday'),(3, '1999-01-08', 'tomorrow');
INSERT INTO t2 VALUES (1, '1957-04-09', 'today'), (2, '1996-03-01', 'yesterday'),(3, '1999-01-08', 'tomorrow');
INSERT INTO t3 VALUES (1, '1957-04-09', 'today'), (2, '1996-03-01', 'yesterday'),(3, '1999-01-08', 'tomorrow');

-- Query

SELECT a FROM t1;
SELECT b FROM t1;
SELECT a FROM t2;
SELECT b FROM t2;
SELECT a FROM t3;
SELECT b FROM t3;

SELECT a, b FROM t1 where b < '2000-01-01';
SELECT a, b FROM t1 where b <= '2000-01-01';
SELECT a, b FROM t1 where b = '1996-03-01';
SELECT a, b FROM t1 where b != '1996-03-01';
SELECT a, b FROM t1 where b > '2000-01-01';
SELECT a, b FROM t1 where b >= '2000-01-01';

SELECT a, b FROM t2 where b < '2000-01-01';
SELECT a, b FROM t2 where b <= '2000-01-01';
SELECT a, b FROM t2 where b = '1996-03-01';
SELECT a, b FROM t2 where b != '1996-03-01';
SELECT a, b FROM t2 where b > '2000-01-01';
SELECT a, b FROM t2 where b >= '2000-01-01';

SELECT a, b FROM t3 where b < '2000-01-01';
SELECT a, b FROM t3 where b <= '2000-01-01';
SELECT a, b FROM t3 where b = '1996-03-01';
SELECT a, b FROM t3 where b != '1996-03-01';
SELECT a, b FROM t3 where b > '2000-01-01';
SELECT a, b FROM t3 where b >= '2000-01-01';

-- SELECT a, b, c FROM t1 WHERE c = timestamp without time zone 'today';
-- SELECT a, b, c FROM t1 WHERE c = timestamp without time zone 'tomorrow';
-- SELECT a, b, c FROM t1 WHERE c = timestamp without time zone 'yesterday';

-- SELECT a, b, c FROM t2 WHERE c = timestamp without time zone 'today';
-- SELECT a, b, c FROM t2 WHERE c = timestamp without time zone 'tomorrow';
-- SELECT a, b, c FROM t2 WHERE c = timestamp without time zone 'yesterday';

-- SELECT a, b, c FROM t3 WHERE c = timestamp without time zone 'today';
-- SELECT a, b, c FROM t3 WHERE c = timestamp without time zone 'tomorrow';
-- SELECT a, b, c FROM t3 WHERE c = timestamp without time zone 'yesterday';

DROP TABLE if EXISTS t1;
DROP TABLE if EXISTS t2;
DROP TABLE if EXISTS t3;

