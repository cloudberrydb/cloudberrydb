-- test heap table sequence 

-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED BY(a);
CREATE TABLE IF NOT EXISTS t2 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED RANDOMLY;
CREATE TABLE IF NOT EXISTS t3 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED REPLICATED;

-- SET VECTORIZATION ON

SET vector.enable_vectorization=ON;
SET hashdata_use_late_materialize=ON;
-- empty data query

SELECT * FROM t1;
SELECT a FROM t1;
SELECT b FROM t1;
SELECT c FROM t1;
SELECT * FROM t2;
SELECT a FROM t2;
SELECT b FROM t2;
SELECT c FROM t2;
SELECT * FROM t3;
SELECT a FROM t3;
SELECT b FROM t3;
SELECT c FROM t3;

SELECT * FROM t1 WHERE a=1;
SELECT a FROM t1 WHERE a=1;
SELECT b FROM t1 WHERE a=1;
SELECT c FROM t1 WHERE a=1;
SELECT * FROM t2 WHERE a=1;
SELECT a FROM t2 WHERE a=1;
SELECT b FROM t2 WHERE a=1;
SELECT c FROM t2 WHERE a=1;
SELECT * FROM t3 WHERE a=1;
SELECT a FROM t3 WHERE a=1;
SELECT b FROM t3 WHERE a=1;
SELECT c FROM t3 WHERE a=1;

SELECT * FROM t1 WHERE b='a';
SELECT a FROM t1 WHERE b='a';
SELECT b FROM t1 WHERE b='a';
SELECT c FROM t1 WHERE b='a';
SELECT * FROM t2 WHERE b='a';
SELECT a FROM t2 WHERE b='a';
SELECT b FROM t2 WHERE b='a';
SELECT c FROM t2 WHERE b='a';
SELECT * FROM t3 WHERE b='a';
SELECT a FROM t3 WHERE b='a';
SELECT b FROM t3 WHERE b='a';
SELECT c FROM t3 WHERE b='a';

SELECT * FROM t1 WHERE b!='a';
SELECT a FROM t1 WHERE b!='a';
SELECT b FROM t1 WHERE b!='a';
SELECT c FROM t1 WHERE b!='a';
SELECT * FROM t2 WHERE b!='a';
SELECT a FROM t2 WHERE b!='a';
SELECT b FROM t2 WHERE b!='a';
SELECT c FROM t2 WHERE b!='a';
SELECT * FROM t3 WHERE b!='a';
SELECT a FROM t3 WHERE b!='a';
SELECT b FROM t3 WHERE b!='a';
SELECT c FROM t3 WHERE b!='a';

SELECT * FROM t1 WHERE a=1 AND b='a';
SELECT a FROM t1 WHERE a=1 AND b='a';
SELECT b FROM t1 WHERE a=1 AND b='a';
SELECT c FROM t1 WHERE a=1 AND b='a';
SELECT * FROM t2 WHERE a=1 AND b='a';
SELECT a FROM t2 WHERE a=1 AND b='a';
SELECT b FROM t2 WHERE a=1 AND b='a';
SELECT c FROM t2 WHERE a=1 AND b='a';
SELECT * FROM t3 WHERE a=1 AND b='a';
SELECT a FROM t3 WHERE a=1 AND b='a';
SELECT b FROM t3 WHERE a=1 AND b='a';
SELECT c FROM t3 WHERE a=1 AND b='a';

SELECT * FROM t1 WHERE a=1 AND b='a' AND c='a';
SELECT a FROM t1 WHERE a=1 AND b='a' AND c='a';
SELECT b FROM t1 WHERE a=1 AND b='a' AND c='a';
SELECT c FROM t1 WHERE a=1 AND b='a' AND c='a';
SELECT * FROM t2 WHERE a=1 AND b='a' AND c='a';
SELECT a FROM t2 WHERE a=1 AND b='a' AND c='a';
SELECT b FROM t2 WHERE a=1 AND b='a' AND c='a';
SELECT c FROM t2 WHERE a=1 AND b='a' AND c='a';
SELECT * FROM t3 WHERE a=1 AND b='a' AND c='a';
SELECT a FROM t3 WHERE a=1 AND b='a' AND c='a';
SELECT b FROM t3 WHERE a=1 AND b='a' AND c='a';
SELECT c FROM t3 WHERE a=1 AND b='a' AND c='a';


-- SELECT * FROM t1 WHERE a=1 OR a=2;
-- SELECT a FROM t1 WHERE a=1 OR a=2;
-- SELECT b FROM t1 WHERE a=1 OR a=2;
-- SELECT * FROM t2 WHERE a=1 OR a=2;
-- SELECT a FROM t2 WHERE a=1 OR a=2;
-- SELECT b FROM t2 WHERE a=1 OR a=2;
-- SELECT * FROM t3 WHERE a=1 OR a=2;
-- SELECT a FROM t3 WHERE a=1 OR a=2;
-- SELECT b FROM t3 WHERE a=1 OR a=2;


-- load data

INSERT INTO t1 VALUES (1, 'a', 'a'), (2, 'a', 'a'),(3, 'a', 'a');
INSERT INTO t2 VALUES (1, 'a', 'a'), (2, 'a', 'a'),(3, 'a', 'a');
INSERT INTO t3 VALUES (1, 'a', 'a'), (2, 'a', 'a'),(3, 'a', 'a');

-- Query

SELECT * FROM t1;
SELECT a FROM t1;
SELECT b FROM t1;
SELECT c FROM t1;
SELECT * FROM t2;
SELECT a FROM t2;
SELECT b FROM t2;
SELECT c FROM t2;
SELECT * FROM t3;
SELECT a FROM t3;
SELECT c FROM t3;
SELECT b FROM t3;

SELECT * FROM t1 WHERE a=1;
SELECT a FROM t1 WHERE a=1;
SELECT b FROM t1 WHERE a=1;
SELECT c FROM t1 WHERE a=1;
SELECT * FROM t2 WHERE a=1;
SELECT a FROM t2 WHERE a=1;
SELECT b FROM t2 WHERE a=1;
SELECT c FROM t2 WHERE a=1;
SELECT * FROM t3 WHERE a=1;
SELECT a FROM t3 WHERE a=1;
SELECT b FROM t3 WHERE a=1;
SELECT c FROM t3 WHERE a=1;

SELECT * FROM t1 WHERE b='a';
SELECT a FROM t1 WHERE b='a';
SELECT b FROM t1 WHERE b='a';
SELECT c FROM t1 WHERE b='a';
SELECT * FROM t2 WHERE b='a';
SELECT a FROM t2 WHERE b='a';
SELECT b FROM t2 WHERE b='a';
SELECT c FROM t2 WHERE b='a';
SELECT * FROM t3 WHERE b='a';
SELECT a FROM t3 WHERE b='a';
SELECT b FROM t3 WHERE b='a';
SELECT c FROM t3 WHERE b='a';

SELECT * FROM t1 WHERE b!='a';
SELECT a FROM t1 WHERE b!='a';
SELECT b FROM t1 WHERE b!='a';
SELECT c FROM t1 WHERE b!='a';
SELECT * FROM t2 WHERE b!='a';
SELECT a FROM t2 WHERE b!='a';
SELECT b FROM t2 WHERE b!='a';
SELECT c FROM t2 WHERE b!='a';
SELECT * FROM t3 WHERE b!='a';
SELECT a FROM t3 WHERE b!='a';
SELECT b FROM t3 WHERE b!='a';
SELECT c FROM t3 WHERE b!='a';

-- SELECT * FROM t1 WHERE a=1 AND b='a';
-- SELECT a FROM t1 WHERE a=1 AND b='a';
-- SELECT b FROM t1 WHERE a=1 AND b='a';
-- SELECT c FROM t1 WHERE a=1 AND b='a';
-- SELECT * FROM t2 WHERE a=1 AND b='a';
-- SELECT a FROM t2 WHERE a=1 AND b='a';
-- SELECT b FROM t2 WHERE a=1 AND b='a';
-- SELECT c FROM t2 WHERE a=1 AND b='a';
-- SELECT * FROM t3 WHERE a=1 AND b='a';
-- SELECT a FROM t3 WHERE a=1 AND b='a';
-- SELECT b FROM t3 WHERE a=1 AND b='a';
-- SELECT c FROM t3 WHERE a=1 AND b='a';

-- SELECT * FROM t1 WHERE a=1 AND b='a' AND c='a';
-- SELECT a FROM t1 WHERE a=1 AND b='a' AND c='a';
-- SELECT b FROM t1 WHERE a=1 AND b='a' AND c='a';
-- SELECT c FROM t1 WHERE a=1 AND b='a' AND c='a';
-- SELECT * FROM t2 WHERE a=1 AND b='a' AND c='a';
-- SELECT a FROM t2 WHERE a=1 AND b='a' AND c='a';
-- SELECT b FROM t2 WHERE a=1 AND b='a' AND c='a';
-- SELECT c FROM t2 WHERE a=1 AND b='a' AND c='a';
-- SELECT * FROM t3 WHERE a=1 AND b='a' AND c='a';
-- SELECT a FROM t3 WHERE a=1 AND b='a' AND c='a';
-- SELECT b FROM t3 WHERE a=1 AND b='a' AND c='a';
-- SELECT c FROM t3 WHERE a=1 AND b='a' AND c='a';

-- SELECT * FROM t1 WHERE a=1 OR a=2;
-- SELECT a FROM t1 WHERE a=1 OR a=2;
-- SELECT b FROM t1 WHERE a=1 OR a=2;
-- SELECT * FROM t2 WHERE a=1 OR a=2;
-- SELECT a FROM t2 WHERE a=1 OR a=2;
-- SELECT b FROM t2 WHERE a=1 OR a=2;
-- SELECT * FROM t3 WHERE a=1 OR a=2;
-- SELECT a FROM t3 WHERE a=1 OR a=2;
-- SELECT b FROM t3 WHERE a=1 OR a=2;

-- drop table

DROP TABLE if EXISTS t1;
DROP TABLE if EXISTS t2;
DROP TABLE if EXISTS t3;


-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED BY(a);
CREATE TABLE IF NOT EXISTS t2 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED RANDOMLY;
CREATE TABLE IF NOT EXISTS t3 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED REPLICATED;

-- SET VECTORIZATION ON

SET vector.enable_vectorization=ON;

-- empty data query

INSERT INTO t1 VALUES (1, 'abcl', 'ablx'), (2, 'flsaa', 'ssddf'),(3, 'ddsse', 'dddcd');
INSERT INTO t2 VALUES (1, 'abcl', 'ablx'), (2, 'flsaa', 'ssddf'),(3, 'ddsse', 'dddcd');
INSERT INTO t3 VALUES (1, 'abcl', 'ablx'), (2, 'flsaa', 'ssddf'),(3, 'ddsse', 'dddcd');

-- Query

SELECT * FROM t1;
SELECT a FROM t1;
SELECT b FROM t1;
SELECT c FROM t1;
SELECT * FROM t2;
SELECT a FROM t2;
SELECT b FROM t2;
SELECT c FROM t2;
SELECT * FROM t3;
SELECT a FROM t3;
SELECT c FROM t3;
SELECT b FROM t3;

-- test like ops

SELECT * FROM t1 where b like '%abc%';
SELECT * FROM t1 where b like 'abc%';
SELECT * FROM t1 where b like '%bc%';
SELECT * FROM t1 where b like '%bcl';
SELECT * FROM t1 where b like '_bc%';
SELECT * FROM t1 where b like 'a_c%';

SELECT * FROM t2 where b like '%abc%';
SELECT * FROM t2 where b like 'abc%';
SELECT * FROM t2 where b like '%bc%';
SELECT * FROM t2 where b like '%bcl';
SELECT * FROM t2 where b like '_bc%';
SELECT * FROM t2 where b like 'a_c%';

SELECT * FROM t3 where b like '%abc%';
SELECT * FROM t3 where b like 'abc%';
SELECT * FROM t3 where b like '%bc%';
SELECT * FROM t3 where b like '%bcl';
SELECT * FROM t3 where b like '_bc%';
SELECT * FROM t3 where b like 'a_c%';

-- drop table

DROP TABLE if EXISTS t1;
DROP TABLE if EXISTS t2;
DROP TABLE if EXISTS t3;


-- create table

CREATE TABLE IF NOT EXISTS t1 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED BY(a);
CREATE TABLE IF NOT EXISTS t2 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED RANDOMLY;
CREATE TABLE IF NOT EXISTS t3 (a INT, b TEXT, c VARCHAR(20)) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED REPLICATED;


-- SET VECTORIZATION ON

SET vector.enable_vectorization=ON;

INSERT INTO t1 VALUES (1, NULL, 'ablx'), (2, 'flsaa', NULL),(3, NULL, NULL);
INSERT INTO t2 VALUES (1, NULL, 'ablx'), (2, 'flsaa', NULL),(3, NULL, NULL);
INSERT INTO t3 VALUES (1, NULL, 'ablx'), (2, 'flsaa', NULL),(3, NULL, NULL);

-- Query

SELECT * FROM t1;
SELECT a FROM t1;
SELECT b FROM t1;
SELECT c FROM t1;
SELECT * FROM t2;
SELECT a FROM t2;
SELECT b FROM t2;
SELECT c FROM t2;
SELECT * FROM t3;
SELECT a FROM t3;
SELECT c FROM t3;
SELECT b FROM t3;

-- drop table

DROP TABLE if EXISTS t1;
DROP TABLE if EXISTS t2;
DROP TABLE if EXISTS t3;

