-- set vectorization on

SET vector.enable_vectorization=ON;

--creat table

CREATE TABLE IF NOT EXISTS t1 (a INT, b BIGINT, c TEXT) WITH(APPENDONLY=true, ORIENTATION=column)  DISTRIBUTED BY(a);

-- load data

INSERT INTO t1 SELECT i, i%5, 'abc' FROM GENERATE_SERIES(1, 20)i;
INSERT INTO t1 SELECT i, i%5, 'ssdfjdjasj' FROM GENERATE_SERIES(1, 20)i;
INSERT INTO t1 SELECT i, i%5, 'kkskdfd' FROM GENERATE_SERIES(1, 20)i;

-- set multi phase agg on

SET gp_enable_multiphase_agg=on;

-- query

-- SELECT a+1 col1, a-1 col2 FROM t1 WHERE a > 5 ORDER BY a,b;
SELECT  SUM(a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  SUM(a-1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  SUM(a+1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  COUNT(*) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  COUNT(distinct a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  COUNT(distinct b) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  COUNT(distinct c) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  AVG(a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  AVG(a-1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  AVG(a+1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  MAX(a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  MAX(a+1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  MAX(a-1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  MIN(a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  MIN(a+1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
SELECT  MIN(a-1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;

-- SELECT a+1 col1, a-1 col2 FROM t1 WHERE a > 5 ORDER BY c;
-- SELECT  SUM(a) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  SUM(a-1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  SUM(a+1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  COUNT(*) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  AVG(a) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  AVG(a-1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  AVG(a+1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
SELECT  MAX(a) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MAX(a+1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MAX(a-1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MIN(a) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MIN(a+1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MIN(a-1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;

-- set multi phase agg off

SET gp_enable_multiphase_agg=off;

-- query

-- SELECT a+1 col1, a-1 col2 FROM t1 WHERE a > 5 ORDER BY a,b;
-- SELECT  SUM(a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  SUM(a-1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  SUM(a+1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  COUNT(*) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  COUNT(distinct a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  COUNT(distinct b) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  COUNT(distinct c) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  AVG(a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  AVG(a-1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  AVG(a+1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  MAX(a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  MAX(a+1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  MAX(a-1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  MIN(a) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  MIN(a+1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;
-- SELECT  MIN(a-1) FROM t1 WHERE a > 5 GROUP BY a, b ORDER BY a,b;

-- SELECT a+1 col1, a-1 col2 FROM t1 WHERE a > 5 ORDER BY c;
-- SELECT  SUM(a) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  SUM(a-1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  SUM(a+1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  COUNT(*) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  AVG(a) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  AVG(a-1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  AVG(a+1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MAX(a) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MAX(a+1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MAX(a-1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MIN(a) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MIN(a+1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;
-- SELECT  MIN(a-1) FROM t1 WHERE a > 5 GROUP BY c ORDER BY c;


-- drop table
DROP TABLE IF EXISTS t1;

-- timestamp test
DROP TABLE IF EXISTS tts;
CREATE TABLE tts(c1 INT, c2 TIMESTAMP) WITH(appendonly=true, orientation=column);
INSERT INTO tts VALUES (1, '850-01-01 09:12:34.567891'), (1, '1950-12-31 12:34:56.789123'), (1, '2050-06-01 21:56:12.345678');
-- The execMain.c line number difference is ignored.
-- Here, the line number of the code is replaced with `###` by matching
--
-- start_matchsubs
-- m/\(execMain.c:\d+\)/
-- s/\(execMain.c:\d+\)/\(execMain.c:###\)/
-- end_matchsubs
-- FIXME :wait decimal type to support
SET vector.enable_vectorization = off;
SELECT c2,
  extract(epoch       FROM c2) AS epoch,
  extract(millennium  FROM c2) AS millennium,
  extract(century     FROM c2) AS century,
  extract(decade      FROM c2) AS decade,
  extract(year        FROM c2) AS year,
  extract(isoyear     FROM c2) AS isoyear,
  extract(quarter     FROM c2) AS quarter,
  extract(month       FROM c2) AS month,
  extract(week        FROM c2) AS week,
  extract(day         FROM c2) AS day,
  extract(dow         FROM c2) AS dow,
  extract(isodow      FROM c2) AS isodow,
  extract(doy         FROM c2) AS doy,
  extract(hour        FROM c2) AS hour,
  extract(minute      FROM c2) AS minute,
  extract(second      FROM c2) AS second,
  extract(millisecond FROM c2) AS millisecond,
  extract(microsecond FROM c2) AS microsecond
FROM tts;

SET vector.enable_vectorization = off;
SELECT c2,
  extract(epoch       FROM c2) AS epoch,
  extract(millennium  FROM c2) AS millennium,
  extract(century     FROM c2) AS century,
  extract(decade      FROM c2) AS decade,
  extract(year        FROM c2) AS year,
  extract(isoyear     FROM c2) AS isoyear,
  extract(quarter     FROM c2) AS quarter,
  extract(month       FROM c2) AS month,
  extract(week        FROM c2) AS week,
  extract(day         FROM c2) AS day,
  extract(dow         FROM c2) AS dow,
  extract(isodow      FROM c2) AS isodow,
  extract(doy         FROM c2) AS doy,
  extract(hour        FROM c2) AS hour,
  extract(minute      FROM c2) AS minute,
  extract(second      FROM c2) AS second,
  extract(millisecond FROM c2) AS millisecond,
  extract(microsecond FROM c2) AS microsecond
FROM tts;
DROP TABLE IF EXISTS tts;
