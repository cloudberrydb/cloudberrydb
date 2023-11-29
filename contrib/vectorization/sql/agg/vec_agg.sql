SET vector.enable_vectorization = on;


-- *** hash agg ***
SET optimizer = off;
SET enable_groupagg = off;

-- normal argument
EXPLAIN (costs off)
SELECT count(cint4), sum(cint4), avg(cint4), min(cint4), max(cint4),
       count(cint8), sum(cint8), avg(cint8), min(cint8), max(cint8),
       count(cint2), sum(cint2), avg(cint2), min(cint2), max(cint2),
       count(cdate), min(cdate), max(cdate),
       count(cts)  , min(cts)  , max(cts)  ,
       count(ctext), min(ctext), max(ctext),
       count(),
       grpkey
FROM tagg GROUP BY grpkey ORDER BY grpkey;

SELECT count(cint4), sum(cint4), avg(cint4), min(cint4), max(cint4),
       count(cint8), sum(cint8), avg(cint8), min(cint8), max(cint8),
       count(cint2), sum(cint2), avg(cint2), min(cint2), max(cint2),
       count(cdate), min(cdate), max(cdate),
       count(cts)  , min(cts)  , max(cts)  ,
       count(ctext), min(ctext), max(ctext),
       count(),
       grpkey
FROM tagg GROUP BY grpkey ORDER BY grpkey;

-- distinct argument
EXPLAIN (costs off)
SELECT count(DISTINCT cint4), sum(DISTINCT cint4), avg(DISTINCT cint4), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;

EXPLAIN (costs off)
SELECT count(DISTINCT cint8), sum(DISTINCT cint8), avg(DISTINCT cint8), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;

EXPLAIN (costs off)
SELECT count(DISTINCT ctext), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;

SELECT count(DISTINCT cint4), sum(DISTINCT cint4), avg(DISTINCT cint4), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;
SELECT count(DISTINCT cint8), sum(DISTINCT cint8), avg(DISTINCT cint8), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;
SELECT count(DISTINCT cint2), sum(DISTINCT cint2), avg(DISTINCT cint2), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;
SELECT count(DISTINCT ctext), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;


-- *** plain agg ***

-- single plain agg
SET optimizer = on;

-- normal argument
EXPLAIN (costs off)
SELECT count(cint4), sum(cint4), avg(cint4), min(cint4), max(cint4),
       count(cint8), sum(cint8), avg(cint8), min(cint8), max(cint8),
       count(cint2), sum(cint2), avg(cint2), min(cint2), max(cint2),
       count(cdate), min(cdate), max(cdate),
       count(cts)  , min(cts)  , max(cts)  ,
       count(ctext), min(ctext), max(ctext),
       count()
FROM tagg;

SELECT count(cint4), sum(cint4), avg(cint4), min(cint4), max(cint4),
       count(cint8), sum(cint8), avg(cint8), min(cint8), max(cint8),
       count(cint2), sum(cint2), avg(cint2), min(cint2), max(cint2),
       count(cdate), min(cdate), max(cdate),
       count(cts)  , min(cts)  , max(cts)  ,
       count(ctext), min(ctext), max(ctext),
       count()
FROM tagg;


-- distinct argument
EXPLAIN (costs off)
SELECT count(DISTINCT cint4), sum(DISTINCT cint4), avg(DISTINCT cint4) FROM tagg;

EXPLAIN (costs off)
SELECT count(DISTINCT cint8), sum(DISTINCT cint8), avg(DISTINCT cint8) FROM tagg;

EXPLAIN (costs off)
SELECT count(DISTINCT ctext) FROM tagg;

SELECT count(DISTINCT cint4), sum(DISTINCT cint4), avg(DISTINCT cint4) FROM tagg;
SELECT count(DISTINCT cint8), sum(DISTINCT cint8), avg(DISTINCT cint8) FROM tagg;
SELECT count(DISTINCT cint2), sum(DISTINCT cint2), avg(DISTINCT cint2) FROM tagg;
SELECT count(DISTINCT ctext) FROM tagg;


-- double plain aggs
SET optimizer = off;

-- normal argument
EXPLAIN (costs off)
SELECT count(cint4), sum(cint4), avg(cint4), min(cint4), max(cint4),
       count(cint8), sum(cint8), avg(cint8), min(cint8), max(cint8),
       count(cint2), sum(cint2), avg(cint2), min(cint2), max(cint2),
       count(cdate), min(cdate), max(cdate),
       count(cts)  , min(cts)  , max(cts)  ,
       count(ctext), min(ctext), max(ctext),
       count()
FROM tagg;

SELECT count(cint4), sum(cint4), avg(cint4), min(cint4), max(cint4),
       count(cint8), sum(cint8), avg(cint8), min(cint8), max(cint8),
       count(cint2), sum(cint2), avg(cint2), min(cint2), max(cint2),
       count(cdate), min(cdate), max(cdate),
       count(cts)  , min(cts)  , max(cts)  ,
       count(ctext), min(ctext), max(ctext),
       count()
FROM tagg;


-- distinct argument
EXPLAIN (costs off)
SELECT count(DISTINCT cint4), sum(DISTINCT cint4), avg(DISTINCT cint4) FROM tagg;

EXPLAIN (costs off)
SELECT count(DISTINCT cint8), sum(DISTINCT cint8), avg(DISTINCT cint8) FROM tagg;

EXPLAIN (costs off)
SELECT count(DISTINCT ctext) FROM tagg;

SELECT count(DISTINCT cint4), sum(DISTINCT cint4), avg(DISTINCT cint4) FROM tagg;
SELECT count(DISTINCT cint8), sum(DISTINCT cint8), avg(DISTINCT cint8) FROM tagg;
SELECT count(DISTINCT cint2), sum(DISTINCT cint2), avg(DISTINCT cint2) FROM tagg;
SELECT count(DISTINCT ctext) FROM tagg;


-- *** group agg ***
ANALYZE tagg;

-- normal argument
set optimizer = on;
EXPLAIN (costs off)
SELECT avg(cint8), min(cint8), max(cint8), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;

SELECT avg(cint8), min(cint8), max(cint8), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;

set optimizer = off;
set enable_hashagg = off;
EXPLAIN (costs off)
SELECT count(cint4), sum(cint4), avg(cint4), min(cint4), max(cint4),
       count(cint8), sum(cint8), avg(cint8), min(cint8), max(cint8),
       count(cint2), sum(cint2), avg(cint2), min(cint2), max(cint2),
       count(cdate), min(cdate), max(cdate),
       count(cts)  , min(cts)  , max(cts)  ,
       count(ctext), min(ctext), max(ctext),
       count(),
       grpkey
FROM tagg GROUP BY grpkey ORDER BY grpkey;

SELECT count(cint4), sum(cint4), avg(cint4), min(cint4), max(cint4),
       count(cint8), sum(cint8), avg(cint8), min(cint8), max(cint8),
       count(cint2), sum(cint2), avg(cint2), min(cint2), max(cint2),
       count(cdate), min(cdate), max(cdate),
       count(cts)  , min(cts)  , max(cts)  ,
       count(ctext), min(ctext), max(ctext),
       count(),
       grpkey
FROM tagg GROUP BY grpkey ORDER BY grpkey;

-- disinct argument
set optimizer = on;

EXPLAIN (costs off)
SELECT count(DISTINCT cint4), sum(DISTINCT cint4), avg(DISTINCT cint4), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;

EXPLAIN (costs off)
SELECT count(DISTINCT cint8), sum(DISTINCT cint8), avg(DISTINCT cint8), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;

EXPLAIN (costs off)
SELECT count(DISTINCT ctext), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;

SELECT count(DISTINCT cint4), sum(DISTINCT cint4), avg(DISTINCT cint4), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;
SELECT count(DISTINCT cint8), sum(DISTINCT cint8), avg(DISTINCT cint8), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;
SELECT count(DISTINCT cint2), sum(DISTINCT cint2), avg(DISTINCT cint2), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;
SELECT count(DISTINCT ctext), grpkey FROM tagg GROUP BY grpkey ORDER BY grpkey;
