SET default_table_access_method=ao_column;

CREATE TABLE test_agg_stddev(
	i int,
	x int);

INSERT INTO test_agg_stddev(i, x)
SELECT n, n
FROM generate_series(1, 20) AS s(n);

ANALYZE test_agg_stddev;

explain SELECT stddev_samp(i) FROM test_agg_stddev;
explain SELECT stddev_samp(a.i) FROM test_agg_stddev a , test_agg_stddev b WHERE a.i = b.i ;
explain SELECT stddev_samp(a.i) FROM test_agg_stddev a , test_agg_stddev b WHERE a.i = b.i GROUP BY a.x;

SELECT stddev_samp(i) FROM test_agg_stddev;
SELECT stddev_samp(a.i) FROM test_agg_stddev a , test_agg_stddev b WHERE a.i = b.i ;

INSERT INTO test_agg_stddev(i, x)
SELECT n + 15, n
FROM generate_series(1, 20) AS s(n)
UNION ALL
SELECT n + 15, n
FROM generate_series(1, 20) AS s(n)
UNION ALL
SELECT n + 15, n
FROM generate_series(1, 20) AS s(n)
UNION ALL
SELECT n + 15, n
FROM generate_series(1, 20) AS s(n);

SELECT stddev_samp(a.i) AS stddev FROM test_agg_stddev a , test_agg_stddev b WHERE a.i = b.i GROUP BY a.x ORDER BY stddev;
