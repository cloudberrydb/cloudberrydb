SELECT 'tpch11',
       pv.ps_partkey,
       pv.value
FROM
  ( SELECT ps_partkey,
           sum(ps_supplycost * ps_availqty) AS value
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'ROMANIA'
   GROUP BY ps_partkey ) pv,
  ( SELECT sum(ps_supplycost * ps_availqty) * 0.0001000000 AS value
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'ROMANIA' ) r
WHERE pv.value > r.value
ORDER BY value DESC;
