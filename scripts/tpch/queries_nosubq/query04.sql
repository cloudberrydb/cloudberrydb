SELECT 'tpch4',
       o_orderpriority,
       count(*) AS order_count
FROM orders
INNER JOIN
  (SELECT l_orderkey
   FROM lineitem
   WHERE l_commitdate < l_receiptdate
   GROUP BY l_orderkey) l ON (o_orderkey = l.l_orderkey)
WHERE o_orderdate >= date '1994-05-01'
  AND o_orderdate < date '1994-05-01' + interval '3 month'
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
