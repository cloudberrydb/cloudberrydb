SELECT 'tpch18',
       c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
FROM customer, orders, lineitem l,
  (
  select ll1.l_orderkey
  from
  ( SELECT l_orderkey, sum(l_quantity) as q
   FROM lineitem
   GROUP BY l_orderkey) ll1
   WHERE ll1.q > 313) ll
WHERE o_orderkey = ll.l_orderkey
  AND c_custkey = o_custkey
  AND o_orderkey = l.l_orderkey
GROUP BY c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;
