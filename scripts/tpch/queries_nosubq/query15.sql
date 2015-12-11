SELECT 'tpch15',
       s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM supplier,
     revenue0,
  (SELECT max(total_revenue) AS max_total
   FROM revenue0) AS maxrev0
WHERE s_suppkey = supplier_no
  AND total_revenue = maxrev0.max_total
ORDER BY s_suppkey;

