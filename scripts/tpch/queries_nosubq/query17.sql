select 'tpch17',
      sum(l_extendedprice) / 7.0 as avg_yearly
from
      lineitem,
      (
          select
              p_partkey as x_partkey,
              0.2 * avg(l_quantity) as x_avg_20
          from
              part,
              lineitem
          where
              p_partkey = l_partkey
              and p_brand = 'Brand#32'
              and p_container = 'JUMBO PKG'
          group by
              p_partkey
      ) x
where
      x_partkey = l_partkey
and l_quantity < x_avg_20;
