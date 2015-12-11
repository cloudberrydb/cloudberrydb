select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and r_regionkey=1994
        and o_orderdate = date '1997-01-01'
        and o_orderkey= 3014593
        and n_nationkey =3
        and l_orderkey= 3014593
        and s_suppkey=3
group by
        n_name
order by
        revenue desc;