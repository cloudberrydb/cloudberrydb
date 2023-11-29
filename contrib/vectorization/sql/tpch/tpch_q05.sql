set extra_float_digits = -1;;;
set default_table_access_method=ao_column;
set vector.enable_vectorization = on;

ANALYZE;
explain (costs off) select
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
	and r_name = 'ASIA'
	and o_orderdate >= date '1993-01-01'
	and o_orderdate < date '1993-01-01' + interval '1 year'
group by
	n_name
order by
	revenue desc;

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
	and r_name = 'ASIA'
	and o_orderdate >= date '1993-01-01'
	and o_orderdate < date '1993-01-01' + interval '1 year'
group by
	n_name
order by
	revenue desc;
