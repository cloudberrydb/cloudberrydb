set extra_float_digits = -1;;;
set default_table_access_method=ao_column;
set vector.enable_vectorization = on;

ANALYZE;
explain (costs off) select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
		select
				sum(ps_supplycost * ps_availqty) * 0.0000020000
		from
				partsupp,
				supplier,
				nation
		where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'GERMANY'
    )
order by
    value desc;


select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
		select
				sum(ps_supplycost * ps_availqty) * 0.0000020000
		from
				partsupp,
				supplier,
				nation
		where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'GERMANY'
    )
order by
    value desc;
