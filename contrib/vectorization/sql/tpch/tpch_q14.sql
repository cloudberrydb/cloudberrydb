set extra_float_digits = -1;
set default_table_access_method=ao_column;
set vector.enable_vectorization = on;

ANALYZE;
explain (costs off)  select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1997-08-01'
	and l_shipdate < date '1997-08-01' + interval '1 month';


select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1997-08-01'
	and l_shipdate < date '1997-08-01' + interval '1 month';
