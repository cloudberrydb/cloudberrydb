select  'tpch20',
	s_name,
	s_address 
from 
	supplier, 
	nation 
where 
	s_suppkey in( 
		select 
			ps_suppkey 
		from 
			partsupp, 
			( 
				select 
					sum(l_quantity) as qty_sum, l_partkey, l_suppkey 
				from 
					lineitem 
				where 
					l_shipdate >= date '1996-01-01' 
					and l_shipdate < date '1996-01-01' + interval '1 year' 
				group by l_partkey, l_suppkey ) g 
		where 
			g.l_partkey = ps_partkey 
			and g.l_suppkey = ps_suppkey 
			and ps_availqty > 0.5 * g.qty_sum 
			and ps_partkey in ( select p_partkey from part where p_name like 'medium%' ) 
		) 
	and s_nationkey = n_nationkey 
	and n_name = 'IRAQ'
order by 
	s_name;

