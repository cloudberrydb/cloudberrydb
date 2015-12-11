select  'tpch21',
	s_name, 
	count(distinct(l1.l_orderkey||l1.l_linenumber)) as numwait 
from 
	supplier, 
	orders, 
	nation, 
	lineitem l1 
		left join lineitem l2 
			on (l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) 
		left join (
			select 
				l3.l_orderkey,
				l3.l_suppkey 
			from 
				lineitem l3 
			where 
				l3.l_receiptdate > l3.l_commitdate) l4 
			on (l4.l_orderkey = l1.l_orderkey and l4.l_suppkey <> l1.l_suppkey) 
where 
	s_suppkey = l1.l_suppkey 
	and o_orderkey = l1.l_orderkey 
	and o_orderstatus = 'F' 
	and l1.l_receiptdate > l1.l_commitdate 
	and l2.l_orderkey is not null 
	and l4.l_orderkey is null 
	and s_nationkey = n_nationkey 
	and n_name = 'MOZAMBIQUE' 
group by 
	s_name 
order by 
	numwait desc, 
	s_name
LIMIT 100;
