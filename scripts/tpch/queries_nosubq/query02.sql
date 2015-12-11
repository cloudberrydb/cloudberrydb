select  'tpch2',
	s.s_acctbal,
	s.s_name,
	n.n_name,
	p.p_partkey,
	p.p_mfgr,
	s.s_address,
	s.s_phone,
	s.s_comment 
from 
	supplier s, 
	partsupp ps, 
	nation n, 
	region r, 
	part p, 
	(select p_partkey, min(ps_supplycost) as min_ps_cost 
		from 
			part, 
			partsupp , 	
			supplier,
			nation, 
			region 
		where 
			p_partkey=ps_partkey 
			and s_suppkey = ps_suppkey 
			and s_nationkey = n_nationkey 
			and n_regionkey = r_regionkey 
			and r_name = 'EUROPE' 
		group by p_partkey ) g 
where 
	p.p_partkey = ps.ps_partkey 
	and g.p_partkey = p.p_partkey 
	and g. min_ps_cost = ps.ps_supplycost 
	and s.s_suppkey = ps.ps_suppkey 
	and p.p_size = 45 
	and p.p_type like '%NICKEL' 
	and s.s_nationkey = n.n_nationkey 
	and n.n_regionkey = r.r_regionkey 
	and r.r_name = 'EUROPE' 
order by 
	s.s_acctbal desc,
	n.n_name,
	s.s_name,
	p.p_partkey
LIMIT 100;
