select 'tpch16',
	p_brand, 
	p_type, 
	p_size,
	count(*) as supplier_cnt 
from 
(
select 
	p_brand, 
	p_type, 
	p_size, 
	ps_suppkey 
from 
	part, 
	partsupp left join supplier on (ps_suppkey=s_suppkey and s_comment like '%Customer%Complaints%' ) 
where 
	p_partkey = ps_partkey 
	and p_brand <> 'Brand#35' 
	and p_type not like 'MEDIUM ANODIZED%' 
--	and p_size in (39, 31, 24, 22, 46, 20, 42, 15)
	and (p_size = 39 or p_size = 31 or p_size = 24 or p_size = 22 or p_size = 46 or p_size = 20 or p_size = 42 or p_size = 15)
	and s_suppkey is null 
group by 
	p_brand, 
	p_type, 
	p_size,
	ps_suppkey
) foo

group by 
	p_brand, 
	p_type, 
	p_size 
order by 
	supplier_cnt desc, 
	p_brand, 
	p_type, 
	p_size;
