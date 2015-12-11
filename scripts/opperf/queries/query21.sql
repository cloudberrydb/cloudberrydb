-- Full outer join, merge join
-- set enable_hashjoin = off;

-- explain 
select count(*) from lineitem l full outer join partsupp p
on l.l_partkey = p.ps_partkey and l.l_suppkey = p.ps_suppkey
-- and l.l_quantity > (p.ps_availqty / 10)
;

-- set enable_hashjoin = on;
