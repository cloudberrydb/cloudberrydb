-- Full outer join, with hash join
-- explain 
-- select count(*) from lineitem l full outer join partsupp p
-- on l.l_partkey = p.ps_partkey and l.l_suppkey = p.ps_suppkey
-- ;
select 'Not supported';
