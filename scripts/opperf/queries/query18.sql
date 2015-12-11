-- Full outer join, merge join

-- explain 
select count(*) from lineitem l left outer join partsupp p
on l.l_partkey = p.ps_partkey and l.l_suppkey = p.ps_suppkey
and l.l_quantity > (p.ps_availqty / 10)
where l.l_orderkey % (select max(nscaleperseg) from opperfscale) = 0
;

