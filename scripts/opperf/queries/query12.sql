-- Hash join

-- explain
select count(*) from lineitem l1, lineitem l2
where 
l1.l_orderkey % (select max(nscaleperseg) from opperfscale) = 0 and
l2.l_orderkey % (select max(nscaleperseg) from opperfscale) = 0 and
l1.l_partkey = l2.l_partkey and l1.l_returnflag = l2.l_returnflag
;

