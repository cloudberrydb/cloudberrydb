-- Sort agg 2: easy
select count(*) from (
select avg(l_quantity), max(l_discount) 
from lineitem
where l_orderkey % (select max(nscaleperseg) from opperfscale) = 0
group by 
l_linenumber, l_linestatus, l_returnflag
) tmpt;
