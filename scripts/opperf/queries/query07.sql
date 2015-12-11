-- Hashagg 1: yahoo

select count(*) from (
select avg(l_quantity) as c1, max(l_discount) as c2 
from lineitem
group by 
l_orderkey, l_linenumber, l_linestatus, l_comment
) tmpt;

