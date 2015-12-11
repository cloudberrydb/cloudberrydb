-- Hashagg 2: easy
select count(*) from
(
select avg(l_quantity), max(l_discount) 
from lineitem
group by 
l_linenumber, l_linestatus, l_returnflag
) tmpt;
