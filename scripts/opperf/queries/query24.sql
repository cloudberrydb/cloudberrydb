-- distinct using group by
select count(*) from 
(select count(1) from lineitem
 where l_orderkey % (select max(nscaleperseg) from opperfscale) = 0
 group by l_partkey, l_suppkey, l_shipmode
) tmpt;

