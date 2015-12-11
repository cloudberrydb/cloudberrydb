-- distinct
select count(*) from
(
select distinct l_partkey, l_suppkey, l_shipmode from lineitem 
where l_orderkey % (select max(nscaleperseg) from opperfscale) = 0 
) tmpt;

