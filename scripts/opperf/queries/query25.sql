-- count distinct
select count(distinct l_partkey), count(distinct l_suppkey), count(distinct l_shipmode) 
	from lineitem 
where l_orderkey % (select max(nscale * 4) from opperfscale) = 0;
