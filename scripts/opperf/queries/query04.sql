-- Sort test 2:
select count(*) from
(
select l_linenumber, l_shipdate, l_linestatus  from lineitem
where l_orderkey % (
	select max(nscaleperseg) from opperfscale
	) = 0 
order by l_linenumber, l_shipdate, l_linestatus
limit 1000000
) tmpt;

