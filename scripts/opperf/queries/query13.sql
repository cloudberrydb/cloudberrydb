-- Hash join, lots of fields
-- explain 
select count(*) from lineitem l1, lineitem l2
where 
l1.l_orderkey % (
		select max(nscaleperseg) from opperfscale 
		) = 0 and
l2.l_orderkey % (
		select max(nscaleperseg) from opperfscale 
		) = 0 and
-- l1.l_orderkey = l2.l_orderkey and 
l1.l_partkey = l2.l_partkey
and l1.l_suppkey = l2.l_suppkey
and l1.l_linenumber = l2.l_linenumber
and l1.l_extendedprice = l2.l_extendedprice
and l1.l_returnflag = l2.l_returnflag
and l1.l_shipdate = l2.l_shipdate
and l1.l_commitdate = l2.l_commitdate
and l1.l_shipmode = l2.l_shipmode
and l1.l_comment = l2.l_comment
;

