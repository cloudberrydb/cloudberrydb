-- nested loop outer join
-- explain analyze
select count(*) from part p left outer join supplier s 
on p.p_partkey > s.s_suppkey and p.p_size < s.s_nationkey
where
p.p_partkey % (select max(nscale * 4) from opperfscale) = 1
and s.s_suppkey % (select max(nscale * 4) from opperfscale) = 1
;
