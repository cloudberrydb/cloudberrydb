-- Nested loop join
-- explain
select count(*) from part p1, part p2
where 
p1.p_partkey % (select max(nscale * 10) from opperfscale) = 0 and 
p2.p_partkey % (select max(nscale * 10) from opperfscale) = 0 and 
p1.p_size < p2.p_size 
and p1.p_retailprice > p2.p_retailprice
and p1.p_brand > p2.p_brand
;

