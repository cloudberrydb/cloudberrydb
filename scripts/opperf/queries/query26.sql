-- Union

select count(*) from
(
  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg) * 20 from opperfscale) = 1
  union

  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg) * 20 from opperfscale) = 2
  union

  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg) * 20 from opperfscale) = 3
  union

  select l_suppkey, l_partkey, l_shipmode from lineitem  where
  l_orderkey % (select max(nscaleperseg) * 20 from opperfscale) = 4
  union

  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg) * 20 from opperfscale) = 5
) tmpt;
