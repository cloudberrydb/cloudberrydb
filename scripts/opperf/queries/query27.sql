-- Union all

select count(*) from
(
  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg * 10) from opperfscale) = 1
  union all
  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg * 10) from opperfscale) = 2
  union all
  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg * 10) from opperfscale) = 3
  union all
  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg * 10) from opperfscale) = 4
  union all
  select l_suppkey, l_partkey, l_shipmode from lineitem where
  l_orderkey % (select max(nscaleperseg * 10) from opperfscale) = 5
) tmpt;
