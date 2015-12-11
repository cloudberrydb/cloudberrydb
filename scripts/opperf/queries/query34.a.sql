select 'q1',count(distinct l_partkey), count(distinct l_suppkey), count(distinct l_shipmode)  from pg_sleep(10), lineitem  where l_orderkey % (select max(nscale * 4) from opperfscale where nseg=1) = 0;
--explain  select 'q1',count(distinct l_partkey), count(distinct l_suppkey), count(distinct l_shipmode)  from pg_sleep(10), lineitem  where l_orderkey % (select max(nscale * 4) from opperfscale where nseg=1) = 0;

