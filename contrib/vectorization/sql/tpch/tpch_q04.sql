set extra_float_digits = -1;;
set default_table_access_method=ao_column;
set vector.enable_vectorization = on;

ANALYZE;
explain (costs off) select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '1996-07-01'
        and o_orderdate < date '1996-07-01' + interval '3 month'
        and exists (
                select
                        *
                from
                        lineitem
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
        )
group by
        o_orderpriority
order by
        o_orderpriority;


select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '1996-07-01'
        and o_orderdate < date '1996-07-01' + interval '3 month'
        and exists (
                select
                        *
                from
                        lineitem
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
        )
group by
        o_orderpriority
order by
        o_orderpriority;
