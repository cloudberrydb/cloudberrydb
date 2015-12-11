-- targeted dispatch for deepslice query this doesnt work today will have to update o/p to ans once feature is made avaialbe. MPP-7622
-- removing Explain from select queries due to MPP-8099
--set test_print_direct_dispatch_info=on;
select o_orderpriority, count(*) as order_count  from orders where  o_orderdate = date '1994-05-01'  and o_orderkey = 1500130 and exists (select * from lineitem where l_orderkey=o_orderkey and l_commitdate=l_receiptdate and l_orderkey=1500130) group by   o_orderpriority order by  o_orderpriority; 



