--start_ignore
-- @author tungs1
-- @modified 2013-07-17 12:00:00
-- @created 2013-07-17 12:00:00
-- @description SplitDQA lineitem2_SK10_1_SUM.sql
-- @db_name splitdqa
-- @tags SplitAgg HAWQ
-- @executemode normal
--end_ignore
create table lineitem1 as (select * from lineitem ORDER BY l_orderkey, l_partkey, l_suppkey,l_linenumber,l_returnflag,l_linestatus,l_receiptdate, l_shipinstruct limit 500) distributed by (l_orderkey);
create table part1 as (select * from part, partsupp where p_partkey = ps_partkey ORDER BY p_partkey,ps_partkey, ps_availqty, p_size , ps_suppkey, ps_supplycost limit 500) distributed by (p_partkey);
create table part2 as (select * from part, partsupp where p_partkey = ps_partkey ORDER BY p_partkey,ps_partkey, ps_availqty, p_size , ps_suppkey, ps_supplycost limit 500) distributed by (p_partkey, ps_suppkey);
create table lineitem2 as (select * from lineitem ORDER BY l_orderkey, l_partkey, l_suppkey,l_linenumber,l_returnflag,l_linestatus,l_receiptdate, l_shipinstruct limit 500) distributed by (l_orderkey, l_partkey);
