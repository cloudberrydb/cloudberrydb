-- @author ramans2
-- @created 2014-03-25 12:00:00
-- @modified 2014-03-25 12:00:00
-- @gucs gp_dump_memory_usage=on
-- @description Single query to test GUC gp_dump_memory_usage

show gp_vmem_protect_limit;

select count(*) from 
    (select o0.o_orderkey from 
    (orders o0 
        left outer join orders o1 on o0.o_orderkey = o1.o_orderkey 
	left outer join orders o2 on o1.o_orderkey = o2.o_orderkey 
	left outer join orders o3 on o2.o_orderkey = o3.o_orderkey)
    order by o0.o_orderkey) as foo;
