-- @author ramans2
-- @created 2014-02-05 12:00:00
-- @modified 2014-02-05 12:00:00
-- @description Single query that runs OOM

show gp_vmem_protect_limit;

select 2 as oom_test;
select count(*) from 
    (select o0.o_orderkey from 
    (orders o0 
        left outer join orders o1 on o0.o_orderkey = o1.o_orderkey 
	left outer join orders o2 on o1.o_orderkey = o2.o_orderkey 
	left outer join orders o3 on o2.o_orderkey = o3.o_orderkey 
	left outer join orders o4 on o3.o_orderkey = o4.o_orderkey 
	left outer join orders o5 on o4.o_orderkey = o5.o_orderkey 
	left outer join orders o6 on o5.o_orderkey = o6.o_orderkey 
	left outer join orders o7 on o6.o_orderkey = o7.o_orderkey 
	left outer join orders o8 on o7.o_orderkey = o8.o_orderkey 
	left outer join orders o9 on o8.o_orderkey = o9.o_orderkey 
	left outer join orders o10 on o9.o_orderkey = o10.o_orderkey 
	left outer join orders o11 on o10.o_orderkey = o11.o_orderkey 
	left outer join orders o12 on o11.o_orderkey = o12.o_orderkey 
	left outer join orders o13 on o12.o_orderkey = o13.o_orderkey 
	left outer join orders o14 on o13.o_orderkey = o14.o_orderkey 
	left outer join orders o15 on o14.o_orderkey = o15.o_orderkey) 
    order by o0.o_orderkey) as foo;
