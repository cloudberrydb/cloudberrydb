select count(*), sum(n_nationkey), min(n_nationkey), max(n_nationkey) from nation;
select count(*), sum(r_regionkey), min(r_regionkey), max(r_regionkey) from region;
select count(*), sum(p_partkey), min(p_partkey), max(p_partkey) from part;
select count(*), sum(s_suppkey), min(s_suppkey), max(s_suppkey) from supplier;
select count(*), sum(ps_partkey + ps_suppkey), min(ps_partkey + ps_suppkey), max(ps_partkey + ps_suppkey) from partsupp;
select count(*), sum(c_custkey), min(c_custkey), max(c_custkey) from customer;
select count(*), sum(o_orderkey), min(o_orderkey), max(o_orderkey) from orders;
select count(*), sum(l_linenumber), min(l_linenumber), max(l_linenumber) from lineitem;

\echo -- start_ignore
drop table if exists opperfscale;
create table opperfscale(nseg int, nscale int, nscaleperseg int); 

insert into opperfscale
 	select case when lc < 7000000 then 1 else seg end as nseg, 
	       case when lc < 7000000 then 1 else lc / 6000000 end as nscale,
	       case when lc < 7000000 or lc / 6000000 < seg 
	       		then 2 
			else lc / 6000000 / seg + 1 end as nscaleperseg
	from
	(
 		select max(content)+1 as seg from gp_configuration where isprimary = 't'
	) S,
	(
	 select count(*) as lc from lineitem 
	) L;

analyze opperfscale;
\echo -- end_ignore

select * from opperfscale;
