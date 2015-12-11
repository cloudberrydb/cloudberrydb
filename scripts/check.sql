--
-- simple tests
--

set search_path to public, gpoptutils;
set client_min_messages=log;

drop table foo;
create table foo();
alter table foo add column x int;
insert into foo select generate_series(1,11);

drop table bar;
create table bar(x int);
insert into bar select generate_series(1,11);

select DumpPlanToFile('select x from foo where x > -1 and x < 2', '/tmp/foo.plan');
select RestorePlanFromFile('/tmp/foo.plan');


--
-- xml master only tables
--

drop table r;
create table r();
alter table r add column a int;
alter table r add column b int;

insert into r select generate_series(1,1000);

drop table s;
create table s();
alter table s add column c int;
alter table s add column d int;

insert into s select generate_series(1,1000);

select RestorePlanDXL(DumpPlanDXL('select * from r'));
--select RestorePlanDXL(DumpPlanDXL('select * from r,s where r.a=s.c'));
select RestorePlanDXL(DumpPlanDXL('select * from r,s where r.a<s.c+1 or r.a>s.c'));
select RestorePlanDXL(DumpPlanDXL('select sum(r.a) from r'));
select RestorePlanDXL(DumpPlanDXL('select count(*) from r'));
select RestorePlanDXL(DumpPlanDXL('select a, b from r,s group by a,b'));
select RestorePlanDXL(DumpPlanDXL('select r.a+1 from r'));
select RestorePlanDXL(DumpPlanDXL('select * from r,s where r.a<s.c or (r.b<s.d and r.b>s.c)'));
select RestorePlanDXL(DumpPlanDXL('select case when r.a<s.c then r.a<s.c else r.a<s.c end from r,s'));
--select RestorePlanDXL(DumpPlanDXL('select case r.b<s.c when true then r.b else s.c end from r,s where r.a = s.d'));
select RestorePlanDXL(DumpPlanDXL('select * from r limit 100'));
select RestorePlanDXL(DumpPlanDXL('select * from r limit 10 offset 991 '));
select RestorePlanDXL(DumpPlanDXL('select * from r offset 100'));
select RestorePlanDXL(DumpPlanDXL('select r.a * random() from r'));
select RestorePlanDXL(DumpPlanDXL('select pow(r.b,r.a) from r'));

select RestoreQueryDXL(DumpQueryDXL('select a from r group by a having  count(*) > 10'));
select RestoreQueryDXL(DumpQueryDXL('select a from r group by a having  count(*) > max(b) + (select count(*) from s where s.c = r.a)'));

select DumpQueryDXL('select sum(b) from r group by a having count(*) > 10 order by a+1');
select DumpQueryDXL('select a, sum(b) from r group by a having count(*) > 0 limit 10;');
select RestoreQueryDXL(DumpQueryDXL('select sum(b) from r group by a having count(*) > 10 order by a+1'));

--
-- constants
--
select DumpPlanDXL('select 0.001::numeric from r');
select DumpPlanDXL('select NULL::text, NULL::int from r');
select DumpPlanDXL('select \'helloworld\'::text, \'helloworld2\'::varchar from r');
select DumpPlanDXL('select 129::bigint, 5623::int, 45::smallint from r');

select RestorePlanDXL(DumpPlanDXL('select 0.001::numeric from r'));
select RestorePlanDXL(DumpPlanDXL('select NULL::text, NULL::int from r'));
select RestorePlanDXL(DumpPlanDXL('select \'helloworld\'::text, \'helloworld2\'::varchar from r'));
select RestorePlanDXL(DumpPlanDXL('select 129::bigint, 5623::int, 45::smallint from r'));

--
-- xml distributed tables
--

-- result node
drop table foo;
drop table bar1;
drop table bar2;

create table foo (x1 int, x2 int, x3 int);
create table bar1 (x1 int, x2 int, x3 int) distributed randomly;
create table bar2 (x1 int, x2 int, x3 int) distributed randomly;

insert into foo select i,i+1,i+2 from generate_series(1,10) i;

insert into bar1 select i,i+1,i+2 from generate_series(1,1000) i;
insert into bar2 select i,i+1,i+2 from generate_series(1,1000) i;

select RestorePlanDXL(DumpPlanDXL('select x2 from foo where x1 in (select x2 from bar1)')); -- produces result node

select DumpPlanDXL('select 1');

select RestorePlanDXL(DumpPlanDXL('select 1'));


drop table r cascade;
create table r(a int, b int);
create unique index r_a on r(a);
create index r_b on r(b);

insert into r select generate_series(1,1000) %20, generate_series(1,1000) %10;

drop table s;
create table s(c int, d int);

insert into s select generate_series(1,1000);

analyze r;
analyze s;

select RestorePlanDXL(DumpPlanDXL('select * from r,s where r.a=s.c'));

-- Materialize node
select RestorePlanDXL(DumpPlanDXL('select * from r,s where r.a<s.c+1 or r.a>s.c'));

-- empty target list
select RestorePlanDXL(DumpPlanDXL('select r.* from r,s where s.c=5'));

drop table m;
create table m();
alter table m add column a int;
alter table m add column b int;

drop table m1;
create table m1();
alter table m1 add column a int;
alter table m1 add column b int;

-- join types
select RestorePlanDXL(DumpPlanDXL('select r.a, s.c from r left outer join s on(r.a=s.c)'));
select RestorePlanDXL(DumpPlanDXL('select r.a, s.c from r right outer join s on(r.a=s.c)'));
select RestorePlanDXL(DumpPlanDXL('select * from r where exists (select * from s where s.c=r.a + 5)'));
select RestorePlanDXL(DumpPlanDXL('select * from r where exists (select * from s where s.c=r.b)'));
select RestorePlanDXL(DumpPlanDXL('select * from m where m.a not in (select a from m1 where a=5)'));
select RestorePlanDXL(DumpPlanDXL('select * from m where m.a not in (select a from m1)'));
select RestorePlanDXL(DumpPlanDXL('select * from m where m.a in (select a from m1 where m1.a+5 = m.b)'));

-- enable_hashjoin=off; enable_mergejoin=on
select RestorePlanDXL(DumpPlanDXL('select 1 from m,m1 where m.a = m1.a and m.b!=m1.b'));

-- plan.qual vs hashclauses/join quals:
select RestorePlanDXL(DumpPlanDXL('select * from r left outer join s on (r.a=s.c and r.b<s.d) where s.d is null'));
select RestorePlanDXL(DumpPlanDXL('select * from r m full outer join r m1 on (m.a=m1.a) where m.a is null'));

-- sort
select RestorePlanDXL(DumpPlanDXL('select a from r order by b'));
select RestorePlanDXL(DumpPlanDXL('select * from r join s on(r.a=s.c) order by r.a, s.d'));
select RestorePlanDXL(DumpPlanDXL('select * from r join s on(r.a=s.c) order by r.a, s.d limit 10'));
select RestorePlanDXL(DumpPlanDXL('select * from r join s on(r.a=s.c) order by r.a + 5, s.d limit 10'));

-- group by
select RestorePlanDXL(DumpPlanDXL('select 1 from m group by a+b'));

-- const table get

select DumpQueryDXL('select * from r where a = (select 1)');
select Optimize('select * from r where a = (select 1)');
select RestorePlanDXL(Optimize('select * from r where a = (select 1)'));

-- relcache dump
select DumpMDObjDXL('r'::regclass);
select DumpMDObjDXL('r_a'::regclass);
select DumpMDObjDXL('r_b'::regclass);
select DumpMDObjDXL(23);
select DumpMDObjDXL(25);
select DumpMDObjDXL(701);
select DumpMDObjDXL(2101);
select DumpRelStatsDXL('r'::regclass);

insert into m select generate_series(1,10)%2, generate_series(1,10)%3;

-- optimize query
select Optimize('select * from m where a=b');
select RestorePlanDXL(Optimize('select * from m where a=b'));
select RestorePlanDXL(Optimize('select * from m where a=1'));

insert into m values (1,-1), (1,2), (1,1);

-- computed columns
select RestorePlanDXL(Optimize('select a,a,a+b from m'));
select RestorePlanDXL(Optimize('select a,a+b,a+b from m'));

-- func expr
select Optimize('select * from m where a=abs(b)');
select RestorePlanDXL(Optimize('select * from m where a=abs(b)'));

-- group by
select Optimize('select count(*) from m where a =5');
select RestorePlanDXL(Optimize('select count(*) from m where a =5'));

select RestorePlanDXL(Optimize('select count(*) from m group by b'));
select RestorePlanDXL(Optimize('select count(*), b from m group by b'));
select RestorePlanDXL(Optimize('select sum(a), b from m group by b'));

select Optimize('select sum(a), b from m group by b,a');
select RestorePlanDXL(Optimize('select sum(a), b from m group by b,a'));

select RestorePlanDXL(Optimize('select sum(a) from m'));

-- grouping sets
select DumpQueryDXL('select a,b,count(*) from m group by grouping sets ((a), (a,b))'); 
select DumpQueryDXL('select b,count(*) from m group by grouping sets ((a), (a,b))'); 
select DumpQueryDXL('select a,count(*) from m group by grouping sets ((a), (a,b))');
select Optimize('select a,b,count(*) from m group by grouping sets ((a), (a,b))'); 
select Optimize('select b,count(*) from m group by grouping sets ((a), (a,b))'); 
select Optimize('select a,count(*) from m group by grouping sets ((a), (a,b))');
select RestorePlanDXL(Optimize('select a,count(*) from m group by grouping sets ((a), (b))')); 
select RestorePlanDXL(Optimize('select a,b,count(*) from m group by rollup(a, b)'));
select RestorePlanDXL(Optimize('select count(*) from m group by ()'));
select RestorePlanDXL(Optimize('select a, count(*) from r group by (), a'));

select RestorePlanDXL(Optimize('select a from (select 1 as a) r(a) group by rollup(a)'));

select RestorePlanDXL(Optimize('select a, count(*) from r group by grouping sets ((),(a))'));

create table sale
(
cn int not null,
vn int not null,
pn int not null,
dt date not null,
qty int not null,
prc float not null,

primary key (cn, vn, pn)

) distributed by (cn,vn,pn);

select RestorePlanDXL(Optimize('select cn,vn,sum(qty) from sale group by grouping sets (rollup(cn,vn))'));

-- arrays
select RestorePlanDXL(DumpPlanDXL('select array[array[a,b]], array[b] from r'));

-- setops
select DumpQueryDXL('select a, b from m union select b,a from m');
select DumpQueryDXL('select a, b from m union select b,a from m union all select * from m');

select Optimize('select a, b from m union select b,a from m');
select RestorePlanDXL(Optimize('select a, b from m union select b,a from m'));
select Optimize('select a, b from m union select b,a from m union all select * from m');
select RestorePlanDXL(Optimize('select a, b from m union select b,a from m union all select * from m'));

drop table foo;
create table foo(a int, b int, c int, d int);

drop table bar;
create table bar(a int, b int, c int);

select DumpQueryDXL('SELECT a from foo UNION ALL select b from foo UNION ALL select a+b from foo group by 1');
select Optimize('SELECT a from foo UNION ALL select b from foo UNION ALL select a+b from foo group by 1');
select RestorePlanDXL(Optimize('SELECT a from foo UNION ALL select b from foo UNION ALL select a+b from foo group by 1'));
select DumpQueryDXL('SELECT a, a-1 from foo UNION ALL select b, b-1 from foo UNION ALL select a+b, a-b from foo order by 2, 1');
select Optimize('SELECT a, a-1 from foo UNION ALL select b, b-1 from foo UNION ALL select a+b, a-b from foo order by 2, 1');
select RestorePlanDXL(Optimize('SELECT a, a-1 from foo UNION ALL select b, b-1 from foo UNION ALL select a+b, a-b from foo order by 2, 1'));

--- distinct operation

select DumpQueryDXL('SELECT distinct a, b from foo');
select DumpQueryDXL('SELECT distinct a, count(*) from foo group by a');
select DumpQueryDXL('SELECT distinct foo.a, bar.b from foo, bar where foo.b = bar.a');
select DumpQueryDXL('SELECT distinct foo.a, bar.b, sum(bar.c+foo.c) from foo, bar where foo.b = bar.a group by foo.a, bar.b');

select Optimize('SELECT distinct a, b from foo');
select Optimize('SELECT distinct a, count(*) from foo group by a');
select Optimize('SELECT distinct foo.a, bar.b from foo, bar where foo.b = bar.a');
select Optimize('SELECT distinct foo.a, bar.b, sum(bar.c+foo.c) from foo, bar where foo.b = bar.a group by foo.a, bar.b');

select RestorePlanDXL(Optimize('SELECT distinct a, b from foo'));
select RestorePlanDXL(Optimize('SELECT distinct a, count(*) from foo group by a'));
select RestorePlanDXL(Optimize('SELECT distinct foo.a, bar.b from foo, bar where foo.b = bar.a'));
select RestorePlanDXL(Optimize('SELECT distinct foo.a, bar.b, sum(bar.c+foo.c) from foo, bar where foo.b = bar.a group by foo.a, bar.b'));

--- window operations

select gpoptutils.DumpQueryDXL('select row_number() over() +rank() over(partition by b order by a) from foo');
select gpoptutils.DumpQueryDXL('select rank() over(partition by b order by count(*)/sum(a)) from foo group by a, b');
select gpoptutils.DumpQueryDXL('select foo.a, bar.a, count(*)+1 from foo inner join bar using(b) group by foo.a, bar.a order by 1 limit 10');
select gpoptutils.DumpQueryDXL('select row_number() over(order by foo.a) from foo inner join bar using(b) group by foo.a, bar.b, bar.a');
select gpoptutils.DumpQueryDXL('select 1+row_number() over(order by foo.a+bar.a) from foo inner join bar using(b)');
select gpoptutils.DumpQueryDXL('select row_number() over(order by foo.a+ bar.a)/count(*) from foo inner join bar using(b) group by foo.a, bar.a, bar.b');
select gpoptutils.DumpQueryDXL('select row_number() over(), 1 + rank() over(order by foo.a+ bar.a)/count(*) from foo inner join bar using(b) group by foo.a, bar.a, bar.b order by 1');
select gpoptutils.DumpQueryDXL('select row_number() over(), 1 + rank() over(order by foo.a+ bar.a)/count(*) from foo inner join bar using(b) group by foo.a, bar.a, bar.b  having max(foo.c) > 10');
select gpoptutils.DumpQueryDXL('select count(*) over(partition by b order by a range between 1 preceding and (select count(*) from bar) following) from foo');
select gpoptutils.DumpQueryDXL('select a+1, rank() over(partition by b+1 order by a+1) from foo');

select gpoptutils.Optimize('select row_number() over() +rank() over(partition by b order by a) from foo');
select gpoptutils.Optimize('select rank() over(partition by b order by count(*)/sum(a)) from foo group by a, b');
select gpoptutils.Optimize('select foo.a, bar.a, count(*)+1 from foo inner join bar using(b) group by foo.a, bar.a order by 1 limit 10');
select gpoptutils.Optimize('select row_number() over(order by foo.a) from foo inner join bar using(b) group by foo.a, bar.b, bar.a');
select gpoptutils.Optimize('select 1+row_number() over(order by foo.a+bar.a) from foo inner join bar using(b)');
select gpoptutils.Optimize('select row_number() over(order by foo.a+ bar.a)/count(*) from foo inner join bar using(b) group by foo.a, bar.a, bar.b');
select gpoptutils.Optimize('select row_number() over(), 1 + rank() over(order by foo.a+ bar.a)/count(*) from foo inner join bar using(b) group by foo.a, bar.a, bar.b order by 1');
select gpoptutils.Optimize('select row_number() over(), 1 + rank() over(order by foo.a+ bar.a)/count(*) from foo inner join bar using(b) group by foo.a, bar.a, bar.b  having max(foo.c) > 10');
select gpoptutils.Optimize('select count(*) over(partition by b order by a range between 1 preceding and (select count(*) from bar) following) from foo');
select gpoptutils.Optimize('select a+1, rank() over(partition by b+1 order by a+1) from foo');

select gpoptutils.DumpPlanDXL(gpoptutils.Optimize('select tablename, partitiontablename, partitionboundary from pg_partitions'));

--- cte

drop table if exists smallt;
create table smallt (i int, t text, d date);
drop table if exists smallt2;
create table smallt2 (i int, t text, d date);

select gpoptutils.DumpQueryDXL('
with my_group_sum(d, total) as (select d, sum(i) from smallt group by d)
select smallt2.* from smallt2
where i < all (select total from my_group_sum, smallt, smallt2 as tmp where my_group_sum.d = smallt.d and smallt.d = tmp.d and my_group_sum.d = smallt2.d)
and i = 0 order by 1,2,3;;');

select gpoptutils.Optimize('
with my_group_sum(d, total) as (select d, sum(i) from smallt group by d)
select smallt2.* from smallt2
where i < all (select total from my_group_sum, smallt, smallt2 as tmp where my_group_sum.d = smallt.d and smallt.d = tmp.d and my_group_sum.d = smallt2.d)
and i = 0 order by 1,2,3;;');

select gpoptutils.DumpQueryDXL('
with x as (select a, b from r)
select rank() over(partition by a, case when b = 0 then a+b end 
 	order by b asc) as rank_within_parent
 from x
 order by
   a desc
  ,case when a+b = 0 then a end
  ,b');

select gpoptutils.DumpQueryDXL('with v1 as (select a from r), v2 as (select x.a as a1, y.a as a2 from v1 x, v1 y) select * from v2');
select gpoptutils.Optimize('with v1 as (select a from r), v2 as (select x.a as a1, y.a as a2 from v1 x, v1 y) select * from v2');  


select gpoptutils.DumpQueryDXL('
with cte1 as (SELECT a, b from r where a > 15)
SELECT *
FROM cte1
where a = 
     (WITH cte2 AS (SELECT a, b from R where A < 5)
      SELECT max(a) from cte2)');

-- DML
select gpoptutils.DumpQueryDXL('insert into r values (1,2)');
select gpoptutils.DumpQueryDXL('insert into r select * from r');
select gpoptutils.DumpQueryDXL('delete from r where a = 5');
select gpoptutils.DumpQueryDXL('delete from r where exists (select * from s where s.d = r.b');
select gpoptutils.DumpQueryDXL('delete from r using s where r.b = s.d');
select gpoptutils.DumpQueryDXL('update r set b=2 where b=5');
select gpoptutils.DumpQueryDXL('update r set a=5 from s where r.b=s.d');

-- unsupported insert
select gpoptutils.DumpQueryDXL('insert into r(a) values (1)');

-- unsupported indexes
drop table ao;
create table ao (i int, j int, k varchar) with(appendonly=true);
create index ao_j on ao using btree(j);
create index ao_k on ao using btree(k);
create index ao_jk on ao using btree((j + length(k)));
select gpoptutils.Optimize('select * from ao where j = 2');

-- folded function expressions
drop table if exists x;
create table x(a int, b int);
create or replace function f(a anyelement) returns anyelement as $$ select $1 $$ language sql;
select gpoptutils.Optimize('select * from f( (select ''hello world''::text from x limit 1) )');
select gpoptutils.DumpPlanDXL('select * from f( (select ''hello world''::text from x limit 1) )');

--- percentile function

select gpoptutils.DumpQueryDXL('select percentile_cont(0.2) within group (order by a) from generate_series(1, 100)a;');
select gpoptutils.DumpQueryDXL('select a from generate_series(1, 100)a group by a having percentile_cont(0.2) within group (order by a) > 10');

-- hash filters
select gpoptutils.RestorePlanDXL('<?xml version="1.0" encoding="UTF-8"?>                                                                
 <dxl:DXLMessage xmlns:dxl="http://greenplum.com/dxl/2010/12/">                                        
   <dxl:Plan>                                                                                          
     <dxl:GatherMotion InputSegments="0,1,2" OutputSegments="-1">                          
       <dxl:Properties>                                                                                
         <dxl:Cost StartupCost="0" TotalCost="100.000000" Rows="1000.000000" Width="1"/>               
       </dxl:Properties>                                                                               
       <dxl:ProjList>                                                                                  
         <dxl:ProjElem ColId="1" Alias="a">                                                            
           <dxl:Ident ColId="1" ColName="a" TypeMdid="0.23.1.0"/>                                      
         </dxl:ProjElem>                                                                               
         <dxl:ProjElem ColId="2" Alias="b">                                                            
           <dxl:Ident ColId="2" ColName="b" TypeMdid="0.23.1.0"/>                                      
         </dxl:ProjElem>                                                                               
       </dxl:ProjList>
       <dxl:Filter/>                                                                        
       <dxl:SortingColumnList/>                                                                                  
       <dxl:RedistributeMotion InputSegments="0,1,2" OutputSegments="0,1,2" DuplicateSensitive="true"> 
         <dxl:Properties>                                                                              
           <dxl:Cost StartupCost="0" TotalCost="107.882812" Rows="1000.000000" Width="24"/>            
         </dxl:Properties>                                                                             
         <dxl:ProjList>                                                                                
           <dxl:ProjElem ColId="1" Alias="a">                                                          
             <dxl:Ident ColId="1" ColName="a" TypeMdid="0.23.1.0"/>                                    
           </dxl:ProjElem>                                                                             
           <dxl:ProjElem ColId="2" Alias="b">                                                          
             <dxl:Ident ColId="2" ColName="b" TypeMdid="0.23.1.0"/>                                    
           </dxl:ProjElem>                                                                             
           <dxl:ProjElem ColId="3" Alias="ColRef_0003">                                                
             <dxl:Ident ColId="3" ColName="ColRef_0003" TypeMdid="0.23.1.0"/>                          
           </dxl:ProjElem>                                                                             
         </dxl:ProjList>                                                                               
         <dxl:Filter/>                                                                                 
         <dxl:SortingColumnList/>                                                                      
         <dxl:HashExprList>                                                                            
           <dxl:HashExpr TypeMdid="0.23.1.0">                                                          
             <dxl:Ident ColId="1" ColName="a" TypeMdid="0.23.1.0"/>                                    
           </dxl:HashExpr>                                                                             
         </dxl:HashExprList>                                                                           
         <dxl:Result>                                                                                  
           <dxl:Properties>                                                                            
             <dxl:Cost StartupCost="0" TotalCost="105.906250" Rows="1000.000000" Width="24"/>          
           </dxl:Properties>                                                                           
           <dxl:ProjList>                                                                              
             <dxl:ProjElem ColId="1" Alias="a">                                                        
               <dxl:Ident ColId="1" ColName="a" TypeMdid="0.23.1.0"/>                                  
             </dxl:ProjElem>                                                                           
             <dxl:ProjElem ColId="2" Alias="b">                                                        
               <dxl:Ident ColId="2" ColName="b" TypeMdid="0.23.1.0"/>                                  
             </dxl:ProjElem>                                                                           
             <dxl:ProjElem ColId="3" Alias="ColRef_0003">                                              
               <dxl:ConstValue TypeMdid="0.23.1.0" IsNull="false" IsByValue="true" Value="0"/>         
             </dxl:ProjElem>                                                                           
           </dxl:ProjList>                                                                             
           <dxl:Filter/>                                                                               
           <dxl:OneTimeFilter/>                                                                        
           <dxl:Result>                                                                                
             <dxl:Properties>                                                                          
               <dxl:Cost StartupCost="0" TotalCost="102.953125" Rows="1000.000000" Width="16"/>        
             </dxl:Properties>                                                                         
             <dxl:ProjList>                                                                            
               <dxl:ProjElem ColId="1" Alias="a">                                                      
                 <dxl:ConstValue TypeMdid="0.23.1.0" IsNull="false" IsByValue="true" Value="1"/>       
               </dxl:ProjElem>                                                                         
               <dxl:ProjElem ColId="2" Alias="b">                                                      
                 <dxl:ConstValue TypeMdid="0.23.1.0" IsNull="false" IsByValue="true" Value="2"/>       
               </dxl:ProjElem>                                                                         
             </dxl:ProjList>                                                                           
             <dxl:Filter/>                                                                             
             <dxl:OneTimeFilter/>                                                                      
             <dxl:Result>                                                                              
               <dxl:Properties>                                                                        
                 <dxl:Cost StartupCost="0" TotalCost="100.000000" Rows="1000.000000" Width="1"/>       
               </dxl:Properties>                                                                       
               <dxl:ProjList>                                                                          
                 <dxl:ProjElem ColId="0" Alias="">                                                     
                   <dxl:ConstValue TypeMdid="0.16.1.0" IsNull="false" IsByValue="true" Value="true"/>  
                 </dxl:ProjElem>                                                                       
               </dxl:ProjList>                                                                         
               <dxl:Filter/>                                                                           
               <dxl:OneTimeFilter/>                                                                    
             </dxl:Result>                                                                             
           </dxl:Result>                                                                               
         </dxl:Result>                                                                                 
       </dxl:RedistributeMotion>                                                                       
     </dxl:GatherMotion>                                                                                  
   </dxl:Plan>                                                                                         
 </dxl:DXLMessage>')