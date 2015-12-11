drop table foo;
create table foo(x int);
insert into foo select generate_series(1,1000);

drop table r;
create table r();
alter table r add column a int;
alter table r add column b int;

drop table s;
create table s();
alter table s add column c int;
alter table s add column d int;
alter table s add column e int;

select RestorePlan(DumpPlan('select * from r'));
select RestorePlan(DumpPlan('select * from r,s where r.a=s.c'));
select RestorePlan(DumpPlan('select * from r, s where r.a=s.c'));
select RestorePlan(DumpPlan('select * from r,s where r.a<s.c+5 or r.a>s.c+10'));
select RestorePlan(DumpPlan('select a, b, sum(d) from r,s group by a,b'));
select DumpPlanToFile('select * from r', 'dxl-q1.aux');
select DumpPlanToFile('select * from r,s where r.a=s.c', 'dxl-q2.aux');
select DumpPlanToFile('select * from r, s where r.a=s.c', 'dxl-q3.aux');
select DumpPlanToFile('select * from r,s where r.a<s.c+1 or r.a>s.c', 'dxl-q4.aux');
select DumpPlanToFile('select r.a+1 from r', 'dxl-q6.aux');
select DumpPlanToFile('select a, b, sum(d) from r,s group by a,b', 'dxl-q5.aux');
select DumpPlanToFile('select * from r', 'dxl-q1.aux');
select DumpPlanToFile('select 1', 'dxl-q7.aux');
select DumpPlanToFile('select * from s where s.c<s.d+s.e', 'dxl-q8.aux');
select RestorePlanFromFile('dxl-q1.aux');
select RestorePlanFromFile('dxl-q2.aux');
select RestorePlanFromFile('dxl-q3.aux');
select RestorePlanFromFile('dxl-q4.aux');
select RestorePlanFromFile('dxl-q5.aux');
select RestorePlanFromFile('dxl-q6.aux');
