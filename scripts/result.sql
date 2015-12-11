drop table foo;
drop table bar1;
drop table bar2;

create table foo (x1 int, x2 int, x3 int);
create table bar1 (x1 int, x2 int, x3 int) distributed randomly;
create table bar2 (x1 int, x2 int, x3 int) distributed randomly;

insert into foo select i,i+1,i+2 from generate_series(1,10) i;

insert into bar1 select i,i+1,i+2 from generate_series(1,1000) i;
insert into bar2 select i,i+1,i+2 from generate_series(1,1000) i;

select DumpPlanXML('select x2 from foo where x1 in (select x2 from bar1)'); -- produces result node

select RestorePlanXML(DumpPlanXML('select x2 from foo where x1 in (select x2 from bar1)')); -- produces result node

select DumpPlanXML('select 1');

select RestorePlanXML(DumpPlanXML('select 1'));

