set search_path=public,gpoptutils;

drop table r;
create table r();
alter table r add column a numeric;
alter table r add column b numeric;

insert into r select generate_series(1,1000);

select DumpPlanDXL('select 0.001::numeric from r');
select DumpPlanDXL('select NULL::text, NULL::int from r');
select DumpPlanDXL('select \'helloworld\'::text, \'helloworld2\'::varchar from r');
select DumpPlanDXL('select 129::bigint, 5623::int, 45::smallint from r');


select RestorePlanDXL(DumpPlanDXL('select 0.001::numeric from r'));
select RestorePlanDXL(DumpPlanDXL('select NULL::text, NULL::int from r'));
select RestorePlanDXL(DumpPlanDXL('select \'helloworld\'::text, \'helloworld2\'::varchar from r'));
select RestorePlanDXL(DumpPlanDXL('select 129::bigint, 5623::int, 45::smallint from r'));
