--  MPP-21536: Duplicated rows inserted when ORCA is turned on

-- create test table
create table m();
alter table m add column a int;
alter table m add column b int;

-- generate data for m
insert into m select i, i%5 from generate_series(1,10)i;

-- INSERT and UPDATE
create table yyy(a int, b int) distributed randomly;

insert into yyy select a,b from m;
select * from yyy order by 1, 2;

update yyy set a=m.b from m where m.a=yyy.b;
select * from yyy order by 1, 2;

drop table yyy;


-- UPDATE with different values
create table yyy(a int, b int) distributed randomly;

insert into yyy select a,b from m;
update yyy set b=m.b from m where m.a=yyy.a;
select * from yyy order by 1, 2;

drop table yyy;


-- DELETE
create table yyy(a int, b int) distributed randomly;

insert into yyy select a,b from m;
delete from yyy where a in (select a from m);
select * from yyy order by 1, 2;

drop table yyy;

create table yyy(a int, b int) distributed randomly;
insert into yyy select a,b from m;
delete from yyy where b in (select a from m);
select * from yyy order by 1, 2;

drop table yyy;


-- Now repeat all the above tests, but using a hacked master-only 'm' table
drop table m;

set optimizer_enable_master_only_queries=on;


-- create master-only table
create table m();
set allow_system_table_mods='DML';
delete from gp_distribution_policy where localoid='m'::regclass;
reset allow_system_table_mods;
alter table m add column a int;
alter table m add column b int;

-- generate data for m
insert into m select i, i%5 from generate_series(1,10)i;

create table zzz(a int primary key, b int) distributed by (a);
insert into zzz select a,b from m;
select * from zzz order by 1, 2;

delete from zzz where a in (select a from m);
select * from zzz order by 1, 2;

drop table zzz;

create table zzz(a int primary key, b int) distributed by (a);

insert into zzz select a,b from m;
delete from zzz where b in (select a from m);
select * from zzz order by 1, 2;

drop table zzz;

create table zzz(a int primary key, b int) distributed by (a);
insert into zzz select a,b from m;

-- This update fails with duplicate key error, but it varies which segment
-- reports it first, i.e. it varies which row it complaints first. Silence
-- that difference in the error DETAIL line
\set VERBOSITY terse
update zzz set a=m.b from m where m.a=zzz.b;
select * from zzz order by 1, 2;

drop table zzz;

create table zzz(a int primary key, b int) distributed by (a);
insert into zzz select a,b from m;
update zzz set b=m.b from m where m.a=zzz.a;
select * from zzz order by 1, 2;

drop table zzz;
drop table m;


-- MPP-21622 Update with primary key: only sort if the primary key is updated
--
-- Aside from testing that bug, this also tests EXPLAIN of an DMLActionExpr
-- that ORCA generates for plans that update the primary key.
create table update_pk_test (a int primary key, b int) distributed by (a);
insert into update_pk_test values(1,1);

explain update update_pk_test set b = 5;
update update_pk_test set b = 5;
select * from update_pk_test order by 1,2;

explain update update_pk_test set a = 5;
update update_pk_test set a = 5;
select * from update_pk_test order by 1,2;
