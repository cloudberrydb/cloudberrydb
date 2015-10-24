-- Test Mark/Restore in Material Node
drop table if exists test1;
drop table if exists test2;
create table test1 (a integer);
create table test2 (a integer);
insert into test1 select a from generate_series(1,400000) a;
insert into test2 select a from generate_series(1,400000) a;

set enable_hashagg=off;
set enable_mergejoin=on;
set enable_hashjoin=off;
set enable_nestloop=off;
set statement_mem=10000;

create temporary table testresult1 as
select t1.a as t1a, t2.a as t2a
from (select a from test1 group by a) t1,
     (select a from test2 group by a) t2
where t1.a = t2.a;

-- check that the result looks sane
select count(*), sum(t1a), sum(t2a), sum(t1a - t2a) from testresult1;

-- Test Hash Aggregation when the work mem is too small for the hash table
drop table if exists test;
create table test (a integer, b integer);
insert into test select a, a%25 from generate_series(1,8000) a;
analyze;
set enable_hashagg=on;
set enable_groupagg=off;
set statement_mem=10000;

select b,count(*) from test group by b;

select b,count(*) from test group by b;
-- Test Hash Join when the work mem is too small for the hash table
drop table if exists test;
create table test (a integer, b integer);
insert into test select a, a%25 from generate_series(1,800000) a;
analyze; -- We have to do an analyze to force a hash join
set enable_mergejoin=off;
set enable_nestloop=off;
set enable_hashjoin=on;
set statement_mem=10000;

create temporary table testresult2 as
select t1.a as t1a, t1.b as t1b, t2.a as t2a, t2.b as t2b from test t1, test t2 where t1.a = t2.a;
-- check that the result looks sane
select count(*), sum(t1a), sum(t2a), sum(t2a), sum(t2b), sum(t1a * t1b) from testresult2;
