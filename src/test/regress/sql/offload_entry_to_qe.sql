set optimizer = off; -- orca is currently unsupported
set enable_offload_entry_to_qe = on;

create temp table tst(x int, y int);
create index tst_y_idx on tst(y);
insert into tst values (1, 1), (1, 1), (2, 1), (10, 10);
analyze tst;

-- accept Aggregate, Finalize Aggregate, Limit + Finalize Aggregate, WindowAgg, Sort + Unique
explain (costs off, locus) select count(x) from tst where x > 1;
explain (costs off, locus) select count(x) from tst;
explain (costs off, locus) select count(x) from tst limit 1;
explain (costs off, locus) select count(x) over () from tst;
explain (costs off, locus) select count(x) over () from tst group by x limit 1;
explain (costs off, locus) select distinct min(x), max(x) from tst;
reset enable_offload_entry_to_qe;  -- compare results with GUC set to false
select count(x) from tst where x > 1;
select count(x) from tst;
select count(x) from tst limit 1;
select count(x) over () from tst;
select count(x) over () from tst group by x limit 1;
select distinct min(x), max(x) from tst;
set enable_offload_entry_to_qe = on;
select count(x) from tst where x > 1;
select count(x) from tst;
select count(x) from tst limit 1;
select count(x) over () from tst;
select count(x) over () from tst group by x limit 1;
select distinct min(x), max(x) from tst;

-- accept Merge Join, Nested Loop and Hash Join
set enable_seqscan = off;
set enable_nestloop = off;
set enable_mergejoin = on;
explain (costs off, locus) select * from tst t1, tst t2 where t1.y = t2.y order by t1.y;
set enable_offload_entry_to_qe = off;
select * from tst t1, tst t2 where t1.y = t2.y order by t1.y;
set enable_offload_entry_to_qe = on;
select * from tst t1, tst t2 where t1.y = t2.y order by t1.y;
reset enable_mergejoin;
reset enable_nestloop;
reset enable_seqscan;
explain (costs off, locus) select * from (select min(x) x from tst) t1 join (values (1), (10)) v(x) on true;
explain (costs off, locus) select * from (select min(x) x from tst) t1 join (values (1), (10)) v(x) on t1.x = v.x;
reset enable_offload_entry_to_qe;
select * from (select min(x) x from tst) t1 join (values (1), (10)) v(x) on true;
select * from (select min(x) x from tst) t1 join (values (1), (10)) v(x) on t1.x = v.x;
set enable_offload_entry_to_qe = on;
select * from (select min(x) x from tst) t1 join (values (1), (10)) v(x) on true;
select * from (select min(x) x from tst) t1 join (values (1), (10)) v(x) on t1.x = v.x;

-- accept InitPlan and SubPlan
explain (costs off, locus) select count(*) from tst where tst.y = (select max(y) from tst);
explain (costs off, locus) select (select max((select distinct x from tst t2 where t2.x = t1.x))) from tst t1;
reset enable_offload_entry_to_qe;
select count(*) from tst where tst.y = (select max(y) from tst);
select (select max((select distinct x from tst t2 where t2.x = t1.x))) from tst t1;
set enable_offload_entry_to_qe = on;
select count(*) from tst where tst.y = (select max(y) from tst);
select (select max((select distinct x from tst t2 where t2.x = t1.x))) from tst t1;

-- test cte and recursive cte
explain (costs off, locus)
with t(a,b) as (select min(y), max(y) from tst),
     unused(a) as (select * from tst)
select t.b, rank() over () from t; -- test Subquery on QD

explain (costs off, locus)
with t1(a,b) as (select min(y), max(y) from tst),
     t2(a,b) as (select x, count(x) over () from tst group by x)
select * from t1 join t2 on t1.a < t2.a order by t1.a, t2.a;

create table recursive_cte_tst (id int,parentid int,score int) distributed replicated;
insert into recursive_cte_tst values(0, -1, 1);
insert into recursive_cte_tst values(1,  0, 1);
insert into recursive_cte_tst values(2,  0, 2);
insert into recursive_cte_tst values(3,  1, 10);
insert into recursive_cte_tst values(4,  1, 5);
insert into recursive_cte_tst values(5,  2, 1);
insert into recursive_cte_tst values(6,  3, 41);
insert into recursive_cte_tst values(7,  4, 42);
insert into recursive_cte_tst values(8,  5, 42);
insert into recursive_cte_tst values(9,  6, 42);
explain (locus, costs off) with recursive cte as (
  select 1 depth, a.id, a.score from recursive_cte_tst a where id = 0
  union all
  select c.depth + 1, k.id, k.score from recursive_cte_tst k inner join cte c on c.id = k.parentid
)
select rank() over (order by avg(score) desc),
       avg(score)
from cte group by depth order by avg desc limit 5; -- note that the SingleQE on the left side of RecursiveUnion *isn't* the same SingleQE as the right side

reset enable_offload_entry_to_qe;
with t(a,b) as (select min(y), max(y) from tst),
     unused(a) as (select * from tst)
select t.b, rank() over () from t;
with t1(a,b) as (select min(y), max(y) from tst),
     t2(a,b) as (select x, count(x) over () from tst group by x)
select * from t1 join t2 on t1.a < t2.a order by t1.a, t2.a;
with recursive cte as (
  select 1 depth, a.id, a.score from recursive_cte_tst a where id = 0
  union all
  select c.depth + 1, k.id, k.score from recursive_cte_tst k inner join cte c on c.id = k.parentid
) select rank() over (order by avg(score) desc), avg(score) from cte group by depth order by avg desc limit 5;

set enable_offload_entry_to_qe = on;
with t(a,b) as (select min(y), max(y) from tst),
     unused(a) as (select * from tst)
select t.b, rank() over () from t;
with t1(a,b) as (select min(y), max(y) from tst),
     t2(a,b) as (select x, count(x) over () from tst group by x)
select * from t1 join t2 on t1.a < t2.a order by t1.a, t2.a;
with recursive cte as (
  select 1 depth, a.id, a.score from recursive_cte_tst a where id = 0
  union all
  select c.depth + 1, k.id, k.score from recursive_cte_tst k inner join cte c on c.id = k.parentid
) select rank() over (order by avg(score) desc), avg(score) from cte group by depth order by avg desc limit 5;

-- reject pure Limit and pure InitPlan
explain (costs off, locus) select * from tst limit 1;
create function dummyf(int) returns int as 'select 1;' language sql;
explain (costs off, locus) select min(dummyf(x)) from tst;
explain (costs off, locus) select count(*) from tst where tst.x = (select min(dummyf(x)) from tst);
reset enable_offload_entry_to_qe;
select min(dummyf(x)) from tst;
select count(*) from tst where tst.x = (select min(dummyf(x)) from tst);
set enable_offload_entry_to_qe = on;
select min(dummyf(x)) from tst;
select count(*) from tst where tst.x = (select min(dummyf(x)) from tst);

-- reject updates
explain (costs off, locus) update tst set x = (select min(x) from tst);

-- test functions
explain (costs off, locus) select max(x)::text || ' ' || timeofday() from tst; -- volatile
explain (costs off, locus) select max(x)::text || ' ' || now() from tst; -- stable

-- test write functions
create function mod_dummyf(i int) returns int as $$
begin
update tst set y = y + 1 where x = $1;
return $1;
end;
$$ language plpgsql stable;
explain (costs off, locus) select mod_dummyf(42);
select mod_dummyf(42); -- should fail

drop function dummyf;
drop function mod_dummyf;

-- test external table
CREATE EXTERNAL WEB TEMP TABLE tst_exttbl(LIKE tst) EXECUTE 'printf "1\t42\n"' ON COORDINATOR FORMAT 'text';
CREATE EXTERNAL WEB TEMP TABLE tst_exttbl_all(LIKE tst) EXECUTE 'printf "2\t43\n"' ON ALL FORMAT 'text';
explain (costs off, locus) select max(e.y) from tst_exttbl e join tst t2 on (e.x = t2.x);
explain (costs off, locus) select max(e.y) from tst_exttbl_all e join tst t2 on (e.x = t2.x);
reset enable_offload_entry_to_qe;
select max(e.y) from tst_exttbl e join tst t2 on (e.x = t2.x);
select max(e.y) from tst_exttbl_all e join tst t2 on (e.x = t2.x);
set enable_offload_entry_to_qe = on;
select max(e.y) from tst_exttbl e join tst t2 on (e.x = t2.x);
select max(e.y) from tst_exttbl_all e join tst t2 on (e.x = t2.x);

-- test partitioned table
create temp table part(like tst) distributed by (x) partition by range (x)
(
    partition part1 start (0) end (10),
    partition part2 start (10) end (20)
);
insert into part select * from tst;
explain (costs off, locus) select min(y), max(y) from part;
reset enable_offload_entry_to_qe;
select min(y), max(y) from part;
set enable_offload_entry_to_qe = on;
select min(y), max(y) from part;

-- test partitioned table with external table as partition
ALTER TABLE part EXCHANGE PARTITION part1 WITH TABLE tst_exttbl;
explain (costs off, locus) select min(y), max(y) from part;
reset enable_offload_entry_to_qe;
select min(y), max(y) from part;
set enable_offload_entry_to_qe = on;
select min(y), max(y) from part;
ALTER TABLE part EXCHANGE PARTITION part1 WITH TABLE tst_exttbl_all;
explain (costs off, locus) select min(y), max(y) from part;
reset enable_offload_entry_to_qe;
select min(y), max(y) from part;
set enable_offload_entry_to_qe = on;
select min(y), max(y) from part;

reset enable_offload_entry_to_qe;