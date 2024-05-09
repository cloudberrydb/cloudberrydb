set vector.enable_vectorization = on;
-- set the number of tuples in a group to 5,
-- so we can test multiple group with visimap,
-- at the beginning, in the middle, or at the end
-- of a group.
set pax_max_tuples_per_group = 5;

create table pt1(a int, b int) using pax;
insert into pt1 select 1, i from generate_series(1,15)i;

select ctid, * from pt1;

begin;
delete from pt1 where b < 2;
select ctid, * from pt1;
rollback;

begin;
delete from pt1 where b = 5;
select ctid, * from pt1;
rollback;

begin;
delete from pt1 where b >= 6 and b <= 7;
select ctid, * from pt1;
rollback;

begin;
delete from pt1 where b = 8;
select ctid, * from pt1;
rollback;

begin;
delete from pt1 where b > 7 and b <= 10;
select ctid, * from pt1;
rollback;

-- Test if visimap works well with btree index
create unique index on pt1(a, b);
begin;
delete from pt1 where b >= 4 and b < 8;
select ctid, * from pt1;
explain (costs off) select * from pt1 where a = 1 and b >= 2 and b < 10;
select * from pt1 where a = 1 and b >= 2 and b < 10;
rollback;

drop table pt1;

reset pax_max_tuples_per_group;
reset vector.enable_vectorization;
