drop table if exists bug;
drop table if exists bug2;

create table bug as select '2008-02-01'::DATE AS DT, 
	case when j <= 96 
		then 0 
	when j<= 98 then 2 
	when j<= 99 then 3 
	when i % 1000 < 900 then 4 
	when i % 1000 < 800 then 5 
	when i % 1000 <= 998 then 5 else 6 
	end as ind, 
	(i*117-j)::bigint as s from generate_series(1,100) i, generate_series(1,100) j distributed randomly;

create table bug2 as select * from bug;

insert into bug select DT + 1, ind, s from bug2;
insert into bug select DT + 2, ind, s from bug2;
insert into bug select DT + 3, ind, s from bug2;
insert into bug select DT + 4, ind, s from bug2;
insert into bug select DT + 5, ind, s from bug2;
insert into bug select DT + 6, ind, s from bug2;
insert into bug select DT + 7, ind, s from bug2;
insert into bug select DT + 8, ind, s from bug2;
insert into bug select DT + 9, ind, s from bug2;
insert into bug select DT + 10, ind, s from bug2;
insert into bug select DT + 11, ind, s from bug2;
insert into bug select DT + 12, ind, s from bug2;
insert into bug select DT + 13, ind, s from bug2;
insert into bug select DT + 14, ind, s from bug2;
insert into bug select DT + 15, ind, s from bug2;

create index bug_idx on bug using bitmap (ind, dt);

vacuum analyze bug;
create table mpp4593_bmbug (dt date, ind int, s bigint);
create index bmbug_idx on mpp4593_bmbug using bitmap(ind, dt);
-- Test Bitmap Indexscan
drop table if exists test;
create table test (a integer, b integer) distributed by (a);
insert into test select a, a%25 from generate_series(1,100) a;
create index test_a on test (a);
set enable_seqscan=off;
set enable_indexscan=off;
set enable_bitmapscan=on;

-- start_ignore
select * from test where a<10;
-- end_ignore

-- returnning one or more tuples
select * from test where a<10;

-- start_ignore
select * from test where a>100;
-- end_ignore

-- returninig no tuples
select * from test where a>100;
-- Test Bitmap Heapscan + Bitmap OR + Bitmap Indexscan

drop table if exists test;
create table test (a integer, b integer) distributed by (a);
insert into test select a, a%25 from generate_series(1,100) a;

-- Test on 2 btrees
create index test_a on test (a);
create index test_b on test (b);

set enable_seqscan=off;
set enable_indexscan=off;
set enable_bitmapscan=on;

-- start_ignore
select * from test where a<100 or b>10;
-- end_ignore

select * from test where a<100 or b>10;

-- start_ignore
select * from test where a<100 or b>30;
-- end_ignore

-- Returnning no tuples from one branch
select * from test where a<100 or b>30;

-- start_ignore
select * from test where a<1 or b>30;
-- end_ignore

-- Returnning no tuples from both branch
select * from test where a<1 or b>30;

-- Test on 2 bitmaps
drop index test_a;
drop index test_b;
create index test_bm_a on test using bitmap(a);
create index test_bm_b on test using bitmap(b);

-- start_ignore
select * from test where a<100 or b>10;
-- end_ignore

select * from test where a<100 or b>10;

-- start_ignore
select * from test where a<100 or b>30;
-- end_ignore

-- Returnning no tuples from one branch
select * from test where a<100 or b>30;

-- start_ignore
select * from test where a<1 or b>30;
-- end_ignore

-- Returnning no tuples from both branch
select * from test where a<1 or b>30;

-- Test on 1 btree, 1 bitmap
drop index test_bm_a;
drop index test_bm_b;
create index test_a on test (a);
create index test_bm_b on test using bitmap(b);

-- start_ignore
select * from test where a<100 or b>10;
-- end_ignore

select * from test where a<100 or b>10;

-- start_ignore
select * from test where a<100 or b>30;
-- end_ignore

-- Returnning no tuples from one branch
select * from test where a<100 or b>30;

-- start_ignore
select * from test where a<1 or b>30;
-- end_ignore

-- Returnning no tuples from both branch
select * from test where a<1 or b>30;
-- Test ArrayKeys
drop table if exists test;
create table test (a integer, b integer) distributed by (a);
insert into test select a, a%25 from generate_series(1,100) a;
create index test_a on test (a);
set enable_seqscan=off;
set enable_indexscan=off;
set enable_bitmapscan=on;

-- start_ignore
select * from test where a in (1,3,5);
-- end_ignore

select * from test where a in (1,3,5);
