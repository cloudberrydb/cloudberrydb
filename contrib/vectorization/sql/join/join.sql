set vector.enable_vectorization = on;
set optimizer = on;

-- test left anti semi join(lasj not-in)
drop table if exists t1, t2;
create table t1(c1 int, c2 int) with(appendonly=true, orientation=column);
create table t2(c1 int, c2 int) with(appendonly=true, orientation=column);

-- explain
explain select * from t1 where t1.c2 not in (select t2.c2 from t2);

-- case 0: t1 empty; t2.c2 empty;
select * from t1 where t1.c2 not in (select t2.c2 from t2);

-- case 1: t1.c2 not-empty, no null; t2.c2 empty
insert into t1 values (1,2);
select * from t1 where t1.c2 not in (select t2.c2 from t2);

-- case 2: t1.c2 not-empty, has null; t2.c2 empty
insert into t1 values (1, null);
select * from t1 where t1.c2 not in (select t2.c2 from t2);

-- case 3: t1.c2 not-empty, has null; t2.c2 not empty, no null, has match
insert into t2 values (1, 5);
select * from t1 where t1.c2 not in (select t2.c2 from t2);

-- case 4: t1.c2 not-empty, has null; t2.c2 not empty, no null, no match
insert into t2 values (1, 2);
select * from t1 where t1.c2 not in (select t2.c2 from t2);

-- case 5: t1.c2 not-empty, has null; t2.c2 not empty, has null
insert into t2 values (1, null);
select * from t1 where t1.c2 not in (select t2.c2 from t2);

--------------------------------------------------------------------------

-- test semi join(exists)
drop table if exists t1, t2;
create table t1(c1 int, c2 int) with(appendonly=true, orientation=column);
create table t2(c1 int, c2 int) with(appendonly=true, orientation=column);

-- explain
explain select * from t1 where exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 1: t1 empty, t2 empty
select * from t1 where exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 2: t1 null; t2 empty
insert into t1 values (1, null);
select * from t1 where exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 3: t1 (null, 1); t2 empty
insert into t1 values (1,1);
select * from t1 where exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 3: t1 (null, 1), t2 null
insert into t2 values (1, null);
select * from t1 where exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 4: t1 (null, 1), t2 (null, 1)
insert into t2 values (1, 1);
select * from t1 where exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 5: t1 (null, 1), t2 (1)
drop table if exists t2;
create table t2(c1 int, c2 int) with(appendonly=true, orientation=column);
insert into t2 values (1, 1);
select * from t1 where exists (select t2.c2 from t2 where t1.c1 = t2.c1);

--------------------------------------------------------------------------
-- test anti join (not exists)
drop table if exists t1, t2;
create table t1(c1 int, c2 int) with(appendonly=true, orientation=column);
create table t2(c1 int, c2 int) with(appendonly=true, orientation=column);

-- explain
explain select * from t1 where not exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 0: t1 empty; t2.c2 empty;
select * from t1 where not exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 1: t1.c2 not-empty, no null; t2.c2 empty
insert into t1 values (1,2);
select * from t1 where not exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 2: t1.c2 not-empty, has null; t2.c2 empty
insert into t1 values (1, null);
select * from t1 where not exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 3: t1.c2 not-empty, has null; t2.c2 not empty, no null, has match
insert into t2 values (1, 5);
select * from t1 where not exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 4: t1.c2 not-empty, has null; t2.c2 not empty, no null, no match
insert into t2 values (1, 2);
select * from t1 where not exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- case 5: t1.c2 not-empty, has null; t2.c2 not empty, has null
insert into t2 values (1, null);
select * from t1 where not exists (select t2.c2 from t2 where t1.c1 = t2.c1);

-- finish
drop table if exists t1, t2;