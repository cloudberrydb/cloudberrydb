drop schema if exists sirvf cascade;
create schema sirvf;
set search_path=sirvf;

--
-- distributed tables
--

create table read_dist(x int) distributed by (x);
insert into read_dist select generate_series(1,10);

create table scratch_dist(x int) distributed by (x);

analyze read_dist;
analyze scratch_dist;

--
-- UDF that reads distributed tables and writes to distributed scratch tables
--

create or replace function f1_dist (i text) returns text as
$$
declare
  v_c int;
begin
  insert into scratch_dist select * from read_dist;

  -- Some SELECT
  select count(*) as count from scratch_dist into v_c;

  delete from scratch_dist;

  return 'abc' || v_c || '_' || i || '_dist';
end;
$$
language plpgsql volatile MODIFIES SQL DATA;


--
-- 1) CTAS with f1 in the targetlist
--


create table t1 as select f1_dist('booya');

select * from t1;

drop table if exists t1;

-- workaround

create table t1 as select (select f1_dist('booya'));

select * from t1;

drop table if exists t1;


--
-- 2) CTAS with f1 in the from clause
--

create table t1 as select * from f1_dist('booya');

select * from t1;

drop table if exists t1;

-- workaround

create table t1 as select * from (select (select f1_dist('booya'))) as A;

select * from t1;

drop table if exists t1;

--
-- 3) Insert with f1_dist in the targetlist
--

create table t1 (x text);

insert into t1 select f1_dist('booya');

select * from t1;

drop table if exists t1;

-- workaround

create table t1 (x text);

insert into t1 select (select f1_dist('booya'));

select * from t1;

drop table if exists t1;

--
-- 4) Insert with f1 in the from clause
--

create table t1 (x text);

insert into t1 select * from f1_dist('booya');

select * from t1;

drop table if exists t1;

-- workaround

create table t1 (x text);

insert into t1 select * from (select (select f1_dist('booya'))) as A;

select * from t1;

drop table if exists t1;

--
-- Functions that returns records
--

drop table if exists foo cascade;
create table foo(i int, t text);
insert into foo select 1, 'hello world';

create or replace function nextfoo(i int) returns foo as $$
declare
	r foo;
begin
	execute 'select i+1, t  from foo order by i desc limit 1' into r;
	return r;
end
$$ language plpgsql READS SQL DATA;

-- Select statements

select * from foo;

select * from nextfoo(1);

-- Inserts

insert into foo select * from nextfoo(1);

select * from foo order by 1,2;

insert into foo(t) select f.t from nextfoo(1) f;

select * from foo order by 1,2;

-- More complex expression

insert into foo(t) select f.t || '100' from nextfoo(1) f;

select * from foo order by 1,2;

--
-- Nested function calls.
--


create table t1 as select substring(f1_dist('booya'),0,3);

select * from t1;

drop table if exists t1;

create table t1 as select * from substring(f1_dist('booya'),0,3);

select * from t1;

drop table if exists t1;

--
-- MPP-16071. Multiple sirvfs in the from clause
--

create table t1 as select * from f1_dist('booya1') a, f1_dist('booya2');

select * from t1;

drop table if exists t1;
