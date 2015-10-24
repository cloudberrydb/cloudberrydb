
--
-- PLPGSQL
--
-- Testing various scenarios where plans will not be cached. 
-- MPP-16204

--
-- ************************************************************
-- * Repro with drop table inside a function
-- *    - Multiple executions should not raise an error 
-- ************************************************************
--
drop table if exists cache_tab cascade;

drop function if exists cache_test();

create function cache_test() returns void as
$$
begin
	drop table if exists cache_tab;
	create table cache_tab (id int) distributed randomly;
	insert into cache_tab values (1);
end;
$$ language plpgsql MODIFIES SQL DATA;

select cache_test();

-- following should not fail. 
select cache_test();

drop table cache_tab;

drop function cache_test();

--
-- ************************************************************
-- * Repro with SELECT .. INTO inside a function
-- *    - Multiple executions should not raise an error 
-- ************************************************************
--

create table cache_tab
(
	c1 int, 
	c2 text
) partition by range(c1)
(default partition def,
partition part1 start (int '1') end (int '10')); 

insert into cache_tab values(1, 2);
insert into cache_tab values(2, 3);
insert into cache_tab values(3, 4);

create function cache_test(id int) returns int as $$
declare 
	v_int int;
begin 
	select c1 from cache_tab where c2 = id INTO v_int; 
	return v_int;
end;
$$ language plpgsql READS SQL DATA;

select * from cache_test(1);

alter table cache_tab split default partition 
start (11) inclusive
end (20) exclusive 
into (partition part2, partition def);

-- following should not fail. 
select * from cache_test(2);

drop table cache_tab cascade;

drop function cache_test(int);

--
-- ************************************************************
-- * Repro with drop table between executions 
-- *    - Multiple executions should not raise an error 
-- ************************************************************
--

create table cache_tab (
	id int, 
	name varchar(50)
) distributed randomly;

insert into cache_tab values(1, 'abc');
insert into cache_tab values(2, 'def');

drop function if exists cache_test(var int);

create function cache_test(var int) returns varchar as $$
declare 
	v_name varchar(20) DEFAULT 'zzzz';
begin 
	select name from cache_tab into v_name where id = var; 
	return v_name;
end;
$$ language plpgsql READS SQL DATA;

select * from cache_test(1);

drop table  if exists cache_tab;

create table cache_tab (
	id int, 
	name varchar(50)
) distributed randomly;

-- following should not fail. 
select * from cache_test(2);

drop table cache_tab;

drop function cache_test(int);

--
-- ************************************************************
-- * Repro with return cursor 
-- *    - Multiple executions should not raise an error 
-- ************************************************************
--

create table cache_tab (
	c1 int, 
	c2 int, 
	c3 int
) distributed randomly;
 
insert into cache_tab values(1, 2, 100);

insert into cache_tab values(2, 3, 200);

insert into cache_tab values(3, 4, 300);

create function cache_test(refcursor) returns refcursor as $$
begin 
	open $1 for select * from cache_tab;
	return $1;
end;
$$
language plpgsql READS SQL DATA;

begin;
select cache_test('refcursor');
fetch all in refcursor;
commit;

drop table if exists  cache_tab; 

create table cache_tab (
	c1 int, 
	c2 int, 
	c3 int
) distributed randomly;


begin;
select cache_test('refcursor');
fetch all in refcursor;
commit;

drop table cache_tab; 

drop function cache_test(refcursor);

--
-- ************************************************************
-- * Repro with fetch cursor
-- *    - Multiple executions should not raise an error 
-- ************************************************************
--

create table cache_tab(
	c1 int, 
	c2 int, 
	c3 int
) distributed randomly; 

insert into cache_tab values(1, 2, 100);

insert into cache_tab values(1, 2, 100);

create function cache_test(var int) returns int as $$
declare 
	cur refcursor;
	res int;
	total_res int default 0;
begin
	open cur for select c2 from cache_tab where c1 = var; 
	fetch cur into res;
	while res is not null 
	loop
  		total_res := total_res + res;
  		fetch cur into res;
	end loop;
	return total_res;
end;
$$ language plpgsql READS SQL DATA;

select cache_test(1); 

drop table  if exists cache_tab; 

create table cache_tab(
	c1 int, 
	c2 int, 
	c3 int
) distributed randomly; 

insert into cache_tab values(1, 2, 100);

-- following should not fail
select cache_test(1);

drop table cache_tab; 

drop function cache_test(int);

--
-- ************************************************************
-- * Repro with function planned on segments
-- *    - plan should be cached
-- ************************************************************
--

create table cache_tab
(
	c1 int, 
	c2 text
) partition by range(c1)
(default partition def,
partition part1 start (int '1') end (int '10')); 

insert into cache_tab values(1, 'foo1');

create function cache_test() returns int as $$
declare 
	v_temp varchar(10); 
begin 
	select into v_temp hastriggers from pg_tables;
	if v_temp is not null
	then
		return 1;
	else
		return 0;
	end if;
end;
$$ language plpgsql READS SQL DATA;
   
select * from cache_tab where c1 = cache_test(); 

select * from cache_tab where c1 = cache_test();

drop table cache_tab;

drop function cache_test();

--
-- ************************************************************
-- * Block statement execution
-- *    - Multiple executions should not raise an error 
-- ************************************************************
--

create table cache_tab
(
	c1 int, 
	c2 int
) partition by range(c2)
(default partition def,
partition part1 start (int '1') end (int '10')); 

insert into cache_tab values(0, 100);

insert into cache_tab values(1, 100);

insert into cache_tab values(2, 100);

create function cache_test(key int) returns int as $$ 
declare 
	v_int int;
	v_res int default 0;
begin 
loop
	select c1 from cache_tab into v_int where c2 = key;
	if found then
		return v_res;
	end if;
	if v_int != 0 then
		v_res := v_res + v_int;
	end if;
end loop;
end;
$$ language plpgsql READS SQL DATA; 

select cache_test(100); 

alter table cache_tab split default partition 
start (11) inclusive
end (20) exclusive 
into (partition part2, partition def);

select cache_test(100); 

drop table cache_tab cascade; 

drop function cache_test(int);

--
-- ************************************************************
-- * Repro with PERFORM 
-- *    - Multiple executions should not raise an error 
-- ************************************************************
--

create table cache_tab
(
	c1 int, 
	c2 int
) partition by range(c2)
(default partition def,
partition part1 start (int '1') end (int '10')); 

insert into cache_tab values(1, 100);

insert into cache_tab values(2, 100);

insert into cache_tab values(3, 100);

create function cache_test() returns void AS $$
begin
	perform c1 from cache_tab;
end;
$$ language plpgsql READS SQL DATA;

select cache_test();

drop table if exists  cache_tab; 

create table cache_tab
(
	c1 int, 
	c2 int
) partition by range(c2)
(default partition def,
partition part1 start (int '1') end (int '10')); 

insert into cache_tab values(1, 100);

select cache_test();

drop table cache_tab; 

drop function cache_test();

--
-- ************************************************************
-- * example with table functions
-- *
-- ************************************************************
--

create table cache_tab
(
	a int, 
	b int
) distributed randomly;

insert into cache_tab values(1, 100);
insert into cache_tab values(2, 200);

drop function if exists get_cache_tab();

create function get_cache_tab() returns setof cache_tab as $$
	select * from cache_tab where a = 1;
$$ language sql READS SQL DATA;

create function cache_test() returns setof integer as
$$
declare 
	r integer;
begin
	for r IN select a from get_cache_tab() 
	loop
		return next r;
	end loop;
	return;
end
$$ language plpgsql READS SQL DATA;

select cache_test();

drop function if exists get_cache_tab();

create function get_cache_tab() returns setof cache_tab as $$
  select * from cache_tab where a = 2;
$$ language sql READS SQL DATA;


-- plan should not be cached, returns different results
select cache_test();

drop table cache_tab cascade;

drop function if exists get_cache_tab() ;

drop function if exists cache_test();

-- ************************************************************
-- * an example with CTAS
-- * 	multiple executions should not raise an error
-- ************************************************************
create table cache_tab(id int, data text);

insert into cache_tab values(1, 'abc');

insert into cache_tab values(2, 'abc');

insert into cache_tab values(3, 'abcd');

create or replace function cache_test() returns void as 
$$
begin
	drop table if exists cache_temp;
	create table cache_temp as select * from cache_tab distributed randomly;
end
$$ language plpgsql MODIFIES SQL DATA; 

select cache_test();

select * from cache_temp;

drop table cache_tab;

create table cache_tab(id int, data text);

insert into cache_tab values(1, 'abcde');

-- should not raise an error 
select cache_test();

-- should return 1 row 
select * from cache_temp;

drop table cache_tab cascade; 

drop function cache_test();

drop table cache_temp;

--
-- ************************************************************
-- * recursive functions
-- ************************************************************
--

-- start_matchsubs
--
-- m|ERROR:\s+relation with OID \d+ does not exist|
-- s|ERROR:\s+relation with OID \d+ does not exist|ERROR: relation with OID DUMMY does not exist|
--
-- end_matchsubs


create table cache_tab(c1 int, c2 int) distributed randomly; 

drop function if exists cache_test(count int); 
create function cache_test(count int) returns int as $$
begin 
	if $1 <= 0 then 
		return $1;
  	else 
		insert into cache_tab values($1, $1);
  	end if; 
  	return cache_test($1-1);
end; 
$$ language plpgsql MODIFIES SQL DATA;

select cache_test(5);

drop table if exists cache_tab;

create table cache_tab(c1 int, c2 int) distributed randomly; 

select cache_test(5);

drop function cache_test(count int); 

--- another example with recursive functions 
create function cache_test(count int) returns int as $$
begin 
	if $1 <= 0 then 
		return $1;
  	else 
		drop table if exists cache_tab;
		create table cache_tab(c1 int, c2 int) distributed randomly; 
		insert into cache_tab values($1, $1);
  	end if; 
  	return cache_test($1-1);
end; 
$$ language plpgsql MODIFIES SQL DATA;

-- this will fail
select cache_test(5);

set gp_plpgsql_clear_cache_always = on;

-- this will pass
select cache_test(5);

drop table if exists cache_tab;

drop function cache_test(count int) cascade;

--
-- ************************************************************
-- * testing guc
-- ************************************************************
--

drop table if exists cache_tab cascade;

drop function if exists cache_test();

create function cache_test() returns void as
$$
begin
	drop table if exists cache_tab;
	create table cache_tab (id int) distributed randomly;
	insert into cache_tab values (1);
end;
$$ language plpgsql MODIFIES SQL DATA;


BEGIN;

select cache_test();

-- this will fail
select cache_test();

COMMIT; 

set gp_plpgsql_clear_cache_always = on;

select cache_test();


drop table cache_tab;

drop function cache_test();

--
-- ************************************************************
-- * testing guc
-- ************************************************************
--
drop table if exists cache_tab cascade;

drop function if exists cache_test(); 

set gp_plpgsql_clear_cache_always = off;
create function cache_test() returns void as
$$
declare count int; 
begin
	count := 3;
	while count > 0 
	loop
		drop table if exists cache_tab;
		create table cache_tab (id int) distributed randomly;
		insert into cache_tab values (1);
		count := count - 1;
	end loop;
end;
$$ language plpgsql MODIFIES SQL DATA;

-- this will fail
select cache_test();

set gp_plpgsql_clear_cache_always = on;

-- this will pass
select cache_test();

set gp_plpgsql_clear_cache_always = off;

drop function cache_test();

create function cache_test() returns void as
$$
declare count int; 
begin
	count := 3;
	while count > 0 
	loop
		set gp_plpgsql_clear_cache_always = on;
		drop table if exists cache_tab;
		create table cache_tab (id int) distributed randomly;
		insert into cache_tab values (1);
		count := count - 1;
	end loop;
end;
$$ language plpgsql MODIFIES SQL DATA;

select cache_test();

drop table cache_tab;

drop function cache_test();
