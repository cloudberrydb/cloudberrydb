\c tpch_heap
\echo -- start_ignore
-- Drop indexes, function, trigger if exist
DROP INDEX IF EXISTS idx_ckey;
DROP INDEX IF EXISTS idx_ckey_cname;
DROP INDEX IF EXISTS idx_nation_bitmap;
DROP INDEX IF EXISTS idx_lineitem_keys;
DROP INDEX IF EXISTS idx_linenumber;
DROP TRIGGER IF EXISTS before_heap_ins_trig ON customer;
DROP FUNCTION IF EXISTS trigger_func();
\echo -- end_ignore

-- Create indexes on customer table
CREATE UNIQUE INDEX idx_ckey ON customer USING btree (c_custkey);

CREATE INDEX idx_ckey_cname ON customer USING btree (c_custkey, c_name);

CREATE INDEX idx_nation_bitmap ON customer USING bitmap (c_nationkey);

-- Create indexes on lineitem table
CREATE UNIQUE INDEX idx_lineitem_keys ON lineitem USING btree (l_orderkey, l_partkey, l_suppkey, l_linenumber);

CREATE INDEX idx_linenumber ON lineitem USING btree (l_linenumber);

-- Create function trigger_func() that will be used as the trigger
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql NO SQL AS '
BEGIN
RAISE NOTICE ''trigger_func() called: action = %, when = %, level = %'', TG_OP, TG_WHEN, TG_LEVEL;
RETURN NULL;
END;';

-- Create trigger before_heap_ins_trig
CREATE TRIGGER before_heap_ins_trig BEFORE INSERT ON customer
FOR EACH ROW EXECUTE PROCEDURE trigger_func();
--\c gptest
-- Check TPCH all records of all tables are correctly restored
select count(*), sum(n_nationkey), min(n_nationkey), max(n_nationkey) from nation;
select count(*), sum(r_regionkey), min(r_regionkey), max(r_regionkey) from region;
select count(*), sum(p_partkey), min(p_partkey), max(p_partkey) from part;
select count(*), sum(s_suppkey), min(s_suppkey), max(s_suppkey) from supplier;
select count(*), sum(ps_partkey + ps_suppkey), min(ps_partkey + ps_suppkey), max(ps_partkey + ps_suppkey) from partsupp;
select count(*), sum(c_custkey), min(c_custkey), max(c_custkey) from customer;
select count(*), sum(o_orderkey), min(o_orderkey), max(o_orderkey) from orders;
select count(*), sum(l_linenumber), min(l_linenumber), max(l_linenumber) from lineitem;
-- Check indexes and trigger on customer and lineitem table
\d+ customer
\d+ lineitem

\echo -- start_ignore
--
-- Create database parttest_dump to test dump partition tables
drop database if exists parttest_dump;
create database parttest_dump;

\c parttest_dump

-- Create partition tables sales
CREATE TABLE sales (id int, year int, month int, day int, region text) 
DISTRIBUTED BY (id) 
PARTITION BY RANGE (year)

  SUBPARTITION BY RANGE (month)
    SUBPARTITION TEMPLATE (
      START (1) END (13) EVERY (1), 
      DEFAULT SUBPARTITION other_months )

  SUBPARTITION BY LIST (region)
    SUBPARTITION TEMPLATE (
      SUBPARTITION usa VALUES ('usa'),
      SUBPARTITION europe VALUES ('europe'),
      SUBPARTITION asia VALUES ('asia'),
      DEFAULT SUBPARTITION other_regions)

  ( START (2002) END (2010) EVERY (1),
    DEFAULT PARTITION outlying_years);

-- Populate sales table with data
insert into sales values (1, 2002, 1, 1, 'usa');
insert into sales values (2, 2002, 1, 1, 'usa');
insert into sales values (3, 2003, 1, 1, 'usa');
insert into sales values (4, 2003, 2, 1, 'usa');
insert into sales values (5, 2003, 2, 2, 'usa');
insert into sales values (6, 2003, 2, 1, 'asia');
insert into sales values (7, 2001, 1, 1, 'usa');
insert into sales values (8, 2011, 1, 1, 'usa');
insert into sales values (9, 2003, 13, 1, 'usa');
insert into sales values (10, 2003, 0, 1, 'usa');
insert into sales values (11, 2003, 1, 33, 'usa');
insert into sales values (12, 2010, 2, 1, 'canada');
insert into sales values (13, 2003, 1, 2, 'europe');
insert into sales values (14, 2001, 2, 1, 'canada');

-- Create index on sales table
create index ind_sales_id on sales (id);


-- Check gp_toolkit schema and its objects
-- MPP-13382
-- \dv gp_toolkit.

-- ignore this output
\c parttest_dump
\echo -- end_ignore

-- Check db after restore with GPDBRESTORE
select count(*) from pg_class where relname like 'sales%' and relhasindex='t';
select count(*) from pg_class where relname like 'ind_sales_id%';
select count(*) from pg_index where indisvalid='t' and indrelid in (select oid from pg_class where relname like 'sales%' and relhasindex='t');
select * from sales order by id;

-- drop parttest_dump database
\echo -- start_ignore
-- ignore this output
\c template1
drop database if exists parttest_dump;
\echo -- end_ignore
\echo -- start_ignore

--
-- Create test database mpp11880
drop database if exists mpp11880;
create database mpp11880;
\c mpp11880

-- Create test table test_10m
create table public.test_10m (Same text, Mod10 text, Mod100 text, Mod1000 text, Mod10000 text, Uniq text) distributed by (Mod10);

-- insert two long records: MPP-12079
insert into test_10m select 'SAME','0','0',i from repeat('ox',100000)i;
insert into test_10m select 'SAME','0','0',i from repeat('xo',100000)i;

-- insert 10M records
insert into test_10m select 'SAME', i%10, i%100, i%1000, i%10000, i from generate_series(1,10000000) i;

-- ignore this output
\c mpp11880
\echo -- end_ignore

-- Check db after restore with GPDBRESTORE
select count(*) from public.test_10m;

-- drop parttest_dump database
\echo -- start_ignore
-- ignore this output

--
drop schema if exists globaltest_schema;
drop database if exists dumpglobaltest;
drop role if exists globaltest_role;

-- Create test database dumpglobaltest
create database dumpglobaltest;
\c dumpglobaltest

-- Create a role
create role globaltest_role login;

-- Grant privillige
grant create on database dumpglobaltest to globaltest_role;

-- Create a custom schema
create schema globaltest_schema authorization globaltest_role;



\c template1
drop database mpp11880
\echo -- end_ignore
\echo -- start_ignore
-- ignore this output
\c dumpglobaltest
\echo -- end_ignore

-- Check schema and role are correctly restored by GPDBRESTORE
\du globaltest_role

\dn globaltest_schema

-- drop parttest_dump database
\echo -- start_ignore
-- ignore this output
\c template1
drop schema if exists globaltest_schema;
drop database if exists dumpglobaltest;
drop role if exists globaltest_role;

-- rebuild lineitem
drop table lineitem;
alter table lineitem_monkey rename to lineitem;
select count(*), sum(l_linenumber), min(l_linenumber), max(l_linenumber) from lineitem;

-- Check indexes and trigger on customer and lineitem table
\d+ customer
\d+ lineitem

-- Check dumptesttbl
--\d+ dumptesttbl
--insert into dumptesttbl(zipcode) values('123456');
--insert into dumptesttbl(areacode) values(-1);
--select * from dumptesttbl;

-- Check gp_toolkit schema and its objects
-- MPP-13382
-- \dv gp_toolkit.
