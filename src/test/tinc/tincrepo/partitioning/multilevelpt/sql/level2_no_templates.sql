-- @author raghav
-- @created 2015-05-18 12:00:00
-- @tags ORCA
-- @db_name gptest
-- @gpopt 1.577
-- @product_version hawq: [2.0-], gpdb: [4.3.5-]
-- @gucs optimizer_multilevel_partitioning=on
-- @skip "In Orca, we are currently redesigning this functionality"
-- @optimizer_mode on
-- @description Testing the fallback of queries on a multi-level partitioned table with no templates

-- start_ignore
DROP TABLE IF EXISTS sales4;
set gp_enable_hash_partitioned_tables = true;
CREATE TABLE sales4 (id int, country text, state text, year date)
DISTRIBUTED BY (id)
PARTITION BY LIST (country)
subpartition by range (year)
subpartition by list (state)
subpartition template
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
)
(
 PARTITION usa VALUES ('usa') 
 (
	subpartition jan01 start (date '2001-01-01'),
	subpartition jan02 start (date '2002-01-01')
 ),
 PARTITION europe VALUES ('europe')
(
subpartition jan01 start (date '2001-01-01'),
subpartition jan02 start (date '2002-01-01')
)
,
 PARTITION asia VALUES ('asia')
(
subpartition jan01 start (date '2001-01-01'),
subpartition jan02 start (date '2002-01-01')
))
;

set client_min_messages='log';
select * from sales4;
set client_min_messages='notice';

DROP TABLE sales4;
-- end_ignore

\!grep Planner %MYD%/output/level2_no_templates_orca.out|wc -l
