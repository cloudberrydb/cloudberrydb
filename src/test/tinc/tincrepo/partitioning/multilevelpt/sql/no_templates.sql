-- @author raghav
-- @created 2015-05-18 12:00:00
-- @tags ORCA
-- @db_name gptest
-- @gpopt 1.577
-- @product_version gpdb: [4.3.5-]
-- @skip "In Orca, we are currently redesigning this functionality"
-- @gucs optimizer_multilevel_partitioning=on
-- @optimizer_mode on
-- @description Testing the fallback of queries on a multi-level partitioned table with no templates

-- start_ignore
DROP TABLE IF EXISTS rank2;

CREATE TABLE rank2 (id int, rank int,
year date, gender char(1),
usstate char(2))
DISTRIBUTED BY (id, gender, year, usstate)
partition by list (gender)
subpartition by range (year),
subpartition by list (usstate)
(
  partition boys values ('M') 
(
subpartition jan01 start (date '2001-01-01') 
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
),
subpartition jan02 start (date '2002-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
)
)
,
  partition girls values ('F')
(
subpartition jan01 start (date '2001-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA')
),
subpartition jan02 start (date '2002-01-01')
(
subpartition mass values ('MA'),
subpartition cali values ('CA'),
subpartition ohio values ('OH')
)
)
);

set client_min_messages='log';
explain select * from rank2;
set client_min_messages='notice';

DROP TABLE rank2;
-- end_ignore
\!grep Planner %MYD%/output/no_templates_orca.out|wc -l
