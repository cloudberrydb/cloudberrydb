create schema gpexplain;
set search_path = gpexplain;

-- Helper function, to return the EXPLAIN output of a query as a normal
-- result set, so that you can manipulate it further.
create or replace function get_explain_output(explain_query text) returns setof text as
$$
declare
  explainrow text;
begin
  for explainrow in execute 'EXPLAIN ' || explain_query
  loop
    return next explainrow;
  end loop;
end;
$$ language plpgsql;

-- Same, for EXPLAIN ANALYZE VERBOSE
create or replace function get_explain_analyze_output(explain_query text) returns setof text as
$$
declare
  explainrow text;
begin
  for explainrow in execute 'EXPLAIN (ANALYZE, VERBOSE) ' || explain_query
  loop
    return next explainrow;
  end loop;
end;
$$ language plpgsql;

-- Same, for EXPLAIN ANALYZE WAL
create or replace function get_explain_analyze_wal_output(explain_query text) returns setof text as
$$
declare
  explainrow text;
begin
  for explainrow in execute 'EXPLAIN (ANALYZE, WAL) ' || explain_query
  loop
    return next explainrow;
  end loop;
end;
$$ language plpgsql;

-- Test explain wal option
CREATE TABLE explainwal (id int4);

WITH query_plan (et) AS
(
  select get_explain_analyze_wal_output($$
    INSERT INTO explainwal SELECT generate_series(1, 10);
  $$)
),
count_result AS
(
  SELECT COUNT(*) FROM query_plan WHERE et LIKE '%WAL%'
)
select
  (SELECT COUNT(*) FROM count_result WHERE count > 0) as wal_reserved_lines;

WITH query_plan (et) AS
(
  select get_explain_analyze_wal_output($$
    UPDATE explainwal SET id=11 WHERE id=10;
  $$)
),
count_result AS
(
  SELECT COUNT(*) FROM query_plan WHERE et LIKE '%WAL%'
)
select
  (SELECT COUNT(*) FROM count_result WHERE count > 0) as wal_reserved_lines;

WITH query_plan (et) AS
(
  select get_explain_analyze_wal_output($$
    DELETE FROM explainwal
  $$)
),
count_result AS
(
  SELECT COUNT(*) FROM query_plan WHERE et LIKE '%WAL%'
)
select
  (SELECT COUNT(*) FROM count_result WHERE count > 0) as wal_reserved_lines;

--
-- Test explain_memory_verbosity option
-- 
CREATE TABLE explaintest (id int4);
INSERT INTO explaintest SELECT generate_series(1, 10);
ANALYZE explaintest;

EXPLAIN ANALYZE SELECT * FROM explaintest;

set explain_memory_verbosity='summary';

-- The plan should include the slice table with two slices, with a
-- "Vmem reserved: ..." line on both lines.
WITH query_plan (et) AS
(
  select get_explain_analyze_output($$
    SELECT * FROM explaintest;
  $$)
)
SELECT
  (SELECT COUNT(*) FROM query_plan WHERE et like '%Vmem reserved: %') as vmem_reserved_lines,
  (SELECT COUNT(*) FROM query_plan WHERE et like '%Executor Memory: %') as executor_memory_lines
;

-- With 'detail' level, should have an Executor Memory on each executor node.
set explain_memory_verbosity='detail';
WITH query_plan (et) AS
(
  select get_explain_analyze_output($$
    SELECT * FROM explaintest;
  $$)
)
SELECT
  (SELECT COUNT(*) FROM query_plan WHERE et like '%Vmem reserved: %') as vmem_reserved_lines,
  (SELECT COUNT(*) FROM query_plan WHERE et like '%Executor Memory: %') as executor_memory_lines
;

reset explain_memory_verbosity;

EXPLAIN ANALYZE SELECT id FROM 
( SELECT id 
	FROM explaintest
	WHERE id > (
		SELECT avg(id)
		FROM explaintest
	)
) as foo
ORDER BY id
LIMIT 1;


-- Verify that the column references are OK. This tests for an old ORCA bug,
-- where the Filter clause in the IndexScan of this query was incorrectly
-- printed as something like:
--
--   Filter: "outer".column2 = mpp22263.*::text

CREATE TABLE mpp22263 (
        unique1         int4,
        unique2         int4,
        two                     int4,
        four            int4,
        ten                     int4,
        twenty          int4,
        hundred         int4,
        thousand        int4,
        twothousand     int4,
        fivethous       int4,
        tenthous        int4,
        odd                     int4,
        even            int4,
        stringu1        name    COLLATE pg_catalog."default",
        stringu2        name    COLLATE pg_catalog."default",
        string4         name    COLLATE pg_catalog."default"
) distributed by (unique1);

create index mpp22263_idx1 on mpp22263 using btree(unique1);
explain select * from mpp22263, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
WHERE mpp22263.unique1 = v.i and mpp22263.stringu1 = v.j;

-- atmsort.pm masks out differences in the Filter line, so just memorizing
-- the output of the above EXPLAIN isn't enough to catch a faulty Filter line.
-- Extract the Filter explicitly.
SELECT * from
  get_explain_output($$
select * from mpp22263, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
WHERE mpp22263.unique1 = v.i and mpp22263.stringu1 = v.j;
  $$) as et
WHERE et like '%Filter: %';

--
-- Join condition in explain plan should represent constants with proper
-- variable name
--
create table foo (a int) distributed randomly;
-- "outer", "inner" prefix must also be prefixed to variable name as length of rtable > 1
SELECT trim(et) et from
get_explain_output($$ 
	select * from (values (1),(2)) as f(a) join (values(1),(2)) b(b) on a = b join foo on true join foo as foo2 on true $$) as et
WHERE et like '%Join Filter:%' or et like '%Hash Cond:%';

SELECT trim(et) et from
get_explain_output($$
	select * from (values (1),(2)) as f(a) join (values(1),(2)) b(b) on a = b$$) as et
WHERE et like '%Hash Cond:%';

--
-- Test EXPLAINing of the Partition By in a window function. (PostgreSQL
-- doesn't print it at all.)
--
explain (costs off) select count(*) over (partition by g) from generate_series(1, 10) g;


--
-- Test non-text format with a few queries that contain GPDB-specific node types.
--

-- The default init_file rules contain a line to mask this out in normal
-- text-format EXPLAIN output, but it doesn't catch these alternative formats.
-- start_matchignore
-- m/Optimizer.*Pivotal Optimizer \(GPORCA\)/
-- end_matchignore

CREATE EXTERNAL WEB TABLE dummy_ext_tab (x text) EXECUTE 'echo foo' FORMAT 'text';

-- External Table Scan
explain (format json, costs off) SELECT * FROM dummy_ext_tab;

-- Seq Scan on an append-only table
CREATE TEMP TABLE dummy_aotab (x int4) WITH (appendonly=true);
explain (format yaml, costs off) SELECT * FROM dummy_aotab;

-- DML node (with ORCA)
explain (format xml, costs off) insert into dummy_aotab values (1);

-- github issues 5795. explain fails previously.
--start_ignore
explain SELECT * from information_schema.key_column_usage;
--end_ignore

-- github issue 5794.
set gp_enable_explain_allstat=on;
explain analyze SELECT * FROM explaintest;
set gp_enable_explain_allstat=DEFAULT;

-- Test explain rows out.
set gp_enable_explain_rows_out=on;
explain analyze SELECT * FROM explaintest;
set gp_enable_explain_rows_out=DEFAULT;


--
-- Test GPDB-specific EXPLAIN (SLICETABLE) option.
--
explain (slicetable, costs off) SELECT * FROM explaintest;

-- same in JSON format
explain (slicetable, costs off, format json) SELECT * FROM explaintest;

--
-- The same slice may have different number of plan nodes on every qExec.
-- Check if explain analyze can work in that case
--
create schema explain_subplan;
set search_path = explain_subplan;
CREATE TABLE mintab(c1 int);
INSERT into mintab VALUES (120);

CREATE TABLE range_parted (
	a text,
	b bigint,
	c numeric
) PARTITION BY RANGE (a, b);

CREATE TABLE part_a_1_a_10 PARTITION OF range_parted FOR VALUES FROM ('a', 1) TO ('a', 10);
CREATE TABLE part_a_10_a_20 PARTITION OF range_parted FOR VALUES FROM ('a', 10) TO ('a', 20);
CREATE TABLE part_b_1_b_10 PARTITION OF range_parted FOR VALUES FROM ('b', 1) TO ('b', 10);
CREATE TABLE part_b_10_b_20 PARTITION OF range_parted FOR VALUES FROM ('b', 10) TO ('b', 20);
ALTER TABLE range_parted ENABLE ROW LEVEL SECURITY;
INSERT INTO range_parted VALUES ('a', 1, 1), ('a', 12, 200);

CREATE USER regress_range_parted_user;
GRANT ALL ON SCHEMA explain_subplan TO regress_range_parted_user;
GRANT ALL ON range_parted, mintab TO regress_range_parted_user;

CREATE POLICY seeall ON range_parted AS PERMISSIVE FOR SELECT USING (true);
CREATE POLICY policy_range_parted ON range_parted for UPDATE USING (true) WITH CHECK (c % 2 = 0);

CREATE POLICY policy_range_parted_subplan on range_parted
AS RESTRICTIVE for UPDATE USING (true)
WITH CHECK ((SELECT range_parted.c <= c1 FROM mintab));

SET SESSION AUTHORIZATION regress_range_parted_user;
EXPLAIN (analyze,  costs off, timing off, summary off) UPDATE explain_subplan.range_parted set a = 'b', c = 120 WHERE a = 'a' AND c = '200';
RESET SESSION AUTHORIZATION;

DROP POLICY seeall ON range_parted;
DROP POLICY policy_range_parted ON range_parted;
DROP POLICY policy_range_parted_subplan ON range_parted;
DROP TABLE mintab;
DROP TABLE range_parted;
RESET search_path;
DROP SCHEMA explain_subplan;
DROP USER regress_range_parted_user;
-- Test if explain analyze will hang with materialize node
CREATE TABLE recursive_table_ic (a INT) DISTRIBUTED BY (a);
INSERT INTO recursive_table_ic SELECT * FROM generate_series(20, 30000);

explain (analyze, costs off, timing off, summary off) WITH RECURSIVE
r(i) AS (
	SELECT 1
),
y(i) AS (
	SELECT 1
	UNION ALL
	SELECT i + 1 FROM y, recursive_table_ic WHERE NOT EXISTS (SELECT * FROM r LIMIT 10)
)
SELECT * FROM y LIMIT 10;
DROP TABLE recursive_table_ic;
