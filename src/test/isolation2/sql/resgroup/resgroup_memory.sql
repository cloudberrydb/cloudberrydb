-- start_ignore
DROP ROLE IF EXISTS role1_memory_test;
DROP RESOURCE GROUP rg1_memory_test;
DROP ROLE IF EXISTS role2_memory_test;
DROP RESOURCE GROUP rg2_memory_test;
-- end_ignore

CREATE OR REPLACE FUNCTION hold_memory(int, int) RETURNS int AS $$
    SELECT count(pg_sleep(5))::int FROM length(repeat(repeat($1::char, 1 << 10), $2 << 10))
$$ LANGUAGE sql;

CREATE OR REPLACE VIEW eat_memory_on_qd AS
	SELECT hold_memory(0,100);

CREATE OR REPLACE VIEW eat_memory_on_one_slice AS
	SELECT count(*)
	FROM
	gp_dist_random('gp_id') t1
	WHERE hold_memory(t1.dbid,100)=0
	;

CREATE OR REPLACE VIEW eat_memory_on_slices AS
	SELECT count(*)
	FROM
	gp_dist_random('gp_id') t1,
	gp_dist_random('gp_id') t2
	WHERE hold_memory(t1.dbid,100)=0
	  AND hold_memory(t2.dbid,100)=0
	;

CREATE VIEW memory_usage AS
	SELECT g.rsgname, s.value::json->'-1' AS master_mem,
	s.value::json->'0' AS seg0_mem,
	s.value::json->'1' AS seg1_mem,
	s.value::json->'2' AS seg2_mem
	FROM pg_resgroup_get_status_kv('memory_usage') s, pg_resgroup g
	WHERE s.rsgid=g.oid;

CREATE FUNCTION round_test(text, integer) RETURNS text AS $$
	SELECT (round($1::integer / $2) * $2)::text
$$ LANGUAGE sql;

CREATE VIEW memory_result AS
	SELECT rsgname, round_test(master_mem::text, 10) AS master_mem,
	round_test(seg0_mem::text, 10) AS seg0_mem,
	round_test(seg1_mem::text, 10) AS seg1_mem,
	round_test(seg2_mem::text, 10) AS seg2_mem
	FROM memory_usage WHERE rsgname='rg1_memory_test' OR rsgname='rg2_memory_test';

CREATE RESOURCE GROUP rg1_memory_test WITH (cpu_rate_limit=0.1, memory_limit=0.1);
CREATE ROLE role1_memory_test RESOURCE GROUP rg1_memory_test;
CREATE RESOURCE GROUP rg2_memory_test WITH (cpu_rate_limit=0.1, memory_limit=0.1);
CREATE ROLE role2_memory_test RESOURCE GROUP rg2_memory_test;

GRANT ALL ON eat_memory_on_qd TO role1_memory_test;
GRANT ALL ON eat_memory_on_one_slice TO role1_memory_test;
GRANT ALL ON eat_memory_on_slices TO role1_memory_test;

GRANT ALL ON eat_memory_on_qd TO role2_memory_test;
GRANT ALL ON eat_memory_on_one_slice TO role2_memory_test;
GRANT ALL ON eat_memory_on_slices TO role2_memory_test;

1: SET ROLE TO role1_memory_test;

-- check initial state
SELECT pg_sleep(1);
SELECT * FROM memory_result;

-- QD only
1&: SELECT * FROM eat_memory_on_qd;
SELECT pg_sleep(2);
SELECT * FROM memory_result;
1<:

1: SET ROLE TO role2_memory_test;

-- QEs on one slice
1&: SELECT * FROM eat_memory_on_one_slice;
SELECT pg_sleep(2);
SELECT * FROM memory_result;
1<:

1: SET ROLE TO role1_memory_test;

-- QEs on multiple slices
1&: SELECT * FROM eat_memory_on_slices;
SELECT pg_sleep(2);
SELECT * FROM memory_result;
1<:

-- recheck after cleanup
SELECT pg_sleep(2);
SELECT * FROM memory_result;

-- concurrency test
1: SET ROLE TO role1_memory_test;
2: SET ROLE TO role2_memory_test;

-- QEs on multiple slices
1&: SELECT * FROM eat_memory_on_slices;
2&: SELECT * FROM eat_memory_on_slices;
SELECT pg_sleep(2);
SELECT * FROM memory_result;
1<:
2<:

-- recheck after cleanup
SELECT pg_sleep(2);
SELECT * FROM memory_result;

1: SET ROLE TO role1_memory_test;
2: SET ROLE TO role1_memory_test;

-- QEs on multiple slices
1&: SELECT * FROM eat_memory_on_slices;
2&: SELECT * FROM eat_memory_on_slices;
SELECT pg_sleep(2);
SELECT * FROM memory_result;
1<:
2<:

-- recheck after cleanup
SELECT pg_sleep(2);
SELECT * FROM memory_result;

-- cleanup

1: RESET ROLE;
2: RESET ROLE;

REVOKE ALL ON eat_memory_on_qd FROM role1_memory_test;
REVOKE ALL ON eat_memory_on_one_slice FROM role1_memory_test;
REVOKE ALL ON eat_memory_on_slices FROM role1_memory_test;

REVOKE ALL ON eat_memory_on_qd FROM role2_memory_test;
REVOKE ALL ON eat_memory_on_one_slice FROM role2_memory_test;
REVOKE ALL ON eat_memory_on_slices FROM role2_memory_test;

ALTER ROLE role1_memory_test RESOURCE GROUP none;
ALTER ROLE role2_memory_test RESOURCE GROUP none;

DROP ROLE role1_memory_test;
DROP ROLE role2_memory_test;
DROP RESOURCE GROUP rg1_memory_test;
DROP RESOURCE GROUP rg2_memory_test;
