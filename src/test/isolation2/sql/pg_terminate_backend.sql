create table foo as select i a, i b from generate_series(1, 10) i;

-- expect this query terminated by 'test pg_terminate_backend'
1&:create temp table t as select count(*) from foo where pg_sleep(20) is null;

-- extract the pid for the previous query
SELECT pg_terminate_backend(pid,'test pg_terminate_backend')
FROM pg_stat_activity WHERE query like 'create temp table t as select%' ORDER BY pid LIMIT 1;

-- EXPECT: session 1 terminated with 'test pg_terminate_backend'
1<:

-- query backend to ensure no PANIC on postmaster
select count(*) from foo;

--
-- SIGSEGV issue when freeing gangs
--

CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

DROP TABLE IF EXISTS foo;
CREATE TABLE foo (c1 int, c2 int) DISTRIBUTED BY (c1);

10: BEGIN;

SELECT gp_inject_fault('create_gang_in_progress', 'reset', 1);
SELECT gp_inject_fault('create_gang_in_progress', 'suspend', 1);

10&: SELECT * FROM foo a JOIN foo b USING (c2);

SELECT pg_terminate_backend(pid) FROM pg_stat_activity
 WHERE query = 'SELECT * FROM foo a JOIN foo b USING (c2);';

SELECT gp_inject_fault('create_gang_in_progress', 'resume', 1);

10<:
10q
