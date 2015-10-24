\echo -- start_ignore
-- ignore all outputs
--
\c tpch_heap

select version();
\! pg_dump --version
\! pg_restore --version

\! rm -fr ./cdbfast_pgbkup
\! mkdir ./cdbfast_pgbkup

\! pg_dump -Fc -f `pwd`/cdbfast_pgbkup/pgbkup tpch_heap

drop database if exists gptest_pgrestore;
create database gptest_pgrestore;

\! pg_restore -d gptest_pgrestore `pwd`/cdbfast_pgbkup/pgbkup

\! rm -fr ./cdbfast_pgbkup
\echo -- end_ignore
\c gptest_pgrestore

select count(*), sum(n_nationkey), min(n_nationkey), max(n_nationkey) from nation;
select count(*), sum(r_regionkey), min(r_regionkey), max(r_regionkey) from region;
select count(*), sum(p_partkey), min(p_partkey), max(p_partkey) from part;
select count(*), sum(s_suppkey), min(s_suppkey), max(s_suppkey) from supplier;
select count(*), sum(ps_partkey + ps_suppkey), min(ps_partkey + ps_suppkey), max(ps_partkey + ps_suppkey) from partsupp;
select count(*), sum(c_custkey), min(c_custkey), max(c_custkey) from customer;
select count(*), sum(o_orderkey), min(o_orderkey), max(o_orderkey) from orders;
select count(*), sum(l_linenumber), min(l_linenumber), max(l_linenumber) from lineitem;

\echo -- start_ignore
-- ignore all outputs
--
\! rm -fr ./cdbfast_pgbkup
\! mkdir ./cdbfast_pgbkup
-- Test Options to boost up code coverage for pg_dump

\! pg_dump --format=t -f `pwd`/cdbfast_pgbkup/pg_dumpT tpch_heap
\! pg_dump --format=c -f `pwd`/cdbfast_pgbkup/pg_dumpC.tar tpch_heap
\! pg_dump --format=p -f `pwd`/cdbfast_pgbkup/pg_dumpP tpch_heap

\! pg_dump --format=c --compress=9 -f `pwd`/cdbfast_pgbkup/pg_dumpCZ.tar regression
\! rm -fr ./cdbfast_pgbkup

\! pg_restore --help
\! pg_dump --help
\echo -- end_ignore
