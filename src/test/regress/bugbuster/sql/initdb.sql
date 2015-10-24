set optimizer_disable_missing_stats_collection = on;


\echo -- start_ignore
select * from gp_version_at_initdb;
\echo -- end_ignore






\echo -- start_ignore
select * from gp_id;
\echo -- end_ignore






\echo -- start_ignore
select * from gp_pgdatabase;
\echo -- end_ignore





\echo -- start_ignore
select content,definedprimary,dbid,isprimary from gp_configuration;
\echo -- end_ignore






\echo -- start_ignore
select * from pg_conversion;
\echo -- end_ignore




\echo -- start_ignore
select * from pg_authid;
\echo -- end_ignore
 



\echo -- start_ignore
select * from pg_autovacuum;
\echo -- end_ignore


