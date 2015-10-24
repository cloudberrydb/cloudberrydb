-- gp_toolkit
--
-- NOTE: currently in a very initial version. only checks of tables are defined in template1:pg_class.
--       needs to be extended to something more meaningful.

\c template1
select relname from pg_class where relnamespace in (select oid from pg_namespace where nspname = 'gp_toolkit') order by relname;
\c regression
