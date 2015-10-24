CREATE TABLE PG_STAT_LAST_OPERATION_TEST (foo int) DISTRIBUTED BY (foo);

SELECT classname, objname, schemaname, actionname, subtype 
FROM pg_stat_operations WHERE schemaname = 'public' 
AND objname = 'pg_stat_last_operation_test'
AND actionname = 'CREATE';

SELECT classname, objname, schemaname, actionname, subtype 
FROM pg_stat_operations WHERE schemaname = 'public' 
AND objname = 'pg_stat_last_operation_test'
AND actionname = 'TRUNCATE';

insert into PG_STAT_LAST_OPERATION_TEST select generate_series(1,100);

truncate PG_STAT_LAST_OPERATION_TEST;

SELECT classname, objname, schemaname, actionname, subtype 
FROM pg_stat_operations WHERE schemaname = 'public' 
AND objname = 'pg_stat_last_operation_test'
AND actionname = 'TRUNCATE';
