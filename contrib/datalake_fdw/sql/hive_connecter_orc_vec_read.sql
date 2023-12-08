-- create datalake_fdw
-- clean table
DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER foreign_server;
DROP SERVER IF EXISTS foreign_server;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw;

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

SELECT pg_sleep(5);

CREATE EXTENSION IF NOT EXISTS hive_connector;

SELECT pg_sleep(5);

create extension vectorization;
set vector.enable_vectorization=on;
SELECT pg_sleep(5);

CREATE FOREIGN DATA WRAPPER datalake_fdw
HANDLER datalake_fdw_handler
VALIDATOR datalake_fdw_validator
OPTIONS (mpp_execute 'all segments');

SELECT public.create_foreign_server('foreign_server', 'gpadmin', 'datalake_fdw', 'paa_cluster');

-- hive partition tinyint
DROP FOREIGN TABLE IF EXISTS hive_type_test_1;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_1', 'paa_cluster', 'hive_type_test_1', 'foreign_server');
select * from hive_type_test_1 order by id, name, m;
select count(*) from hive_type_test_1;

-- hive partition smallint
DROP FOREIGN TABLE IF EXISTS hive_type_test_2;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_2', 'paa_cluster', 'hive_type_test_2', 'foreign_server');
select * from hive_type_test_2 order by id, name, m;
select count(*) from hive_type_test_2;

-- hive partition int
DROP FOREIGN TABLE IF EXISTS hive_type_test_3;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_3', 'paa_cluster', 'hive_type_test_3', 'foreign_server');
select * from hive_type_test_3 order by id, name, m;
select count(*) from hive_type_test_3;

-- hive partition bigint
DROP FOREIGN TABLE IF EXISTS hive_type_test_4;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_4', 'paa_cluster', 'hive_type_test_4', 'foreign_server');
select * from hive_type_test_4 order by id, name, m;
select count(*) from hive_type_test_4;

-- hive partition float
DROP FOREIGN TABLE IF EXISTS hive_type_test_5;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_5', 'paa_cluster', 'hive_type_test_5', 'foreign_server');
select * from hive_type_test_5 order by id, name, m;
select count(*) from hive_type_test_5;

-- hive partition double
DROP FOREIGN TABLE IF EXISTS hive_type_test_6;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_6', 'paa_cluster', 'hive_type_test_6', 'foreign_server');
select * from hive_type_test_6 order by id, name, m;
select count(*) from hive_type_test_6;

-- hive partition string
DROP FOREIGN TABLE IF EXISTS hive_type_test_7;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_7', 'paa_cluster', 'hive_type_test_7', 'foreign_server');
select * from hive_type_test_7 order by id, name, m;
select count(*) from hive_type_test_7;

-- hive partition date
DROP FOREIGN TABLE IF EXISTS hive_type_test_8;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_8', 'paa_cluster', 'hive_type_test_8', 'foreign_server');
select * from hive_type_test_8 order by id, name, m;
select count(*) from hive_type_test_8;

-- hive partition varchar
DROP FOREIGN TABLE IF EXISTS hive_type_test_9;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_9', 'paa_cluster', 'hive_type_test_9', 'foreign_server');
select * from hive_type_test_9 order by id, name, m;
select count(*) from hive_type_test_9;

-- hive partition decimal
DROP FOREIGN TABLE IF EXISTS hive_type_test_10;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_type_test_10', 'paa_cluster', 'hive_type_test_10', 'foreign_server');
select * from hive_type_test_10 order by id, name, m;
select count(*) from hive_type_test_10;

-- hive simple table
DROP FOREIGN TABLE IF EXISTS hive_test_1;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_test_1', 'paa_cluster', 'hive_test_1', 'foreign_server');
select * from hive_test_1 order by id;
select * from hive_test_1 where id=1 order by id;
select count(*) from hive_test_1 where id=1;
select count(*) from hive_test_1;

-- hive partiton key is int type
DROP FOREIGN TABLE IF EXISTS hive_test_3;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_test_3', 'paa_cluster', 'hive_test_3', 'foreign_server');
select * from hive_test_3 order by id;
select * from hive_test_3 where m=1 order by id;
select count(*) from hive_test_3;
select count(*) from hive_test_3 where m=2;
select count(*) from hive_test_3 where m=3;

-- hive partiton key is int and char type
DROP FOREIGN TABLE IF EXISTS hive_test_4;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_test_4', 'paa_cluster', 'hive_test_4', 'foreign_server');
select * from hive_test_4 order by id, name, m, n;
select count(*) from hive_test_4;
select * from hive_test_4 where m=2 order by id, name, m, n;
select * from hive_test_4 where n='cc' order by id, name, m, n;
select n from hive_test_4 order by n;
select name,m from hive_test_4 order by name, m;
select m,n from hive_test_4 order by m, n;

-- hive partiton key is int and char and date type
DROP FOREIGN TABLE IF EXISTS hive_test_5;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_test_5', 'paa_cluster', 'hive_test_5', 'foreign_server');
select * from hive_test_5 order by id, name, m, n, o;
select count(*) from hive_test_5;

-- hive partiton key is int and string and float type
DROP FOREIGN TABLE IF EXISTS hive_test_6;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_test_6', 'paa_cluster', 'hive_test_6', 'foreign_server');
select * from hive_test_6 order by id, name, m, n, o;
select count(*) from hive_test_6;

-- hive partiton key is int and string and decimal and tinyint type
DROP FOREIGN TABLE IF EXISTS hive_test_7;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_test_7', 'paa_cluster', 'hive_test_7', 'foreign_server');
select * from hive_test_7 order by id, name, m, n, o, p;
select count(*) from hive_test_7;

-- hive partiton key is int and string and decimal and tinyint type
DROP FOREIGN TABLE IF EXISTS hive_test_8;
select sync_hive_table('hive_cluster', 'hive_orc_load_data_test', 'hive_test_8', 'paa_cluster', 'hive_test_8', 'foreign_server');
select * from hive_test_8 order by id, name, m, n, o, p, q, s;
select count(*) from hive_test_8;

DROP FOREIGN TABLE IF EXISTS hive_type_test_1;
DROP FOREIGN TABLE IF EXISTS hive_type_test_2;
DROP FOREIGN TABLE IF EXISTS hive_type_test_3;
DROP FOREIGN TABLE IF EXISTS hive_type_test_4;
DROP FOREIGN TABLE IF EXISTS hive_type_test_5;
DROP FOREIGN TABLE IF EXISTS hive_type_test_6;
DROP FOREIGN TABLE IF EXISTS hive_type_test_7;
DROP FOREIGN TABLE IF EXISTS hive_type_test_8;
DROP FOREIGN TABLE IF EXISTS hive_type_test_9;
DROP FOREIGN TABLE IF EXISTS hive_type_test_10;
DROP FOREIGN TABLE IF EXISTS hive_test_1;
DROP FOREIGN TABLE IF EXISTS hive_test_3;
DROP FOREIGN TABLE IF EXISTS hive_test_4;
DROP FOREIGN TABLE IF EXISTS hive_test_5;
DROP FOREIGN TABLE IF EXISTS hive_test_6;
DROP FOREIGN TABLE IF EXISTS hive_test_7;
DROP FOREIGN TABLE IF EXISTS hive_test_8;
