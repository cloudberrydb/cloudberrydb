DROP SCHEMA IF EXISTS synctab CASCADE;
DROP SCHEMA IF EXISTS syncdb CASCADE;

CREATE SCHEMA synctab;
CREATE SCHEMA syncdb;

DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER hive_server;
DROP SERVER IF EXISTS hive_server;

set vector.enable_vectorization=off;
SET datestyle = ISO, MDY;

SELECT public.create_foreign_server('hive_server', 'gpadmin', 'datalake_fdw', 'paa_cluster');

SELECT public.sync_hive_table('hive_cluster','mytestdb','test_table1','paa_cluster', 'synctab.test_table1', 'hive_server');

SELECT count(*) FROM synctab.test_table1;

SELECT public.sync_hive_table('hive_cluster','mytestdb','transactional_orc','paa_cluster', 'synctab.transactional_orc', 'hive_server');

SELECT * FROM synctab.transactional_orc;

SELECT public.sync_hive_table('hive_cluster','mytestdb','normal_orc','paa_cluster', 'synctab.normal_orc', 'hive_server');

SELECT * FROM synctab.normal_orc;

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_orc_1','paa_cluster', 'synctab.partition_orc_1', 'hive_server');

SELECT * FROM synctab.partition_orc_1;

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_orc_2','paa_cluster', 'synctab.partition_orc_2', 'hive_server');

SELECT * FROM synctab.partition_orc_2;

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_orc_3','paa_cluster', 'synctab.partition_orc_3', 'hive_server');

SELECT * FROM synctab.partition_orc_3;

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_orc_4','paa_cluster', 'synctab.partition_orc_4', 'hive_server');

SELECT count(*) FROM synctab.partition_orc_4;

SELECT public.sync_hive_table('hive_cluster','mytestdb','normal_parquet','paa_cluster', 'synctab.normal_parquet', 'hive_server');

SELECT * FROM synctab.normal_parquet;

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_parquet_1','paa_cluster', 'synctab.partition_parquet_1', 'hive_server');

SELECT * FROM synctab.partition_parquet_1;

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_parquet_2','paa_cluster', 'synctab.partition_parquet_2', 'hive_server');

SELECT * FROM synctab.partition_parquet_2;

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_parquet_3','paa_cluster', 'synctab.partition_parquet_3', 'hive_server');

SELECT * FROM synctab.partition_parquet_3;

SELECT public.sync_hive_table('hive_cluster','mytestdb','text_default','paa_cluster', 'synctab.text_default', 'hive_server');

SELECT * FROM synctab.text_default;

SELECT public.sync_hive_table('hive_cluster','mytestdb','text_custom','paa_cluster', 'synctab.text_custom', 'hive_server');

SELECT * FROM synctab.text_custom;

SELECT public.sync_hive_table('hive_cluster','mytestdb','csv_default','paa_cluster', 'synctab.csv_default', 'hive_server');

SELECT * FROM synctab.csv_default;

SELECT public.sync_hive_table('hive_cluster','mytestdb','csv_custom','paa_cluster', 'synctab.csv_custom', 'hive_server');

SELECT * FROM synctab.csv_custom;

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_orc_transactional','paa_cluster', 'synctab.empty_orc_transactional', 'hive_server');

SELECT * FROM synctab.empty_orc_transactional;

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_orc','paa_cluster', 'synctab.empty_orc', 'hive_server');

SELECT * FROM synctab.empty_orc;

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_parquet','paa_cluster', 'synctab.empty_parquet', 'hive_server');

SELECT * FROM synctab.empty_parquet;

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_orc_partition','paa_cluster', 'synctab.empty_orc_partition', 'hive_server');

SELECT * FROM synctab.empty_orc_partition;

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_parquet_partition','paa_cluster', 'synctab.empty_parquet_partition', 'hive_server');

SELECT * FROM synctab.empty_parquet_partition;

SELECT public.sync_hive_database('hive_cluster', 'mytestdb', 'paa_cluster', 'syncdb', 'hive_server');

SELECT count(*) FROM synctab.test_table1;


SELECT * FROM synctab.transactional_orc;


SELECT * FROM synctab.normal_orc;


SELECT * FROM synctab.partition_orc_1;


SELECT * FROM synctab.partition_orc_2;


SELECT * FROM synctab.partition_orc_3;


SELECT count(*) FROM synctab.partition_orc_4;


SELECT * FROM synctab.normal_parquet;


SELECT * FROM synctab.partition_parquet_1;


SELECT * FROM synctab.partition_parquet_2;


SELECT * FROM synctab.partition_parquet_3;


SELECT * FROM synctab.text_default;


SELECT * FROM synctab.text_custom;


SELECT * FROM synctab.csv_default;


SELECT * FROM synctab.csv_custom;


SELECT * FROM synctab.empty_orc_transactional;


SELECT * FROM synctab.empty_orc;


SELECT * FROM synctab.empty_parquet;


SELECT * FROM synctab.empty_orc_partition;


SELECT * FROM synctab.empty_parquet_partition;

