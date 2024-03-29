DROP SCHEMA IF EXISTS synctab CASCADE;
DROP SCHEMA IF EXISTS syncdb CASCADE;

CREATE SCHEMA synctab;
CREATE SCHEMA syncdb;

DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER hive_server;
DROP SERVER IF EXISTS hive_server;

set vector.enable_vectorization=off;
SET datestyle = ISO, MDY;

SELECT public.create_foreign_server('hive_server', 'gpadmin', 'datalake_fdw', 'paa_cluster');

SELECT public.sync_hive_table('hive_cluster','mytestdb','text_default','paa_cluster', 'synctab.text_default', 'hive_server');

SELECT * FROM synctab.text_default order by a, b, c, d;

SELECT public.sync_hive_table('hive_cluster','mytestdb','text_custom','paa_cluster', 'synctab.text_custom', 'hive_server');

SELECT * FROM synctab.text_custom order by a, b, c, d;

SELECT public.sync_hive_table('hive_cluster','mytestdb','csv_default','paa_cluster', 'synctab.csv_default', 'hive_server');

SELECT * FROM synctab.csv_default order by a, b, c, d;

SELECT public.sync_hive_table('hive_cluster','mytestdb','csv_custom','paa_cluster', 'synctab.csv_custom', 'hive_server');

SELECT * FROM synctab.csv_custom order by a, b, c, d;

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

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_avro','paa_cluster', 'synctab.empty_avro', 'hive_server');

SELECT * FROM synctab.empty_avro;

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_avro_partition','paa_cluster', 'synctab.empty_avro_partition', 'hive_server');

SELECT * FROM synctab.empty_avro_partition;

SELECT public.sync_hive_database('hive_cluster', 'mytestdb', 'paa_cluster', 'syncdb', 'hive_server');

SELECT * FROM syncdb.text_default order by a, b, c, d;

SELECT * FROM syncdb.text_custom order by a, b, c, d;

SELECT * FROM syncdb.csv_default order by a, b, c, d;

SELECT * FROM syncdb.csv_custom order by a, b, c, d;

SELECT * FROM syncdb.empty_orc_transactional;

SELECT * FROM syncdb.empty_orc;

SELECT * FROM syncdb.empty_parquet;

SELECT * FROM syncdb.empty_orc_partition;

SELECT * FROM syncdb.empty_parquet_partition;

SELECT * FROM syncdb.empty_avro;

SELECT * FROM syncdb.empty_avro_partition;

DROP SCHEMA IF EXISTS synctab CASCADE;
DROP SCHEMA IF EXISTS syncdb CASCADE;
DROP SERVER hive_server CASCADE;