SELECT public.sync_hive_table('hive_cluster','mytestdb','test_table1','paa_cluster', 'synctab.test_table1', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','transactional_orc','paa_cluster', 'synctab.transactional_orc', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','normal_orc','paa_cluster', 'synctab.normal_orc', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_orc_1','paa_cluster', 'synctab.partition_orc_1', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_orc_2','paa_cluster', 'synctab.partition_orc_2', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_orc_3','paa_cluster', 'synctab.partition_orc_3', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_orc_4','paa_cluster', 'synctab.partition_orc_4', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','normal_parquet','paa_cluster', 'synctab.normal_parquet', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_parquet_1','paa_cluster', 'synctab.partition_parquet_1', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_parquet_2','paa_cluster', 'synctab.partition_parquet_2', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','partition_parquet_3','paa_cluster', 'synctab.partition_parquet_3', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','text_default','paa_cluster', 'synctab.text_default', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','text_custom','paa_cluster', 'synctab.text_custom', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','csv_default','paa_cluster', 'synctab.csv_default', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','csv_custom','paa_cluster', 'synctab.csv_custom', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_orc_transactional','paa_cluster', 'synctab.empty_orc_transactional', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_orc','paa_cluster', 'synctab.empty_orc', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_parquet','paa_cluster', 'synctab.empty_parquet', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_orc_partition','paa_cluster', 'synctab.empty_orc_partition', 'hdfs_server');

SELECT public.sync_hive_table('hive_cluster','mytestdb','empty_parquet_partition','paa_cluster', 'synctab.empty_parquet_partition', 'hdfs_server');

SELECT public.sync_hive_database('hive_cluster', 'mytestdb', 'paa_cluster', 'syncdb', 'hdfs_server');
\d syncdb.test_table1
SELECT * FROM synctab.test_table1;

\d syncdb.transactional_orc
SELECT * FROM synctab.transactional_orc;

\d syncdb.normal_orc
SELECT * FROM synctab.normal_orc;

\d syncdb.partition_orc_1
SELECT * FROM synctab.partition_orc_1;

\d syncdb.partition_orc_2
SELECT * FROM synctab.partition_orc_2;

\d syncdb.partition_orc_3
SELECT * FROM synctab.partition_orc_3;

\d syncdb.partition_orc_4
SELECT * FROM synctab.partition_orc_4;

\d syncdb.normal_parquet
SELECT * FROM synctab.normal_parquet;

\d syncdb.partition_parquet_1
SELECT * FROM synctab.partition_parquet_1;

\d syncdb.partition_parquet_2
SELECT * FROM synctab.partition_parquet_2;

\d syncdb.partition_parquet_3
SELECT * FROM synctab.partition_parquet_3;

\d syncdb.text_default
SELECT * FROM synctab.text_default;

\d syncdb.text_custom
SELECT * FROM synctab.text_custom;

\d syncdb.csv_default
SELECT * FROM synctab.csv_default;

\d syncdb.csv_custom
SELECT * FROM synctab.csv_custom;

\d syncdb.empty_orc_transactional
SELECT * FROM synctab.empty_orc_transactional;

\d syncdb.empty_orc
SELECT * FROM synctab.empty_orc;

\d syncdb.empty_parquet
SELECT * FROM synctab.empty_parquet;

\d syncdb.empty_orc_partition
SELECT * FROM synctab.empty_orc_partition;

\d syncdb.empty_parquet_partition
SELECT * FROM synctab.empty_parquet_partition;

