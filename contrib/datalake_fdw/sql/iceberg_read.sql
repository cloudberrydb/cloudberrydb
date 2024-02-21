-- create datalake_fdw
-- clean table
DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER sync_server;
DROP SERVER IF EXISTS sync_server;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw;

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

SELECT pg_sleep(5);

CREATE EXTENSION IF NOT EXISTS hive_connector;

SELECT pg_sleep(5);

set vector.enable_vectorization=off;

CREATE FOREIGN DATA WRAPPER datalake_fdw
HANDLER datalake_fdw_handler
VALIDATOR datalake_fdw_validator
OPTIONS (mpp_execute 'all segments');

SELECT public.create_foreign_server('sync_server', 'gpadmin', 'datalake_fdw', 'hdfs-cluster-1');

------ iceberg_table1 ------
CREATE FOREIGN TABLE iceberg_table1 (
    id int,
    name text,
    age int,
    address text
)
server sync_server
OPTIONS (filePath 'icebergdb.iceberg_table1', catalog_type 'hive', server_name 'hive-cluster-1', hdfs_cluster_name 'hdfs-cluster-1', table_identifier 'icebergdb.iceberg_table1', format 'iceberg');

-- count
select count(*) from iceberg_table1;
-- explain
explain select * from iceberg_table1 order by id, name limit 10;
-- order by, limit
select * from iceberg_table1 order by id, name  limit 10;
-- group by
select name, count(*) from iceberg_table1 group by name order by name;
-- where
select * from iceberg_table1 where id = 10;

------ iceberg_table2 ------
CREATE FOREIGN TABLE iceberg_table2
(
    a int,
    b decimal(10,5),
    c text,
    d text,
    e text,
    f text
)
server sync_server
OPTIONS (filePath 'icebergdb.iceberg_table2', catalog_type 'hive', server_name 'hive-cluster-1', hdfs_cluster_name 'hdfs-cluster-1', table_identifier 'icebergdb.iceberg_table2', format 'iceberg');

-- count
select count(*) from iceberg_table2;
-- explain
explain select * from iceberg_table2 order by a, b  limit 10;
-- order by, limit
select * from iceberg_table2 order by a, b  limit 10;
-- group by
select a, count(*) from iceberg_table2 group by a order by a;
-- where
select count(*) from iceberg_table2 where a = 1;

------ iceberg_table3: PARTITIONED ------
CREATE FOREIGN TABLE iceberg_table3
(
    id   bigint,
    name text,
    dt text,
    hh text
)
server sync_server
OPTIONS (filePath 'icebergdb.iceberg_table3', catalog_type 'hive', server_name 'hive-cluster-1', hdfs_cluster_name 'hdfs-cluster-1', table_identifier 'icebergdb.iceberg_table3', format 'iceberg');

-- count
select count(*) from iceberg_table3;
-- explain
explain select * from iceberg_table3 order by id, name limit 10;
-- order by, limit
select * from iceberg_table3 order by id, name  limit 10;
-- group by
select name, count(*) from iceberg_table3 group by name order by name;
-- where
select * from iceberg_table3 where id = 10;

-- clean table
DROP FOREIGN TABLE IF EXISTS iceberg_table1;
DROP FOREIGN TABLE IF EXISTS iceberg_table2;
DROP FOREIGN TABLE IF EXISTS iceberg_table3;