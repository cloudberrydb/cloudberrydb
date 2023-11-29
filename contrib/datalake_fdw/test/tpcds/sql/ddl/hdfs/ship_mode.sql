DROP TABLE IF EXISTS ship_mode_w;
DROP EXTERNAL TABLE IF EXISTS er_ship_mode_hdfs;

CREATE TABLE ship_mode_w(like ship_mode);
CREATE FOREIGN TABLE er_ship_mode_hdfs
(
    sm_ship_mode_sk           integer               ,
    sm_ship_mode_id           char(16)              ,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/ship_mode/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
