DROP TABLE IF EXISTS reason_w;
DROP EXTERNAL TABLE IF EXISTS er_reason_hdfs;

CREATE TABLE reason_w(like reason);
CREATE FOREIGN TABLE er_reason_hdfs
(
    r_reason_sk               integer               ,
    r_reason_id               char(16)              ,
    r_reason_desc             char(100)                     
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/reason/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
