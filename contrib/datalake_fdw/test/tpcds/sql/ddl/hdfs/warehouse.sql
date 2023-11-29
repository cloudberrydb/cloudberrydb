DROP TABLE IF EXISTS warehouse_w;
DROP EXTERNAL TABLE er_warehouse_hdfs;

CREATE TABLE warehouse_w(like warehouse);
CREATE FOREIGN TABLE er_warehouse_hdfs
(
    w_warehouse_sk            integer                       ,
    w_warehouse_id            char(16)                      ,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/warehouse/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
