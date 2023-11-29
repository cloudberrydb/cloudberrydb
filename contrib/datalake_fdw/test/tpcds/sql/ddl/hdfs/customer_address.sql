DROP TABLE IF EXISTS customer_address_w;
DROP EXTERNAL TABLE IF EXISTS er_customer_address_hdfs;

CREATE TABLE customer_address_w(like customer_address);
CREATE FOREIGN TABLE er_customer_address_hdfs
(
    ca_address_sk             integer               ,
    ca_address_id             char(16)              ,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)                      
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/customer_address/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
