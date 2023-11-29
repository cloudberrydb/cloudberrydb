DROP TABLE IF EXISTS customer_demographics_w;
DROP EXTERNAL TABLE IF EXISTS er_customer_demographics_hdfs;

CREATE TABLE customer_demographics_w(like customer_demographics);
CREATE FOREIGN TABLE er_customer_demographics_hdfs
(
    cd_demo_sk                integer                       ,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/customer_demographics/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
