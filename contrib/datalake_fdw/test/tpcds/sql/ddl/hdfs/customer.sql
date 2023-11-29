DROP TABLE IF EXISTS customer_w;
DROP EXTERNAL TABLE IF EXISTS er_customer_hdfs;

CREATE TABLE customer_w(like customer);
CREATE FOREIGN TABLE er_customer_hdfs
(
    c_customer_sk             integer                       ,
    c_customer_id             char(16)                      ,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/customer/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
