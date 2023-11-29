DROP TABLE IF EXISTS household_demographics_w;
DROP EXTERNAL TABLE IF EXISTS er_household_demographics_hdfs;

CREATE TABLE household_demographics_w(like household_demographics);
CREATE FOREIGN TABLE er_household_demographics_hdfs
(
    hd_demo_sk                integer                       ,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/household_demographics/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
