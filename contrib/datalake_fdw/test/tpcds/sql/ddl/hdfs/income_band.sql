DROP TABLE IF EXISTS income_band_w;
DROP EXTERNAL TABLE IF EXISTS er_income_band_hdfs;

CREATE TABLE income_band_w(like income_band);
CREATE FOREIGN TABLE er_income_band_hdfs
(
    ib_income_band_sk         integer                       ,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/income_band/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
