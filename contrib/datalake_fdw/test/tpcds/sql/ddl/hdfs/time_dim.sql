DROP TABLE IF EXISTS time_dim_w;
DROP EXTERNAL TABLE IF EXISTS er_time_dim_hdfs;

CREATE TABLE time_dim_w(like time_dim);
CREATE FOREIGN TABLE er_time_dim_hdfs
(
    t_time_sk                 integer               ,
    t_time_id                 char(16)              ,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/time_dim/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
