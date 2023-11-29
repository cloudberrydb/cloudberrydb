DROP TABLE IF EXISTS date_dim_w;
DROP EXTERNAL TABLE IF EXISTS er_date_dim_hdfs;

CREATE TABLE date_dim_w(like date_dim);
CREATE FOREIGN TABLE er_date_dim_hdfs
(
    d_date_sk                 integer                       ,
    d_date_id                 char(16)                      ,
    d_date                    date                          ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/date_dim/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
