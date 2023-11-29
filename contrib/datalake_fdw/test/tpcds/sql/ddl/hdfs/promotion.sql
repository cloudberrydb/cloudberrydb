DROP TABLE IF EXISTS promotion_w;
DROP EXTERNAL TABLE IF EXISTS er_promotion_hdfs;

CREATE TABLE promotion_w(like promotion);
CREATE FOREIGN TABLE er_promotion_hdfs
(
    p_promo_sk                integer                       ,
    p_promo_id                char(16)                      ,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/promotion/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
