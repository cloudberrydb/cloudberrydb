DROP TABLE IF EXISTS store_returns_w;
DROP EXTERNAL TABLE IF EXISTS er_store_returns_hdfs;

CREATE TABLE store_returns_w(like store_returns);
CREATE FOREIGN TABLE er_store_returns_hdfs
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer                       ,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          bigint                        ,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/store_returns/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
