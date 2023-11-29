DROP TABLE IF EXISTS inventory_w;
DROP EXTERNAL TABLE IF EXISTS er_inventory_hdfs;

CREATE TABLE inventory_w(like inventory);
CREATE FOREIGN TABLE er_inventory_hdfs
(
    inv_date_sk               integer               ,
    inv_item_sk               integer               ,
    inv_warehouse_sk          integer               ,
    inv_quantity_on_hand      integer                       
)
SERVER tpcds_hdfs
OPTIONS (filePath '/test-data/tpcds-orc/10g/inventory/', hdfs_cluster_name 'paa_cluster', enablecache 'true', transactional 'false', format 'orc');
