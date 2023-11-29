DROP TABLE IF EXISTS inventory;
DROP EXTERNAL TABLE IF EXISTS er_inventory;

create table inventory
(
    inv_date_sk               integer               ,
    inv_item_sk               integer               ,
    inv_warehouse_sk          integer               ,
    inv_quantity_on_hand      integer                       
) DISTRIBUTED BY(inv_date_sk,inv_item_sk,inv_warehouse_sk)
PARTITION BY range(inv_date_sk)
(
partition p1 start(2450815) INCLUSIVE end(2451179) INCLUSIVE, 
partition p2 start(2451180) INCLUSIVE end(2451544) INCLUSIVE, 
partition p3 start(2451545) INCLUSIVE end(2451910) INCLUSIVE, 
partition p4 start(2451911) INCLUSIVE end(2452275) INCLUSIVE, 
partition p5 start(2452276) INCLUSIVE end(2452640) INCLUSIVE
,default partition others
);


CREATE FOREIGN TABLE er_inventory
(
    inv_date_sk               integer               ,
    inv_item_sk               integer               ,
    inv_warehouse_sk          integer               ,
    inv_quantity_on_hand      integer                       
)
SERVER tpcds_oss
OPTIONS (filePath '/test-data/tpcds-orc/10g/inventory/', format 'orc');
