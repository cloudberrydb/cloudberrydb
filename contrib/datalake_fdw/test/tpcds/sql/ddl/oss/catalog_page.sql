DROP TABLE IF EXISTS catalog_page;
DROP EXTERNAL TABLE IF EXISTS er_catalog_page;

create table catalog_page
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        char(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  
) DISTRIBUTED BY(cp_catalog_page_sk);


CREATE FOREIGN TABLE er_catalog_page
(
    cp_catalog_page_sk        integer                       ,
    cp_catalog_page_id        char(16)                      ,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  
)
SERVER tpcds_oss
OPTIONS (filePath '/test-data/tpcds-orc/10g/catalog_page/', format 'orc');
