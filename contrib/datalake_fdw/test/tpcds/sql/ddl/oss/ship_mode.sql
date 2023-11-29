DROP TABLE IF EXISTS ship_mode;
DROP EXTERNAL TABLE IF EXISTS er_ship_mode;

create table ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           char(16)              not null,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      
)DISTRIBUTED BY(sm_ship_mode_sk);


CREATE FOREIGN TABLE er_ship_mode
(
    sm_ship_mode_sk           integer               ,
    sm_ship_mode_id           char(16)              ,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      
)
SERVER tpcds_oss
OPTIONS (filePath '/test-data/tpcds-orc/10g/ship_mode/', format 'orc');
