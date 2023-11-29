DROP TABLE IF EXISTS reason;
DROP EXTERNAL TABLE IF EXISTS er_reason;

create table reason
(
    r_reason_sk               integer               ,
    r_reason_id               char(16)              ,
    r_reason_desc             char(100)                     
)DISTRIBUTED BY(r_reason_sk);


CREATE FOREIGN TABLE er_reason
(
    r_reason_sk               integer               ,
    r_reason_id               char(16)              ,
    r_reason_desc             char(100)                     
)
SERVER tpcds_oss
OPTIONS (filePath '/test-data/tpcds-orc/10g/reason/', format 'orc');
