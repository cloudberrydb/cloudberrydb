DROP TABLE IF EXISTS income_band;
DROP EXTERNAL TABLE IF EXISTS er_income_band;

create table income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
) DISTRIBUTED BY(ib_income_band_sk);


CREATE FOREIGN TABLE er_income_band 
(
    ib_income_band_sk         integer                       ,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
)
SERVER tpcds_oss
OPTIONS (filePath '/test-data/tpcds-orc/10g/income_band/', format 'orc');
