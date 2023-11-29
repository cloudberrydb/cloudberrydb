DROP TABLE IF EXISTS household_demographics;
DROP EXTERNAL TABLE IF EXISTS er_household_demographics;

create table household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
) DISTRIBUTED BY(hd_demo_sk);


CREATE FOREIGN TABLE er_household_demographics
(
    hd_demo_sk                integer                       ,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
)
SERVER tpcds_oss
OPTIONS (filePath '/test-data/tpcds-orc/10g/household_demographics/', format 'orc');
