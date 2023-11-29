DROP TABLE IF EXISTS time_dim;
DROP EXTERNAL TABLE IF EXISTS er_time_dim;

create table time_dim
(
    t_time_sk                 integer               not null,
    t_time_id                 char(16)              not null,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      
) DISTRIBUTED BY(t_time_sk);


CREATE FOREIGN TABLE er_time_dim
(
    t_time_sk                 integer               ,
    t_time_id                 char(16)              ,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      
)
SERVER tpcds_oss
OPTIONS (filePath '/test-data/tpcds-orc/10g/time_dim/', format 'orc');
