drop table if exists skewed_for_gpic;
drop table if exists spread_for_gpic;

drop table if exists foo_for_gpic;

create table skewed_for_gpic (a int, b int);
create table spread_for_gpic (a int, b int);

set gp_interconnect_type=tcp;
set gp_enable_mk_sort = off;

insert into skewed_for_gpic select 1, i from generate_series(1, 1000) i;
insert into spread_for_gpic select i, 1 from generate_series(1, 1000) i;

select * from skewed_for_gpic, spread_for_gpic where skewed_for_gpic.a = spread_for_gpic.b order by 1, 2, 3, 4 desc limit 20;

create table foo_for_gpic as select *, 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'::text as c from skewed_for_gpic;

select foo_for_gpic.a, spread_for_gpic.a, c from foo_for_gpic, spread_for_gpic where foo_for_gpic.a = spread_for_gpic.b order by foo_for_gpic.a, spread_for_gpic.a desc, c limit 20;

truncate skewed_for_gpic;
truncate spread_for_gpic;

drop table if exists foo_for_gpic;

set gp_interconnect_type=udp;

insert into skewed_for_gpic select 1, i from generate_series(1, 1000) i;
insert into spread_for_gpic select i, 1 from generate_series(1, 1000) i;

create table foo_for_gpic as select *, 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'::text as c from skewed_for_gpic;

select foo_for_gpic.a, spread_for_gpic.a, c from foo_for_gpic, spread_for_gpic where foo_for_gpic.a = spread_for_gpic.b order by foo_for_gpic.a, spread_for_gpic.a desc, c limit 20;

select * from skewed_for_gpic, spread_for_gpic where skewed_for_gpic.a = spread_for_gpic.b order by 1 asc, 2, 3 asc, 4 limit 20;

drop table if exists foo_for_gpic;
create table foo_for_gpic (a int, b text);

begin;
set gp_interconnect_type=nil;
insert into foo_for_gpic values (1, 'test');
select * from foo_for_gpic;
abort;

set gp_enable_mk_sort = on;
drop table if exists foo_for_gpic;

set gp_interconnect_type=udp;
create temp table gpic_test1 (timet numeric(16,6));
create temp sequence gpic_snoid start 1;
insert into gpic_test1 values (33.0),(33.0),(33.0);
select nextval('gpic_snoid'),to_timestamp(timet) from gpic_test1;


-- TEST for MPP-7835: directed dispatch interaction with TCP-IC
DROP TABLE IF EXISTS MWV_CDetail_TABLE;

CREATE TABLE MWV_CDetail_TABLE(
 ATTRIBUTE066 DATE,
 ATTRIBUTE084 TIMESTAMP,
 ATTRIBUTE097 BYTEA,
 ATTRIBUTE096 BIGINT,
 ATTRIBUTE032 VARCHAR (3),
 ATTRIBUTE031 VARCHAR (20),
 ATTRIBUTE151 VARCHAR (4000),
 ATTRIBUTE037 VARCHAR (4000),
 ATTRIBUTE047 VARCHAR (2048)
)
with (appendonly=true, orientation=column, compresstype=quicklz)
distributed by (attribute066)
partition by range (attribute066)
(start (date '2009-10-01') inclusive end (date '2009-12-31') inclusive every (interval '1 week'));

set gp_enable_direct_dispatch=off;
set gp_interconnect_type=tcp;

SELECT
  ATTRIBUTE066,ATTRIBUTE032 ,ATTRIBUTE031
 ,COUNT(*) AS CNT
FROM 
  MWV_CDETAIL_TABLE
WHERE ATTRIBUTE066 = '2009-12-31'::date - 1
GROUP BY ATTRIBUTE066,ATTRIBUTE032 ,ATTRIBUTE031;

set gp_enable_direct_dispatch=on;

SELECT
  ATTRIBUTE066,ATTRIBUTE032 ,ATTRIBUTE031
 ,COUNT(*) AS CNT
FROM 
  MWV_CDETAIL_TABLE
WHERE ATTRIBUTE066 = '2009-12-31'::date - 1
GROUP BY ATTRIBUTE066,ATTRIBUTE032 ,ATTRIBUTE031;

set gp_enable_direct_dispatch=off;
set gp_interconnect_type=udp;

SELECT
  ATTRIBUTE066,ATTRIBUTE032 ,ATTRIBUTE031
 ,COUNT(*) AS CNT
FROM 
  MWV_CDETAIL_TABLE
WHERE ATTRIBUTE066 = '2009-12-31'::date - 1
GROUP BY ATTRIBUTE066,ATTRIBUTE032 ,ATTRIBUTE031;

set gp_enable_direct_dispatch=on;

SELECT
  ATTRIBUTE066,ATTRIBUTE032 ,ATTRIBUTE031
 ,COUNT(*) AS CNT
FROM 
  MWV_CDETAIL_TABLE
WHERE ATTRIBUTE066 = '2009-12-31'::date - 1
GROUP BY ATTRIBUTE066,ATTRIBUTE032 ,ATTRIBUTE031;

DROP TABLE IF EXISTS MWV_CDetail_TABLE;