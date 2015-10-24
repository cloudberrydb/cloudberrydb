--start_ignore
drop table if exists plineitem cascade;
drop table if exists times;
--end_ignore

-- Ignoring error messages that might cause diff when plan size changes for test queries

-- start_matchsubs
-- m/ERROR:  Query plan size limit exceeded.*/
-- s/ERROR:  Query plan size limit exceeded.*/ERROR_MESSAGE/
-- end_matchsubs

-- create partitioned lineitem, partitioned by an integer column
CREATE TABLE plineitem
(
                                  L_ID          INTEGER ,
                                  L_ORDERKEY    INTEGER ,
                                  L_PARTKEY     INTEGER ,
                                  L_SUPPKEY     INTEGER ,
                                  L_LINENUMBER  INTEGER ,
                                  L_QUANTITY    DECIMAL(15,2) ,
                                  L_EXTENDEDPRICE  DECIMAL(15,2) ,
                                  L_DISCOUNT    DECIMAL(15,2) ,
                                  L_TAX         DECIMAL(15,2) ,
                                  L_RETURNFLAG  CHAR(1) ,
                                  L_LINESTATUS  CHAR(1) ,
                                  L_SHIPDATE    DATE ,
                                  L_COMMITDATE  DATE ,
                                  L_RECEIPTDATE DATE ,
                                  L_SHIPINSTRUCT CHAR(25) ,
                                  L_SHIPMODE     CHAR(10) ,
                                  L_COMMENT      VARCHAR(44)
 ) distributed by (l_orderkey)
partition by range(L_ID)
(
start (0) inclusive end(3000) every(100)
);

create table times
(
id  int,
date_id date,
date_text_id varchar(20),
day_name varchar(20),
day_number_in_week int,
day_number_in_month int,
week_number_in_month int,
week_number_in_year int,
week_start_date date,
week_end_date date,
month_number int,
month_name varchar(20),
year_number int,
quarter_number int,
quarter_desc varchar(20),
leap boolean,
days_in_year int
) distributed by (date_id);



create or replace function f() returns setof plineitem as $$
select * from plineitem;
$$
language SQL;


create or replace function f2() returns bigint as $$
select count(*) from plineitem;
$$
language SQL;


set gp_max_plan_size =20;

-- Following queries should error out for plan size 20KB
-- simple select
select * from plineitem;

-- UDFs
select * from f();

select * from f2();

-- Joins
select  l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        plineitem,times
where
       plineitem.l_shipdate = times.date_id
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;

select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       count(*) as count_order
from
      plineitem join times on (times.date_id = plineitem.l_shipdate and  plineitem.l_receiptdate < times.week_end_date)
group by
       l_orderkey
order by
       l_orderkey
LIMIT 10;

-- Init plans
select p1.l_orderkey,
       sum(p1.l_extendedprice * (1 - p1.l_discount)) as revenue,
       count(*) as count_order
from
      plineitem p1 join times on (times.date_id = p1.l_shipdate and  p1.l_receiptdate < times.week_end_date)
      join plineitem p2 on ( p2.l_shipdate < (select max(l_receiptdate) from plineitem))
group by
       p1.l_orderkey
order by
       p1.l_orderkey
LIMIT 10;



set gp_max_plan_size ='700kB';

-- Following queries should be dispatched for plan size 700kB

-- simple select
select * from plineitem;

-- UDFs
select * from f();

select * from f2();

-- Joins
select  l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        plineitem,times
where
       plineitem.l_shipdate = times.date_id
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;

select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       count(*) as count_order
from
      plineitem join times on (times.date_id = plineitem.l_shipdate and  plineitem.l_receiptdate < times.week_end_date)
group by
       l_orderkey
order by
       l_orderkey
LIMIT 10;

-- Init plans
select p1.l_orderkey,
       sum(p1.l_extendedprice * (1 - p1.l_discount)) as revenue,
       count(*) as count_order
from
      plineitem p1 join times on (times.date_id = p1.l_shipdate and  p1.l_receiptdate < times.week_end_date)
      join plineitem p2 on ( p2.l_shipdate < (select max(l_receiptdate) from plineitem))
group by
       p1.l_orderkey
order by
       p1.l_orderkey
LIMIT 10;

set gp_max_plan_size ='10MB';


-- Following queries should success for plan size 10MB

-- simple select
select * from plineitem;

-- UDFs
select * from f();

select * from f2();

-- Joins
select  l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        plineitem,times
where
       plineitem.l_shipdate = times.date_id
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;

select l_orderkey,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       count(*) as count_order
from
      plineitem join times on (times.date_id = plineitem.l_shipdate and  plineitem.l_receiptdate < times.week_end_date)
group by
       l_orderkey
order by
       l_orderkey
LIMIT 10;

-- Init plans
select p1.l_orderkey,
       sum(p1.l_extendedprice * (1 - p1.l_discount)) as revenue,
       count(*) as count_order
from
      plineitem p1 join times on (times.date_id = p1.l_shipdate and  p1.l_receiptdate < times.week_end_date)
      join plineitem p2 on ( p2.l_shipdate < (select max(l_receiptdate) from plineitem))
group by
       p1.l_orderkey
order by
       p1.l_orderkey
LIMIT 10;