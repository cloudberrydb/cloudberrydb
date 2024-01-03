-- start_ignore
create extension pax;
drop table if exists all_typbyval_pg_types;
-- end_ignore

CREATE TABLE all_typbyval_pg_types (
  id int,
  bool_col bool,
  char_col char,
  int2_col int2,
  cid_col cid,
  
  float4_col float4,
  int4_col int4,
  date_col date,
  
  oid_col oid,
  -- åxid_col xid,
  time_stamp_col timestamp,
  int8_col int8,
  -- xid8_col xid8,
  float8_col float8,
  money_col money,
  time_col time,
  timestamptz_col timestamptz,
  pg_lsn_col pg_lsn
) USING pax distributed by (id);

insert into all_typbyval_pg_types values(1, true,'c',2,'cid',4.2,5,'2023-05-17 17:56:49',7,'2023-05-17 17:56:49',10,11.1111,12,'2023-05-17 17:56:49','2023-05-17 17:56:49', '16/0'),
(1, true,'c',2,'cid',4.2,5,'2023-05-17 17:56:49',7,'2023-05-17 17:56:49',10,11.1111,12,'2023-05-17 17:56:49','2023-05-17 17:56:49', '16/0'),
(1, true,'c',2,'cid',4.2,5,'2023-05-17 17:56:49',7,'2023-05-17 17:56:49',10,11.1111,12,'2023-05-17 17:56:49','2023-05-17 17:56:49', '16/0');
select * from all_typbyval_pg_types;

-- start_ignore
drop table if exists all_typlen_lt_0_pg_type;
-- end_ignore

create table all_typlen_lt_0_pg_type (
  id int,
  name_col name,
  numeric_col numeric,
  text_col text,
  varchar_col varchar(128),
  point_col point
) USING pax distributed by (id);

insert into all_typlen_lt_0_pg_type values(1,'hello', 1.23, 'text', 'varchar', point(1,2));
select * from all_typlen_lt_0_pg_type;

-- start_ignore
drop table if exists all_typbyval_pg_types;
-- end_ignore