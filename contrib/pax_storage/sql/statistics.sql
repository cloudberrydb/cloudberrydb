set default_table_access_method = pax;

CREATE OR REPLACE FUNCTION "get_pax_aux_table"(table_name text)
  RETURNS TABLE("ptblockname" name,"pttupcount" integer, 
    "ptblocksize" integer, "ptstatistics" pg_ext_aux.paxauxstats, "ptvisimapname" name) AS $BODY$
    DECLARE
    subquery  varchar;
    pre_sql   varchar;
    table_oid Oid;
    begin
    pre_sql:='select oid from pg_class where relname='''||table_name||'''';
    EXECUTE pre_sql into table_oid;
    subquery := 'select * from gp_dist_random(''pg_ext_aux.pg_pax_blocks_'||table_oid||''')';
    RETURN QUERY execute subquery;
    END
    $BODY$
  LANGUAGE plpgsql;

-- 
-- Test with small group
-- 
set pax_max_tuples_per_group = 10;

-- overview
create table t1(v1 int, v2 text, v3 float8, v4 bool) with (minmax_columns='v1,v2,v3,v4');
insert into t1 select i,i::text,i::float8, i % 2 > 0 from generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
drop table t1;

-- test `hasnull/allnull`/`count`, not need setting the minmax_columns
create table t1(v1 int, v2 float8);

insert into t1 values(generate_series(1, 1000), NULL);
select * from get_pax_aux_table('t1');
truncate t1;

insert into t1 select NULL, NULL from generate_series(1, 200)i;
select * from get_pax_aux_table('t1');
truncate t1;

insert into t1 SELECT (CASE WHEN i % 2 > 0 THEN NULL ELSE i END), (CASE WHEN i % 2 < 1 THEN NULL ELSE i END) FROM generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
truncate t1;

drop table t1;

-- test `sum`

create table t1(v1 int2, v2 int4, v3 int8, v4 float4, v5 float8, v6 money, v7 interval) with (minmax_columns='v1,v2,v3,v4,v5,v6,v7');
insert into t1 values(generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), 
  generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), (interval '3 seconds'));
select * from get_pax_aux_table('t1');
drop table t1;

create table t_int2(v1 int2) with (minmax_columns='v1');
insert into t_int2 values(generate_series(1, 1000));
select * from get_pax_aux_table('t_int2');
drop table t_int2;

create table t_int4(v1 int4) with (minmax_columns='v1');
insert into t_int4 values(generate_series(1001, 2000));
select * from get_pax_aux_table('t_int4');
drop table t_int4;

create table t_int8(v1 int8) with (minmax_columns='v1');
insert into t_int8 values(generate_series(2001, 3000));
select * from get_pax_aux_table('t_int8');
drop table t_int8;

create table t_float4(v1 float4) with (minmax_columns='v1');
insert into t_float4 values(generate_series(3001, 4000));
select * from get_pax_aux_table('t_float4');
drop table t_float4;

create table t_float8(v1 float8) with (minmax_columns='v1');
insert into t_float8 values(generate_series(4001, 5000));
select * from get_pax_aux_table('t_float8');
drop table t_float8;

create table t_money(v1 int4, v2 money) with (minmax_columns='v1,v2');
insert into t_money values(generate_series(5001, 6000), generate_series(5001, 6000));
select * from get_pax_aux_table('t_money');
drop table t_money;

create table t_interval(v1 interval) with (minmax_columns='v1');

insert into t_interval values 
  (interval '1 seconds'), (interval '2 seconds'), (interval '3 seconds'), (interval '4 seconds'), (interval '5 seconds'), (interval '50 seconds'), 
  (interval '6 minutes'), (interval '7 minutes'), (interval '8 minutes'), (interval '9 minutes'), (interval '10 minutes'), (interval '60 minutes'), 
  (interval '7 hours') , (interval '8 hours') , (interval '9 hours') , (interval '10 hours') , (interval '11 hours') , (interval '400 hours');
select * from get_pax_aux_table('t_interval');
drop table t_interval;

reset pax_max_tuples_per_group;

-- 
-- Test with small group
-- 

create table t1(v1 int, v2 text, v3 float8, v4 bool) with (minmax_columns='v1,v2,v3,v4');
insert into t1 select i,i::text,i::float8, i % 2 > 0 from generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
drop table t1;

create table t1(v1 int, v2 float8);
insert into t1 values(generate_series(1, 1000), NULL);
select * from get_pax_aux_table('t1');
truncate t1;

insert into t1 select NULL, NULL from generate_series(1, 200)i;
select * from get_pax_aux_table('t1');
truncate t1;

insert into t1 SELECT (CASE WHEN i % 2 > 0 THEN NULL ELSE i END), (CASE WHEN i % 2 < 1 THEN NULL ELSE i END) FROM generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
truncate t1;

drop table t1;

create table t1(v1 int2, v2 int4, v3 int8, v4 float4, v5 float8, v6 money, v7 interval) with (minmax_columns='v1,v2,v3,v4,v5,v6,v7');
insert into t1 values(generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), 
  generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), (interval '3 seconds'));
select * from get_pax_aux_table('t1');
drop table t1;

create table t_int2(v1 int2) with (minmax_columns='v1');
insert into t_int2 values(generate_series(1, 1000));
select * from get_pax_aux_table('t_int2');
drop table t_int2;

create table t_int4(v1 int4) with (minmax_columns='v1');
insert into t_int4 values(generate_series(1001, 2000));
select * from get_pax_aux_table('t_int4');
drop table t_int4;

create table t_int8(v1 int8) with (minmax_columns='v1');
insert into t_int8 values(generate_series(2001, 3000));
select * from get_pax_aux_table('t_int8');
drop table t_int8;

create table t_float4(v1 float4) with (minmax_columns='v1');
insert into t_float4 values(generate_series(3001, 4000));
select * from get_pax_aux_table('t_float4');
drop table t_float4;

create table t_float8(v1 float8) with (minmax_columns='v1');
insert into t_float8 values(generate_series(4001, 5000));
select * from get_pax_aux_table('t_float8');
drop table t_float8;

create table t_money(v1 int4, v2 money) with (minmax_columns='v1,v2');
insert into t_money values(generate_series(5001, 6000), generate_series(5001, 6000));
select * from get_pax_aux_table('t_money');
drop table t_money;

create table t_interval(v1 interval) with (minmax_columns='v1');

-- 15sec + 50sec + 40mins + 60 mins + 45hrs + 400hrs
insert into t_interval values 
  (interval '1 seconds'), (interval '2 seconds'), (interval '3 seconds'), (interval '4 seconds'), (interval '5 seconds'), (interval '50 seconds'), 
  (interval '6 minutes'), (interval '7 minutes'), (interval '8 minutes'), (interval '9 minutes'), (interval '10 minutes'), (interval '60 minutes'), 
  (interval '7 hours') , (interval '8 hours') , (interval '9 hours') , (interval '10 hours') , (interval '11 hours') , (interval '400 hours');
select * from get_pax_aux_table('t_interval');
drop table t_interval;
