-- Casts defined in the 4.0 Catalog
--
-- FIXME: Skips casts that have functions that return the "wrong" datatype.
-- FIXME: Skips casts implemented with SQL language functions 
--
-- NOTE: the catalog contains 87 casts that can not be created by normal
-- means because the functions are defined with the wrong signature.  
-- We handle this by creating an extra schema "dummy_cast_functions"
-- and adding the needed functions there.  (see dummy_cast_functions.sql)
--
-- Derived via the following SQL run within a 4.0 catalog
/*
\o /tmp/casts
SELECT 'CREATE CAST ('
    || 'upg_catalog.' || quote_ident(t1.typname) || ' AS '
    || 'upg_catalog.' || quote_ident(t2.typname) || ') '
    || case when proname is not null 
            then 'WITH FUNCTION ' 
                 || case when p.proargtypes[0] != c.castsource or p.prorettype != c.casttarget
                         then 'dummy_cast_functions.dummycast_' 
                              || t1.typname || '_' || t2.typname || '_' || p.proname
                              || '(upg_catalog.' || quote_ident(t1.typname) || ')'
                         else 'upg_catalog.' || p.proname || '('
                              || array_to_string( array ( 
                                    select coalesce(proargnames[i] || ' ','') 
                                           || case when i = 1 then 'upg_catalog.' else 'pg_catalog.' end
                                           || quote_ident(typname)
                                    from pg_type t, generate_series(1, pronargs) i
                                     where t.oid = proargtypes[i-1]
                                     order by i), ', ') || ')'
                         end
            else 'WITHOUT FUNCTION' end
    || case when castcontext = 'e' then ''
            when castcontext = 'i' then ' AS IMPLICIT'
            when castcontext = 'a' then ' AS ASSIGNMENT' end
    || ';'
FROM pg_cast c
     join pg_type t1 on (c.castsource = t1.oid)
     join pg_type t2 on (c.casttarget = t2.oid)
     left join pg_proc p on (c.castfunc = p.oid)
ORDER BY 1;
*/
 CREATE CAST (upg_catalog."bit" AS upg_catalog."bit") WITH FUNCTION upg_catalog.bit(upg_catalog."bit", pg_catalog.int4, pg_catalog.bool) AS IMPLICIT;
 CREATE CAST (upg_catalog."bit" AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog."bit");
 CREATE CAST (upg_catalog."bit" AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog."bit");
 CREATE CAST (upg_catalog."bit" AS upg_catalog.varbit) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog."char" AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_char_varchar_text(upg_catalog."char") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."char" AS upg_catalog.bpchar) WITH FUNCTION upg_catalog.bpchar(upg_catalog."char") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."char" AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog."char");
 CREATE CAST (upg_catalog."char" AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog."char") AS IMPLICIT;
 CREATE CAST (upg_catalog."interval" AS upg_catalog."interval") WITH FUNCTION upg_catalog.interval(upg_catalog."interval", pg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog."interval" AS upg_catalog."time") WITH FUNCTION upg_catalog.time(upg_catalog."interval") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."interval" AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_interval_varchar_text(upg_catalog."interval") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."interval" AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_interval_bpchar_text(upg_catalog."interval") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."interval" AS upg_catalog.reltime) WITH FUNCTION upg_catalog.reltime(upg_catalog."interval") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."interval" AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog."interval") AS IMPLICIT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog."numeric") WITH FUNCTION upg_catalog.numeric(upg_catalog."numeric", pg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_numeric_varchar_text(upg_catalog."numeric") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_numeric_bpchar_text(upg_catalog."numeric") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog.float4) WITH FUNCTION upg_catalog.float4(upg_catalog."numeric") AS IMPLICIT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog.float8) WITH FUNCTION upg_catalog.float8(upg_catalog."numeric") AS IMPLICIT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog.int2) WITH FUNCTION upg_catalog.int2(upg_catalog."numeric") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog."numeric") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog."numeric") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."numeric" AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog."numeric") AS IMPLICIT;
 CREATE CAST (upg_catalog."time" AS upg_catalog."interval") WITH FUNCTION upg_catalog.interval(upg_catalog."time") AS IMPLICIT;
 CREATE CAST (upg_catalog."time" AS upg_catalog."time") WITH FUNCTION upg_catalog.time(upg_catalog."time", pg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog."time" AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_time_varchar_text(upg_catalog."time") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."time" AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_time_bpchar_text(upg_catalog."time") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."time" AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog."time") AS IMPLICIT;
 CREATE CAST (upg_catalog."time" AS upg_catalog.timetz) WITH FUNCTION upg_catalog.timetz(upg_catalog."time") AS IMPLICIT;
 CREATE CAST (upg_catalog."timestamp" AS upg_catalog."time") WITH FUNCTION upg_catalog.time(upg_catalog."timestamp") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."timestamp" AS upg_catalog."timestamp") WITH FUNCTION upg_catalog.timestamp(upg_catalog."timestamp", pg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog."timestamp" AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_timestamp_varchar_text(upg_catalog."timestamp") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."timestamp" AS upg_catalog.abstime) WITH FUNCTION upg_catalog.abstime(upg_catalog."timestamp") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."timestamp" AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_timestamp_bpchar_text(upg_catalog."timestamp") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."timestamp" AS upg_catalog.date) WITH FUNCTION upg_catalog.date(upg_catalog."timestamp") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."timestamp" AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog."timestamp") AS IMPLICIT;
 CREATE CAST (upg_catalog."timestamp" AS upg_catalog.timestamptz) WITH FUNCTION upg_catalog.timestamptz(upg_catalog."timestamp") AS IMPLICIT;
 CREATE CAST (upg_catalog."varchar" AS upg_catalog."char") WITH FUNCTION dummy_cast_functions.dummycast_varchar_char_char(upg_catalog."varchar") AS ASSIGNMENT;
 CREATE CAST (upg_catalog."varchar" AS upg_catalog."interval") WITH FUNCTION dummy_cast_functions.dummycast_varchar_interval_interval(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog."numeric") WITH FUNCTION dummy_cast_functions.dummycast_varchar_numeric_numeric(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog."time") WITH FUNCTION dummy_cast_functions.dummycast_varchar_time_time(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog."timestamp") WITH FUNCTION dummy_cast_functions.dummycast_varchar_timestamp_timestamp(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog."varchar") WITH FUNCTION upg_catalog.varchar(upg_catalog."varchar", pg_catalog.int4, pg_catalog.bool) AS IMPLICIT;
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.bpchar) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.cidr) WITH FUNCTION dummy_cast_functions.dummycast_varchar_cidr_cidr(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.date) WITH FUNCTION dummy_cast_functions.dummycast_varchar_date_date(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.float4) WITH FUNCTION dummy_cast_functions.dummycast_varchar_float4_float4(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.float8) WITH FUNCTION dummy_cast_functions.dummycast_varchar_float8_float8(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.inet) WITH FUNCTION dummy_cast_functions.dummycast_varchar_inet_inet(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.int2) WITH FUNCTION dummy_cast_functions.dummycast_varchar_int2_int2(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.int4) WITH FUNCTION dummy_cast_functions.dummycast_varchar_int4_int4(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.int8) WITH FUNCTION dummy_cast_functions.dummycast_varchar_int8_int8(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.macaddr) WITH FUNCTION dummy_cast_functions.dummycast_varchar_macaddr_macaddr(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.name) WITH FUNCTION upg_catalog.name(upg_catalog."varchar") AS IMPLICIT;
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.oid) WITH FUNCTION dummy_cast_functions.dummycast_varchar_oid_oid(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.regclass) WITH FUNCTION dummy_cast_functions.dummycast_varchar_regclass_regclass(upg_catalog."varchar") AS IMPLICIT;
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.text) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.timestamptz) WITH FUNCTION dummy_cast_functions.dummycast_varchar_timestamptz_timestamptz(upg_catalog."varchar");
 CREATE CAST (upg_catalog."varchar" AS upg_catalog.timetz) WITH FUNCTION dummy_cast_functions.dummycast_varchar_timetz_timetz(upg_catalog."varchar");
 CREATE CAST (upg_catalog.abstime AS upg_catalog."time") WITH FUNCTION upg_catalog.time(upg_catalog.abstime) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.abstime AS upg_catalog."timestamp") WITH FUNCTION upg_catalog.timestamp(upg_catalog.abstime) AS IMPLICIT;
 CREATE CAST (upg_catalog.abstime AS upg_catalog.date) WITH FUNCTION upg_catalog.date(upg_catalog.abstime) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.abstime AS upg_catalog.int4) WITHOUT FUNCTION;
 CREATE CAST (upg_catalog.abstime AS upg_catalog.timestamptz) WITH FUNCTION upg_catalog.timestamptz(upg_catalog.abstime) AS IMPLICIT;
 CREATE CAST (upg_catalog.bool AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog.bool);
 CREATE CAST (upg_catalog.box AS upg_catalog.circle) WITH FUNCTION upg_catalog.circle(upg_catalog.box);
 CREATE CAST (upg_catalog.box AS upg_catalog.lseg) WITH FUNCTION upg_catalog.lseg(upg_catalog.box);
 CREATE CAST (upg_catalog.box AS upg_catalog.point) WITH FUNCTION upg_catalog.point(upg_catalog.box);
 CREATE CAST (upg_catalog.box AS upg_catalog.polygon) WITH FUNCTION upg_catalog.polygon(upg_catalog.box) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.bpchar AS upg_catalog."char") WITH FUNCTION dummy_cast_functions.dummycast_bpchar_char_char(upg_catalog.bpchar) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.bpchar AS upg_catalog."interval") WITH FUNCTION dummy_cast_functions.dummycast_bpchar_interval_interval(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog."numeric") WITH FUNCTION dummy_cast_functions.dummycast_bpchar_numeric_numeric(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog."time") WITH FUNCTION dummy_cast_functions.dummycast_bpchar_time_time(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog."timestamp") WITH FUNCTION dummy_cast_functions.dummycast_bpchar_timestamp_timestamp(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_bpchar_varchar_text(upg_catalog.bpchar) AS IMPLICIT;
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.bpchar) WITH FUNCTION upg_catalog.bpchar(upg_catalog.bpchar, pg_catalog.int4, pg_catalog.bool) AS IMPLICIT;
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.date) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_date_date(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.float4) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_float4_float4(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.float8) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_float8_float8(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.int2) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_int2_int2(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.int4) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_int4_int4(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.int8) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_int8_int8(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.macaddr) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_macaddr_macaddr(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.name) WITH FUNCTION upg_catalog.name(upg_catalog.bpchar) AS IMPLICIT;
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.oid) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_oid_oid(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.bpchar) AS IMPLICIT;
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.timestamptz) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_timestamptz_timestamptz(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.bpchar AS upg_catalog.timetz) WITH FUNCTION dummy_cast_functions.dummycast_bpchar_timetz_timetz(upg_catalog.bpchar);
 CREATE CAST (upg_catalog.cidr AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_cidr_varchar_text(upg_catalog.cidr);
 CREATE CAST (upg_catalog.cidr AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_cidr_bpchar_text(upg_catalog.cidr);
 CREATE CAST (upg_catalog.cidr AS upg_catalog.inet) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.cidr AS upg_catalog.text) WITH FUNCTION dummy_cast_functions.dummycast_cidr_text_text(upg_catalog.cidr);
 CREATE CAST (upg_catalog.circle AS upg_catalog.box) WITH FUNCTION upg_catalog.box(upg_catalog.circle);
 CREATE CAST (upg_catalog.circle AS upg_catalog.point) WITH FUNCTION upg_catalog.point(upg_catalog.circle);
 CREATE CAST (upg_catalog.circle AS upg_catalog.polygon) WITH FUNCTION upg_catalog.polygon(upg_catalog.circle);
 CREATE CAST (upg_catalog.date AS upg_catalog."timestamp") WITH FUNCTION upg_catalog.timestamp(upg_catalog.date) AS IMPLICIT;
 CREATE CAST (upg_catalog.date AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_date_varchar_text(upg_catalog.date) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.date AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_date_bpchar_text(upg_catalog.date) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.date AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.date) AS IMPLICIT;
 CREATE CAST (upg_catalog.date AS upg_catalog.timestamptz) WITH FUNCTION upg_catalog.timestamptz(upg_catalog.date) AS IMPLICIT;
 CREATE CAST (upg_catalog.float4 AS upg_catalog."numeric") WITH FUNCTION upg_catalog.numeric(upg_catalog.float4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float4 AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_float4_varchar_text(upg_catalog.float4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float4 AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_float4_bpchar_text(upg_catalog.float4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float4 AS upg_catalog.float8) WITH FUNCTION upg_catalog.float8(upg_catalog.float4) AS IMPLICIT;
 CREATE CAST (upg_catalog.float4 AS upg_catalog.int2) WITH FUNCTION upg_catalog.int2(upg_catalog.float4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float4 AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog.float4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float4 AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog.float4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float4 AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.float4) AS IMPLICIT;
 CREATE CAST (upg_catalog.float8 AS upg_catalog."numeric") WITH FUNCTION upg_catalog.numeric(upg_catalog.float8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float8 AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_float8_varchar_text(upg_catalog.float8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float8 AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_float8_bpchar_text(upg_catalog.float8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float8 AS upg_catalog.float4) WITH FUNCTION upg_catalog.float4(upg_catalog.float8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float8 AS upg_catalog.int2) WITH FUNCTION upg_catalog.int2(upg_catalog.float8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float8 AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog.float8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float8 AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog.float8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.float8 AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.float8) AS IMPLICIT;
 CREATE CAST (upg_catalog.gpaotid AS upg_catalog.tid) WITHOUT FUNCTION;
 CREATE CAST (upg_catalog.inet AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_inet_varchar_text(upg_catalog.inet);
 CREATE CAST (upg_catalog.inet AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_inet_bpchar_text(upg_catalog.inet);
 CREATE CAST (upg_catalog.inet AS upg_catalog.cidr) WITH FUNCTION upg_catalog.cidr(upg_catalog.inet) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.inet AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.inet);
 CREATE CAST (upg_catalog.int2 AS upg_catalog."numeric") WITH FUNCTION upg_catalog.numeric(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_int2_varchar_text(upg_catalog.int2) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_int2_bpchar_text(upg_catalog.int2) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.float4) WITH FUNCTION upg_catalog.float4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.float8) WITH FUNCTION upg_catalog.float8(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.oid) WITH FUNCTION dummy_cast_functions.dummycast_int2_oid_int4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.regclass) WITH FUNCTION dummy_cast_functions.dummycast_int2_regclass_int4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.regoper) WITH FUNCTION dummy_cast_functions.dummycast_int2_regoper_int4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.regoperator) WITH FUNCTION dummy_cast_functions.dummycast_int2_regoperator_int4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.regproc) WITH FUNCTION dummy_cast_functions.dummycast_int2_regproc_int4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.regprocedure) WITH FUNCTION dummy_cast_functions.dummycast_int2_regprocedure_int4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.regtype) WITH FUNCTION dummy_cast_functions.dummycast_int2_regtype_int4(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int2 AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.int2) AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog."bit") WITH FUNCTION upg_catalog.bit(upg_catalog.int4, pg_catalog.int4);
 CREATE CAST (upg_catalog.int4 AS upg_catalog."char") WITH FUNCTION upg_catalog.char(upg_catalog.int4);
 CREATE CAST (upg_catalog.int4 AS upg_catalog."numeric") WITH FUNCTION upg_catalog.numeric(upg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_int4_varchar_text(upg_catalog.int4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.abstime) WITHOUT FUNCTION;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.bool) WITH FUNCTION upg_catalog.bool(upg_catalog.int4);
 CREATE CAST (upg_catalog.int4 AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_int4_bpchar_text(upg_catalog.int4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.float4) WITH FUNCTION upg_catalog.float4(upg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.float8) WITH FUNCTION upg_catalog.float8(upg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.int2) WITH FUNCTION upg_catalog.int2(upg_catalog.int4) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.oid) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.regclass) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.regoper) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.regoperator) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.regproc) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.regprocedure) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.regtype) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.reltime) WITHOUT FUNCTION;
 CREATE CAST (upg_catalog.int4 AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog."bit") WITH FUNCTION upg_catalog.bit(upg_catalog.int8, pg_catalog.int4);
 CREATE CAST (upg_catalog.int8 AS upg_catalog."numeric") WITH FUNCTION upg_catalog.numeric(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_int8_varchar_text(upg_catalog.int8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_int8_bpchar_text(upg_catalog.int8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.float4) WITH FUNCTION upg_catalog.float4(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.float8) WITH FUNCTION upg_catalog.float8(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.int2) WITH FUNCTION upg_catalog.int2(upg_catalog.int8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog.int8) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.oid) WITH FUNCTION upg_catalog.oid(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.regclass) WITH FUNCTION dummy_cast_functions.dummycast_int8_regclass_oid(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.regoper) WITH FUNCTION dummy_cast_functions.dummycast_int8_regoper_oid(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.regoperator) WITH FUNCTION dummy_cast_functions.dummycast_int8_regoperator_oid(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.regproc) WITH FUNCTION dummy_cast_functions.dummycast_int8_regproc_oid(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.regprocedure) WITH FUNCTION dummy_cast_functions.dummycast_int8_regprocedure_oid(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.regtype) WITH FUNCTION dummy_cast_functions.dummycast_int8_regtype_oid(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.int8 AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.int8) AS IMPLICIT;
 CREATE CAST (upg_catalog.lseg AS upg_catalog.point) WITH FUNCTION upg_catalog.point(upg_catalog.lseg);
 CREATE CAST (upg_catalog.macaddr AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_macaddr_varchar_text(upg_catalog.macaddr);
 CREATE CAST (upg_catalog.macaddr AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_macaddr_bpchar_text(upg_catalog.macaddr);
 CREATE CAST (upg_catalog.macaddr AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.macaddr);
 CREATE CAST (upg_catalog.name AS upg_catalog."varchar") WITH FUNCTION upg_catalog.varchar(upg_catalog.name) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.name AS upg_catalog.bpchar) WITH FUNCTION upg_catalog.bpchar(upg_catalog.name) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.name AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.name) AS IMPLICIT;
 CREATE CAST (upg_catalog.oid AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_oid_varchar_text(upg_catalog.oid) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_oid_bpchar_text(upg_catalog.oid) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.int4) WITHOUT FUNCTION AS ASSIGNMENT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog.oid) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.regclass) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.regoper) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.regoperator) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.regproc) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.regprocedure) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.regtype) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.oid AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.oid) AS IMPLICIT;
 CREATE CAST (upg_catalog.path AS upg_catalog.point) WITH FUNCTION upg_catalog.point(upg_catalog.path);
 CREATE CAST (upg_catalog.path AS upg_catalog.polygon) WITH FUNCTION upg_catalog.polygon(upg_catalog.path) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.polygon AS upg_catalog.box) WITH FUNCTION upg_catalog.box(upg_catalog.polygon);
 CREATE CAST (upg_catalog.polygon AS upg_catalog.circle) WITH FUNCTION upg_catalog.circle(upg_catalog.polygon);
 CREATE CAST (upg_catalog.polygon AS upg_catalog.path) WITH FUNCTION upg_catalog.path(upg_catalog.polygon) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.polygon AS upg_catalog.point) WITH FUNCTION upg_catalog.point(upg_catalog.polygon);
 CREATE CAST (upg_catalog.regclass AS upg_catalog.int4) WITHOUT FUNCTION AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regclass AS upg_catalog.int8) WITH FUNCTION dummy_cast_functions.dummycast_regclass_int8_int8(upg_catalog.regclass) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regclass AS upg_catalog.oid) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regoper AS upg_catalog.int4) WITHOUT FUNCTION AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regoper AS upg_catalog.int8) WITH FUNCTION dummy_cast_functions.dummycast_regoper_int8_int8(upg_catalog.regoper) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regoper AS upg_catalog.oid) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regoper AS upg_catalog.regoperator) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regoperator AS upg_catalog.int4) WITHOUT FUNCTION AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regoperator AS upg_catalog.int8) WITH FUNCTION dummy_cast_functions.dummycast_regoperator_int8_int8(upg_catalog.regoperator) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regoperator AS upg_catalog.oid) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regoperator AS upg_catalog.regoper) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regproc AS upg_catalog.int4) WITHOUT FUNCTION AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regproc AS upg_catalog.int8) WITH FUNCTION dummy_cast_functions.dummycast_regproc_int8_int8(upg_catalog.regproc) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regproc AS upg_catalog.oid) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regproc AS upg_catalog.regprocedure) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regprocedure AS upg_catalog.int4) WITHOUT FUNCTION AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regprocedure AS upg_catalog.int8) WITH FUNCTION dummy_cast_functions.dummycast_regprocedure_int8_int8(upg_catalog.regprocedure) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regprocedure AS upg_catalog.oid) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regprocedure AS upg_catalog.regproc) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.regtype AS upg_catalog.int4) WITHOUT FUNCTION AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regtype AS upg_catalog.int8) WITH FUNCTION dummy_cast_functions.dummycast_regtype_int8_int8(upg_catalog.regtype) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.regtype AS upg_catalog.oid) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.reltime AS upg_catalog."interval") WITH FUNCTION upg_catalog.interval(upg_catalog.reltime) AS IMPLICIT;
 CREATE CAST (upg_catalog.reltime AS upg_catalog.int4) WITHOUT FUNCTION;
 CREATE CAST (upg_catalog.text AS upg_catalog."char") WITH FUNCTION upg_catalog.char(upg_catalog.text) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.text AS upg_catalog."interval") WITH FUNCTION upg_catalog.interval(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog."numeric") WITH FUNCTION upg_catalog.numeric(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog."time") WITH FUNCTION upg_catalog.time(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog."timestamp") WITH FUNCTION upg_catalog.timestamp(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog."varchar") WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.text AS upg_catalog.bpchar) WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.text AS upg_catalog.cidr) WITH FUNCTION upg_catalog.cidr(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.date) WITH FUNCTION upg_catalog.date(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.float4) WITH FUNCTION upg_catalog.float4(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.float8) WITH FUNCTION upg_catalog.float8(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.inet) WITH FUNCTION upg_catalog.inet(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.int2) WITH FUNCTION upg_catalog.int2(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.int4) WITH FUNCTION upg_catalog.int4(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.macaddr) WITH FUNCTION upg_catalog.macaddr(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.name) WITH FUNCTION upg_catalog.name(upg_catalog.text) AS IMPLICIT;
 CREATE CAST (upg_catalog.text AS upg_catalog.oid) WITH FUNCTION upg_catalog.oid(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.regclass) WITH FUNCTION upg_catalog.regclass(upg_catalog.text) AS IMPLICIT;
 CREATE CAST (upg_catalog.text AS upg_catalog.timestamptz) WITH FUNCTION upg_catalog.timestamptz(upg_catalog.text);
 CREATE CAST (upg_catalog.text AS upg_catalog.timetz) WITH FUNCTION upg_catalog.timetz(upg_catalog.text);
 CREATE CAST (upg_catalog.tid AS upg_catalog.gpaotid) WITHOUT FUNCTION;
 CREATE CAST (upg_catalog.tid AS upg_catalog.int8) WITH FUNCTION upg_catalog.int8(upg_catalog.tid);
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog."time") WITH FUNCTION upg_catalog.time(upg_catalog.timestamptz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog."timestamp") WITH FUNCTION upg_catalog.timestamp(upg_catalog.timestamptz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_timestamptz_varchar_text(upg_catalog.timestamptz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog.abstime) WITH FUNCTION upg_catalog.abstime(upg_catalog.timestamptz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_timestamptz_bpchar_text(upg_catalog.timestamptz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog.date) WITH FUNCTION upg_catalog.date(upg_catalog.timestamptz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.timestamptz) AS IMPLICIT;
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog.timestamptz) WITH FUNCTION upg_catalog.timestamptz(upg_catalog.timestamptz, pg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog.timestamptz AS upg_catalog.timetz) WITH FUNCTION upg_catalog.timetz(upg_catalog.timestamptz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timetz AS upg_catalog."time") WITH FUNCTION upg_catalog.time(upg_catalog.timetz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timetz AS upg_catalog."varchar") WITH FUNCTION dummy_cast_functions.dummycast_timetz_varchar_text(upg_catalog.timetz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timetz AS upg_catalog.bpchar) WITH FUNCTION dummy_cast_functions.dummycast_timetz_bpchar_text(upg_catalog.timetz) AS ASSIGNMENT;
 CREATE CAST (upg_catalog.timetz AS upg_catalog.text) WITH FUNCTION upg_catalog.text(upg_catalog.timetz) AS IMPLICIT;
 CREATE CAST (upg_catalog.timetz AS upg_catalog.timetz) WITH FUNCTION upg_catalog.timetz(upg_catalog.timetz, pg_catalog.int4) AS IMPLICIT;
 CREATE CAST (upg_catalog.varbit AS upg_catalog."bit") WITHOUT FUNCTION AS IMPLICIT;
 CREATE CAST (upg_catalog.varbit AS upg_catalog.varbit) WITH FUNCTION upg_catalog.varbit(upg_catalog.varbit, pg_catalog.int4, pg_catalog.bool) AS IMPLICIT;

