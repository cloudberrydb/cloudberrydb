-- dummy_cast_functions.sql
--
-- Creates extra dummy functions in a dummy schema to support creating
-- otherwise impossible to create casts.  (see casts.sql)
--
-- Derived via the following SQL run within a 4.0 catalog
/*
\o /tmp/functions
SELECT 'CREATE FUNCTION '
    || 'dummy_cast_functions.dummycast_'||t1.typname||'_'||t2.typname||'_'||p.proname||'('
    || 'upg_catalog.' || quote_ident(t1.typname) || ') '
    || 'RETURNS upg_catalog.' || quote_ident(t2.typname) || ' '
    || 'LANGUAGE internal AS ''' || p.prosrc || ''' STRICT IMMUTABLE;'
FROM pg_cast c
     join pg_type t1 on (c.castsource = t1.oid)
     join pg_type t2 on (c.casttarget = t2.oid)
     join pg_proc p on (c.castfunc = p.oid)
     join pg_namespace n on (p.pronamespace = n.oid)
WHERE n.nspname = 'pg_catalog'
  AND p.proargtypes[0] != c.castsource OR p.prorettype != c.casttarget
ORDER BY 1;
*/
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_char_char(upg_catalog.bpchar) RETURNS upg_catalog."char" LANGUAGE internal AS 'text_char' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_date_date(upg_catalog.bpchar) RETURNS upg_catalog.date LANGUAGE internal AS 'text_date' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_float4_float4(upg_catalog.bpchar) RETURNS upg_catalog.float4 LANGUAGE internal AS 'text_float4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_float8_float8(upg_catalog.bpchar) RETURNS upg_catalog.float8 LANGUAGE internal AS 'text_float8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_int2_int2(upg_catalog.bpchar) RETURNS upg_catalog.int2 LANGUAGE internal AS 'text_int2' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_int4_int4(upg_catalog.bpchar) RETURNS upg_catalog.int4 LANGUAGE internal AS 'text_int4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_int8_int8(upg_catalog.bpchar) RETURNS upg_catalog.int8 LANGUAGE internal AS 'text_int8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_interval_interval(upg_catalog.bpchar) RETURNS upg_catalog."interval" LANGUAGE internal AS 'text_interval' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_macaddr_macaddr(upg_catalog.bpchar) RETURNS upg_catalog.macaddr LANGUAGE internal AS 'text_macaddr' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_numeric_numeric(upg_catalog.bpchar) RETURNS upg_catalog."numeric" LANGUAGE internal AS 'text_numeric' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_oid_oid(upg_catalog.bpchar) RETURNS upg_catalog.oid LANGUAGE internal AS 'text_oid' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_time_time(upg_catalog.bpchar) RETURNS upg_catalog."time" LANGUAGE internal AS 'text_time' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_timestamp_timestamp(upg_catalog.bpchar) RETURNS upg_catalog."timestamp" LANGUAGE internal AS 'text_timestamp' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_timestamptz_timestamptz(upg_catalog.bpchar) RETURNS upg_catalog.timestamptz LANGUAGE internal AS 'text_timestamptz' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_timetz_timetz(upg_catalog.bpchar) RETURNS upg_catalog.timetz LANGUAGE internal AS 'text_timetz' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_bpchar_varchar_text(upg_catalog.bpchar) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'rtrim1' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_char_varchar_text(upg_catalog."char") RETURNS upg_catalog."varchar" LANGUAGE internal AS 'char_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_cidr_bpchar_text(upg_catalog.cidr) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'network_show' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_cidr_text_text(upg_catalog.cidr) RETURNS upg_catalog.text LANGUAGE internal AS 'network_show' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_cidr_varchar_text(upg_catalog.cidr) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'network_show' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_date_bpchar_text(upg_catalog.date) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'date_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_date_varchar_text(upg_catalog.date) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'date_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_float4_bpchar_text(upg_catalog.float4) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'float4_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_float4_varchar_text(upg_catalog.float4) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'float4_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_float8_bpchar_text(upg_catalog.float8) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'float8_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_float8_varchar_text(upg_catalog.float8) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'float8_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_inet_bpchar_text(upg_catalog.inet) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'network_show' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_inet_varchar_text(upg_catalog.inet) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'network_show' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_bpchar_text(upg_catalog.int2) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'int2_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_oid_int4(upg_catalog.int2) RETURNS upg_catalog.oid LANGUAGE internal AS 'i2toi4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_regclass_int4(upg_catalog.int2) RETURNS upg_catalog.regclass LANGUAGE internal AS 'i2toi4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_regoper_int4(upg_catalog.int2) RETURNS upg_catalog.regoper LANGUAGE internal AS 'i2toi4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_regoperator_int4(upg_catalog.int2) RETURNS upg_catalog.regoperator LANGUAGE internal AS 'i2toi4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_regproc_int4(upg_catalog.int2) RETURNS upg_catalog.regproc LANGUAGE internal AS 'i2toi4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_regprocedure_int4(upg_catalog.int2) RETURNS upg_catalog.regprocedure LANGUAGE internal AS 'i2toi4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_regtype_int4(upg_catalog.int2) RETURNS upg_catalog.regtype LANGUAGE internal AS 'i2toi4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int2_varchar_text(upg_catalog.int2) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'int2_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int4_bpchar_text(upg_catalog.int4) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'int4_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int4_varchar_text(upg_catalog.int4) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'int4_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int8_bpchar_text(upg_catalog.int8) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'int8_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int8_regclass_oid(upg_catalog.int8) RETURNS upg_catalog.regclass LANGUAGE internal AS 'i8tooid' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int8_regoper_oid(upg_catalog.int8) RETURNS upg_catalog.regoper LANGUAGE internal AS 'i8tooid' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int8_regoperator_oid(upg_catalog.int8) RETURNS upg_catalog.regoperator LANGUAGE internal AS 'i8tooid' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int8_regproc_oid(upg_catalog.int8) RETURNS upg_catalog.regproc LANGUAGE internal AS 'i8tooid' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int8_regprocedure_oid(upg_catalog.int8) RETURNS upg_catalog.regprocedure LANGUAGE internal AS 'i8tooid' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int8_regtype_oid(upg_catalog.int8) RETURNS upg_catalog.regtype LANGUAGE internal AS 'i8tooid' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_int8_varchar_text(upg_catalog.int8) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'int8_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_interval_bpchar_text(upg_catalog."interval") RETURNS upg_catalog.bpchar LANGUAGE internal AS 'interval_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_interval_varchar_text(upg_catalog."interval") RETURNS upg_catalog."varchar" LANGUAGE internal AS 'interval_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_macaddr_bpchar_text(upg_catalog.macaddr) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'macaddr_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_macaddr_varchar_text(upg_catalog.macaddr) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'macaddr_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_numeric_bpchar_text(upg_catalog."numeric") RETURNS upg_catalog.bpchar LANGUAGE internal AS 'numeric_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_numeric_varchar_text(upg_catalog."numeric") RETURNS upg_catalog."varchar" LANGUAGE internal AS 'numeric_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_oid_bpchar_text(upg_catalog.oid) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'oid_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_oid_varchar_text(upg_catalog.oid) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'oid_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_regclass_int8_int8(upg_catalog.regclass) RETURNS upg_catalog.int8 LANGUAGE internal AS 'oidtoi8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_regoper_int8_int8(upg_catalog.regoper) RETURNS upg_catalog.int8 LANGUAGE internal AS 'oidtoi8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_regoperator_int8_int8(upg_catalog.regoperator) RETURNS upg_catalog.int8 LANGUAGE internal AS 'oidtoi8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_regproc_int8_int8(upg_catalog.regproc) RETURNS upg_catalog.int8 LANGUAGE internal AS 'oidtoi8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_regprocedure_int8_int8(upg_catalog.regprocedure) RETURNS upg_catalog.int8 LANGUAGE internal AS 'oidtoi8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_regtype_int8_int8(upg_catalog.regtype) RETURNS upg_catalog.int8 LANGUAGE internal AS 'oidtoi8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_time_bpchar_text(upg_catalog."time") RETURNS upg_catalog.bpchar LANGUAGE internal AS 'time_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_time_varchar_text(upg_catalog."time") RETURNS upg_catalog."varchar" LANGUAGE internal AS 'time_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_timestamp_bpchar_text(upg_catalog."timestamp") RETURNS upg_catalog.bpchar LANGUAGE internal AS 'timestamp_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_timestamp_varchar_text(upg_catalog."timestamp") RETURNS upg_catalog."varchar" LANGUAGE internal AS 'timestamp_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_timestamptz_bpchar_text(upg_catalog.timestamptz) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'timestamptz_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_timestamptz_varchar_text(upg_catalog.timestamptz) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'timestamptz_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_timetz_bpchar_text(upg_catalog.timetz) RETURNS upg_catalog.bpchar LANGUAGE internal AS 'timetz_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_timetz_varchar_text(upg_catalog.timetz) RETURNS upg_catalog."varchar" LANGUAGE internal AS 'timetz_text' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_char_char(upg_catalog."varchar") RETURNS upg_catalog."char" LANGUAGE internal AS 'text_char' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_cidr_cidr(upg_catalog."varchar") RETURNS upg_catalog.cidr LANGUAGE internal AS 'text_cidr' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_date_date(upg_catalog."varchar") RETURNS upg_catalog.date LANGUAGE internal AS 'text_date' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_float4_float4(upg_catalog."varchar") RETURNS upg_catalog.float4 LANGUAGE internal AS 'text_float4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_float8_float8(upg_catalog."varchar") RETURNS upg_catalog.float8 LANGUAGE internal AS 'text_float8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_inet_inet(upg_catalog."varchar") RETURNS upg_catalog.inet LANGUAGE internal AS 'text_inet' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_int2_int2(upg_catalog."varchar") RETURNS upg_catalog.int2 LANGUAGE internal AS 'text_int2' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_int4_int4(upg_catalog."varchar") RETURNS upg_catalog.int4 LANGUAGE internal AS 'text_int4' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_int8_int8(upg_catalog."varchar") RETURNS upg_catalog.int8 LANGUAGE internal AS 'text_int8' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_interval_interval(upg_catalog."varchar") RETURNS upg_catalog."interval" LANGUAGE internal AS 'text_interval' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_macaddr_macaddr(upg_catalog."varchar") RETURNS upg_catalog.macaddr LANGUAGE internal AS 'text_macaddr' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_numeric_numeric(upg_catalog."varchar") RETURNS upg_catalog."numeric" LANGUAGE internal AS 'text_numeric' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_oid_oid(upg_catalog."varchar") RETURNS upg_catalog.oid LANGUAGE internal AS 'text_oid' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_regclass_regclass(upg_catalog."varchar") RETURNS upg_catalog.regclass LANGUAGE internal AS 'text_regclass' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_time_time(upg_catalog."varchar") RETURNS upg_catalog."time" LANGUAGE internal AS 'text_time' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_timestamp_timestamp(upg_catalog."varchar") RETURNS upg_catalog."timestamp" LANGUAGE internal AS 'text_timestamp' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_timestamptz_timestamptz(upg_catalog."varchar") RETURNS upg_catalog.timestamptz LANGUAGE internal AS 'text_timestamptz' STRICT IMMUTABLE;
 CREATE FUNCTION dummy_cast_functions.dummycast_varchar_timetz_timetz(upg_catalog."varchar") RETURNS upg_catalog.timetz LANGUAGE internal AS 'text_timetz' STRICT IMMUTABLE;

