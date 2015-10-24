-- Cast Functions defined in the 4.0 Catalog:
--
-- Derived via the following SQL run within a 4.0 catalog
--
/*
\o /tmp/functions
SELECT 
    'CREATE FUNCTION upg_catalog.' || quote_ident(proname)
    || '(' 
    || coalesce(
        array_to_string( array(
                     select coalesce(proargnames[i] || ' ','') 
                           || case when i > 1 
                                   then 'pg_catalog.' 
                                   else 'upg_catalog.' end
                           || quote_ident(typname)
                     from pg_type t, generate_series(1, pronargs) i
                     where t.oid = proargtypes[i-1]
                     order by i), ', '), '')
    || ') RETURNS '
    || case when proretset then 'SETOF ' else '' end 
    || 'upg_catalog.' || quote_ident(r.typname) || ' '
    || 'LANGUAGE ' || quote_ident(lanname) || ' '
    || case when provolatile = 'i' then 'IMMUTABLE '
            when provolatile = 'v' then 'VOLATILE '
            when provolatile = 's' then 'STABLE ' 
            else '' end
    || case when proisstrict then 'STRICT ' else '' end
    || case when prosecdef then 'SECURITY DEFINER ' else '' end
    || 'AS ' 
    || case when lanname = 'c' 
            then '''' || textin(byteaout(probin)) || ''', ''' || prosrc || '''' 
            when lanname = 'internal'
            then '''' || prosrc || ''''
            when lanname = 'sql'
            then '$$ SELECT null::upg_catalog.' || quote_ident(r.typname) || '$$'
            else 'BROKEN (unsupported language)' end
    || ';'
FROM pg_proc p
JOIN pg_type r on (p.prorettype = r.oid)
JOIN pg_language l on (p.prolang = l.oid)
JOIN pg_namespace n on (p.pronamespace = n.oid)
WHERE n.nspname = 'pg_catalog'
  and p.oid in (select castfunc from pg_cast)
order by 1
;
*/
 CREATE FUNCTION upg_catalog."bit"(upg_catalog."bit", pg_catalog.int4, pg_catalog.bool) RETURNS upg_catalog."bit" LANGUAGE internal IMMUTABLE STRICT AS 'bit';
 CREATE FUNCTION upg_catalog."bit"(upg_catalog.int4, pg_catalog.int4) RETURNS upg_catalog."bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitfromint4';
 CREATE FUNCTION upg_catalog."bit"(upg_catalog.int8, pg_catalog.int4) RETURNS upg_catalog."bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitfromint8';
 CREATE FUNCTION upg_catalog."char"(upg_catalog.int4) RETURNS upg_catalog."char" LANGUAGE internal IMMUTABLE STRICT AS 'i4tochar';
 CREATE FUNCTION upg_catalog."char"(upg_catalog.text) RETURNS upg_catalog."char" LANGUAGE internal IMMUTABLE STRICT AS 'text_char';
 CREATE FUNCTION upg_catalog."interval"(upg_catalog."interval", pg_catalog.int4) RETURNS upg_catalog."interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_scale';
 CREATE FUNCTION upg_catalog."interval"(upg_catalog."time") RETURNS upg_catalog."interval" LANGUAGE internal IMMUTABLE STRICT AS 'time_interval';
 CREATE FUNCTION upg_catalog."interval"(upg_catalog.reltime) RETURNS upg_catalog."interval" LANGUAGE internal IMMUTABLE STRICT AS 'reltime_interval';
 CREATE FUNCTION upg_catalog."interval"(upg_catalog.text) RETURNS upg_catalog."interval" LANGUAGE internal STABLE STRICT AS 'text_interval';
 CREATE FUNCTION upg_catalog."numeric"(upg_catalog."numeric", pg_catalog.int4) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric';
 CREATE FUNCTION upg_catalog."numeric"(upg_catalog.float4) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'float4_numeric';
 CREATE FUNCTION upg_catalog."numeric"(upg_catalog.float8) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'float8_numeric';
 CREATE FUNCTION upg_catalog."numeric"(upg_catalog.int2) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'int2_numeric';
 CREATE FUNCTION upg_catalog."numeric"(upg_catalog.int4) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'int4_numeric';
 CREATE FUNCTION upg_catalog."numeric"(upg_catalog.int8) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'int8_numeric';
 CREATE FUNCTION upg_catalog."numeric"(upg_catalog.text) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'text_numeric';
 CREATE FUNCTION upg_catalog."time"(upg_catalog."interval") RETURNS upg_catalog."time" LANGUAGE internal IMMUTABLE STRICT AS 'interval_time';
 CREATE FUNCTION upg_catalog."time"(upg_catalog."time", pg_catalog.int4) RETURNS upg_catalog."time" LANGUAGE internal IMMUTABLE STRICT AS 'time_scale';
 CREATE FUNCTION upg_catalog."time"(upg_catalog."timestamp") RETURNS upg_catalog."time" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_time';
 CREATE FUNCTION upg_catalog."time"(upg_catalog.abstime) RETURNS upg_catalog."time" LANGUAGE sql STABLE STRICT AS $$ SELECT null::upg_catalog."time"$$;
 CREATE FUNCTION upg_catalog."time"(upg_catalog.text) RETURNS upg_catalog."time" LANGUAGE internal STABLE STRICT AS 'text_time';
 CREATE FUNCTION upg_catalog."time"(upg_catalog.timestamptz) RETURNS upg_catalog."time" LANGUAGE internal STABLE STRICT AS 'timestamptz_time';
 CREATE FUNCTION upg_catalog."time"(upg_catalog.timetz) RETURNS upg_catalog."time" LANGUAGE internal IMMUTABLE STRICT AS 'timetz_time';
 CREATE FUNCTION upg_catalog."timestamp"(upg_catalog."timestamp", pg_catalog.int4) RETURNS upg_catalog."timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_scale';
 CREATE FUNCTION upg_catalog."timestamp"(upg_catalog.abstime) RETURNS upg_catalog."timestamp" LANGUAGE internal STABLE STRICT AS 'abstime_timestamp';
 CREATE FUNCTION upg_catalog."timestamp"(upg_catalog.date) RETURNS upg_catalog."timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'date_timestamp';
 CREATE FUNCTION upg_catalog."timestamp"(upg_catalog.text) RETURNS upg_catalog."timestamp" LANGUAGE internal STABLE STRICT AS 'text_timestamp';
 CREATE FUNCTION upg_catalog."timestamp"(upg_catalog.timestamptz) RETURNS upg_catalog."timestamp" LANGUAGE internal STABLE STRICT AS 'timestamptz_timestamp';
 CREATE FUNCTION upg_catalog."varchar"(upg_catalog."varchar", pg_catalog.int4, pg_catalog.bool) RETURNS upg_catalog."varchar" LANGUAGE internal IMMUTABLE STRICT AS 'varchar';
 CREATE FUNCTION upg_catalog."varchar"(upg_catalog.name) RETURNS upg_catalog."varchar" LANGUAGE internal IMMUTABLE STRICT AS 'name_text';
 CREATE FUNCTION upg_catalog.abstime(upg_catalog."timestamp") RETURNS upg_catalog.abstime LANGUAGE internal STABLE STRICT AS 'timestamp_abstime';
 CREATE FUNCTION upg_catalog.abstime(upg_catalog.timestamptz) RETURNS upg_catalog.abstime LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_abstime';
 CREATE FUNCTION upg_catalog.bool(upg_catalog.int4) RETURNS upg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int4_bool';
 CREATE FUNCTION upg_catalog.box(upg_catalog.circle) RETURNS upg_catalog.box LANGUAGE internal IMMUTABLE STRICT AS 'circle_box';
 CREATE FUNCTION upg_catalog.box(upg_catalog.polygon) RETURNS upg_catalog.box LANGUAGE internal IMMUTABLE STRICT AS 'poly_box';
 CREATE FUNCTION upg_catalog.bpchar(upg_catalog."char") RETURNS upg_catalog.bpchar LANGUAGE internal IMMUTABLE STRICT AS 'char_bpchar';
 CREATE FUNCTION upg_catalog.bpchar(upg_catalog.bpchar, pg_catalog.int4, pg_catalog.bool) RETURNS upg_catalog.bpchar LANGUAGE internal IMMUTABLE STRICT AS 'bpchar';
 CREATE FUNCTION upg_catalog.bpchar(upg_catalog.name) RETURNS upg_catalog.bpchar LANGUAGE internal IMMUTABLE STRICT AS 'name_bpchar';
 CREATE FUNCTION upg_catalog.cidr(upg_catalog.inet) RETURNS upg_catalog.cidr LANGUAGE internal IMMUTABLE STRICT AS 'inet_to_cidr';
 CREATE FUNCTION upg_catalog.cidr(upg_catalog.text) RETURNS upg_catalog.cidr LANGUAGE internal IMMUTABLE STRICT AS 'text_cidr';
 CREATE FUNCTION upg_catalog.circle(upg_catalog.box) RETURNS upg_catalog.circle LANGUAGE internal IMMUTABLE STRICT AS 'box_circle';
 CREATE FUNCTION upg_catalog.circle(upg_catalog.polygon) RETURNS upg_catalog.circle LANGUAGE internal IMMUTABLE STRICT AS 'poly_circle';
 CREATE FUNCTION upg_catalog.date(upg_catalog."timestamp") RETURNS upg_catalog.date LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_date';
 CREATE FUNCTION upg_catalog.date(upg_catalog.abstime) RETURNS upg_catalog.date LANGUAGE internal STABLE STRICT AS 'abstime_date';
 CREATE FUNCTION upg_catalog.date(upg_catalog.text) RETURNS upg_catalog.date LANGUAGE internal STABLE STRICT AS 'text_date';
 CREATE FUNCTION upg_catalog.date(upg_catalog.timestamptz) RETURNS upg_catalog.date LANGUAGE internal STABLE STRICT AS 'timestamptz_date';
 CREATE FUNCTION upg_catalog.float4(upg_catalog."numeric") RETURNS upg_catalog.float4 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_float4';
 CREATE FUNCTION upg_catalog.float4(upg_catalog.float8) RETURNS upg_catalog.float4 LANGUAGE internal IMMUTABLE STRICT AS 'dtof';
 CREATE FUNCTION upg_catalog.float4(upg_catalog.int2) RETURNS upg_catalog.float4 LANGUAGE internal IMMUTABLE STRICT AS 'i2tof';
 CREATE FUNCTION upg_catalog.float4(upg_catalog.int4) RETURNS upg_catalog.float4 LANGUAGE internal IMMUTABLE STRICT AS 'i4tof';
 CREATE FUNCTION upg_catalog.float4(upg_catalog.int8) RETURNS upg_catalog.float4 LANGUAGE internal IMMUTABLE STRICT AS 'i8tof';
 CREATE FUNCTION upg_catalog.float4(upg_catalog.text) RETURNS upg_catalog.float4 LANGUAGE internal IMMUTABLE STRICT AS 'text_float4';
 CREATE FUNCTION upg_catalog.float8(upg_catalog."numeric") RETURNS upg_catalog.float8 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_float8';
 CREATE FUNCTION upg_catalog.float8(upg_catalog.float4) RETURNS upg_catalog.float8 LANGUAGE internal IMMUTABLE STRICT AS 'ftod';
 CREATE FUNCTION upg_catalog.float8(upg_catalog.int2) RETURNS upg_catalog.float8 LANGUAGE internal IMMUTABLE STRICT AS 'i2tod';
 CREATE FUNCTION upg_catalog.float8(upg_catalog.int4) RETURNS upg_catalog.float8 LANGUAGE internal IMMUTABLE STRICT AS 'i4tod';
 CREATE FUNCTION upg_catalog.float8(upg_catalog.int8) RETURNS upg_catalog.float8 LANGUAGE internal IMMUTABLE STRICT AS 'i8tod';
 CREATE FUNCTION upg_catalog.float8(upg_catalog.text) RETURNS upg_catalog.float8 LANGUAGE internal IMMUTABLE STRICT AS 'text_float8';
 CREATE FUNCTION upg_catalog.inet(upg_catalog.text) RETURNS upg_catalog.inet LANGUAGE internal IMMUTABLE STRICT AS 'text_inet';
 CREATE FUNCTION upg_catalog.int2(upg_catalog."numeric") RETURNS upg_catalog.int2 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_int2';
 CREATE FUNCTION upg_catalog.int2(upg_catalog.float4) RETURNS upg_catalog.int2 LANGUAGE internal IMMUTABLE STRICT AS 'ftoi2';
 CREATE FUNCTION upg_catalog.int2(upg_catalog.float8) RETURNS upg_catalog.int2 LANGUAGE internal IMMUTABLE STRICT AS 'dtoi2';
 CREATE FUNCTION upg_catalog.int2(upg_catalog.int4) RETURNS upg_catalog.int2 LANGUAGE internal IMMUTABLE STRICT AS 'i4toi2';
 CREATE FUNCTION upg_catalog.int2(upg_catalog.int8) RETURNS upg_catalog.int2 LANGUAGE internal IMMUTABLE STRICT AS 'int82';
 CREATE FUNCTION upg_catalog.int2(upg_catalog.text) RETURNS upg_catalog.int2 LANGUAGE internal IMMUTABLE STRICT AS 'text_int2';
 CREATE FUNCTION upg_catalog.int4(upg_catalog."bit") RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bittoint4';
 CREATE FUNCTION upg_catalog.int4(upg_catalog."char") RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'chartoi4';
 CREATE FUNCTION upg_catalog.int4(upg_catalog."numeric") RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_int4';
 CREATE FUNCTION upg_catalog.int4(upg_catalog.bool) RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bool_int4';
 CREATE FUNCTION upg_catalog.int4(upg_catalog.float4) RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'ftoi4';
 CREATE FUNCTION upg_catalog.int4(upg_catalog.float8) RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'dtoi4';
 CREATE FUNCTION upg_catalog.int4(upg_catalog.int2) RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'i2toi4';
 CREATE FUNCTION upg_catalog.int4(upg_catalog.int8) RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'int84';
 CREATE FUNCTION upg_catalog.int4(upg_catalog.text) RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'text_int4';
 CREATE FUNCTION upg_catalog.int8(upg_catalog."bit") RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'bittoint8';
 CREATE FUNCTION upg_catalog.int8(upg_catalog."numeric") RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_int8';
 CREATE FUNCTION upg_catalog.int8(upg_catalog.float4) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'ftoi8';
 CREATE FUNCTION upg_catalog.int8(upg_catalog.float8) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'dtoi8';
 CREATE FUNCTION upg_catalog.int8(upg_catalog.int2) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'int28';
 CREATE FUNCTION upg_catalog.int8(upg_catalog.int4) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'int48';
 CREATE FUNCTION upg_catalog.int8(upg_catalog.oid) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'oidtoi8';
 CREATE FUNCTION upg_catalog.int8(upg_catalog.text) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'text_int8';
 CREATE FUNCTION upg_catalog.int8(upg_catalog.tid) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'tidtoi8';
 CREATE FUNCTION upg_catalog.lseg(upg_catalog.box) RETURNS upg_catalog.lseg LANGUAGE internal IMMUTABLE STRICT AS 'box_diagonal';
 CREATE FUNCTION upg_catalog.macaddr(upg_catalog.text) RETURNS upg_catalog.macaddr LANGUAGE internal IMMUTABLE STRICT AS 'text_macaddr';
 CREATE FUNCTION upg_catalog.name(upg_catalog."varchar") RETURNS upg_catalog.name LANGUAGE internal IMMUTABLE STRICT AS 'text_name';
 CREATE FUNCTION upg_catalog.name(upg_catalog.bpchar) RETURNS upg_catalog.name LANGUAGE internal IMMUTABLE STRICT AS 'bpchar_name';
 CREATE FUNCTION upg_catalog.name(upg_catalog.text) RETURNS upg_catalog.name LANGUAGE internal IMMUTABLE STRICT AS 'text_name';
 CREATE FUNCTION upg_catalog.oid(upg_catalog.int8) RETURNS upg_catalog.oid LANGUAGE internal IMMUTABLE STRICT AS 'i8tooid';
 CREATE FUNCTION upg_catalog.oid(upg_catalog.text) RETURNS upg_catalog.oid LANGUAGE internal IMMUTABLE STRICT AS 'text_oid';
 CREATE FUNCTION upg_catalog.path(upg_catalog.polygon) RETURNS upg_catalog.path LANGUAGE internal IMMUTABLE STRICT AS 'poly_path';
 CREATE FUNCTION upg_catalog.point(upg_catalog.box) RETURNS upg_catalog.point LANGUAGE internal IMMUTABLE STRICT AS 'box_center';
 CREATE FUNCTION upg_catalog.point(upg_catalog.circle) RETURNS upg_catalog.point LANGUAGE internal IMMUTABLE STRICT AS 'circle_center';
 CREATE FUNCTION upg_catalog.point(upg_catalog.lseg) RETURNS upg_catalog.point LANGUAGE internal IMMUTABLE STRICT AS 'lseg_center';
 CREATE FUNCTION upg_catalog.point(upg_catalog.path) RETURNS upg_catalog.point LANGUAGE internal IMMUTABLE STRICT AS 'path_center';
 CREATE FUNCTION upg_catalog.point(upg_catalog.polygon) RETURNS upg_catalog.point LANGUAGE internal IMMUTABLE STRICT AS 'poly_center';
 CREATE FUNCTION upg_catalog.polygon(upg_catalog.box) RETURNS upg_catalog.polygon LANGUAGE internal IMMUTABLE STRICT AS 'box_poly';
 CREATE FUNCTION upg_catalog.polygon(upg_catalog.circle) RETURNS upg_catalog.polygon LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT null::upg_catalog.polygon$$;
 CREATE FUNCTION upg_catalog.polygon(upg_catalog.path) RETURNS upg_catalog.polygon LANGUAGE internal IMMUTABLE STRICT AS 'path_poly';
 CREATE FUNCTION upg_catalog.regclass(upg_catalog.text) RETURNS upg_catalog.regclass LANGUAGE internal STABLE STRICT AS 'text_regclass';
 CREATE FUNCTION upg_catalog.reltime(upg_catalog."interval") RETURNS upg_catalog.reltime LANGUAGE internal IMMUTABLE STRICT AS 'interval_reltime';
 CREATE FUNCTION upg_catalog.text(upg_catalog."char") RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'char_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog."interval") RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'interval_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog."numeric") RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'numeric_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog."time") RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'time_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog."timestamp") RETURNS upg_catalog.text LANGUAGE internal STABLE STRICT AS 'timestamp_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.bpchar) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'rtrim1';
 CREATE FUNCTION upg_catalog.text(upg_catalog.date) RETURNS upg_catalog.text LANGUAGE internal STABLE STRICT AS 'date_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.float4) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'float4_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.float8) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'float8_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.inet) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'network_show';
 CREATE FUNCTION upg_catalog.text(upg_catalog.int2) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'int2_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.int4) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'int4_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.int8) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'int8_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.macaddr) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.name) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'name_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.oid) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'oid_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.timestamptz) RETURNS upg_catalog.text LANGUAGE internal STABLE STRICT AS 'timestamptz_text';
 CREATE FUNCTION upg_catalog.text(upg_catalog.timetz) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'timetz_text';
 CREATE FUNCTION upg_catalog.timestamptz(upg_catalog."timestamp") RETURNS upg_catalog.timestamptz LANGUAGE internal STABLE STRICT AS 'timestamp_timestamptz';
 CREATE FUNCTION upg_catalog.timestamptz(upg_catalog.abstime) RETURNS upg_catalog.timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'abstime_timestamptz';
 CREATE FUNCTION upg_catalog.timestamptz(upg_catalog.date) RETURNS upg_catalog.timestamptz LANGUAGE internal STABLE STRICT AS 'date_timestamptz';
 CREATE FUNCTION upg_catalog.timestamptz(upg_catalog.text) RETURNS upg_catalog.timestamptz LANGUAGE internal STABLE STRICT AS 'text_timestamptz';
 CREATE FUNCTION upg_catalog.timestamptz(upg_catalog.timestamptz, pg_catalog.int4) RETURNS upg_catalog.timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_scale';
 CREATE FUNCTION upg_catalog.timetz(upg_catalog."time") RETURNS upg_catalog.timetz LANGUAGE internal STABLE STRICT AS 'time_timetz';
 CREATE FUNCTION upg_catalog.timetz(upg_catalog.text) RETURNS upg_catalog.timetz LANGUAGE internal STABLE STRICT AS 'text_timetz';
 CREATE FUNCTION upg_catalog.timetz(upg_catalog.timestamptz) RETURNS upg_catalog.timetz LANGUAGE internal STABLE STRICT AS 'timestamptz_timetz';
 CREATE FUNCTION upg_catalog.timetz(upg_catalog.timetz, pg_catalog.int4) RETURNS upg_catalog.timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_scale';
 CREATE FUNCTION upg_catalog.varbit(upg_catalog.varbit, pg_catalog.int4, pg_catalog.bool) RETURNS upg_catalog.varbit LANGUAGE internal IMMUTABLE STRICT AS 'varbit';

