-- SQL language Functions defined in the 4.0 Catalog:
--
-- Derived via the following SQL run within a 4.0 catalog
--
-- NOTE: The SQL language functions all require use of the actual catalog types,
-- but this prohibits our ability to use them as definitions of other objects.
-- To handle this we rewrite all of them to return null, which is a less than
-- ideal solution for our testing, but it gets around a lot of issues.  This
-- forces us to use alternate means of verifying that there have been no changes
-- to the definition of SQL language functions.
--
/*
\o /tmp/functions
SELECT 
    'CREATE FUNCTION upg_catalog.'
    || case when proname in ('convert') 
            then '"' || proname || '"' 
            else quote_ident(proname) end
    || '(' 
    || coalesce(
        array_to_string( 
            case when proargmodes is null 
                 then array(
                     select coalesce(proargnames[i] || ' ','') 
                           || 'upg_catalog.' || quote_ident(typname)
                     from pg_type t, generate_series(1, pronargs) i
                     where t.oid = proargtypes[i-1]
                     order by i)
                 else array(
                     select   case when proargmodes[i] = 'i' then 'IN '
                                   when proargmodes[i] = 'o' then 'OUT '
                                   when proargmodes[i] = 'b' then 'INOUT '
                                   else 'BROKEN(proargmode)' end
                           || coalesce(proargnames[i] || ' ','') 
                           || 'upg_catalog.' || quote_ident(typname)
                     from pg_type t, generate_series(1, array_upper(proargmodes, 1)) i
                     where t.oid = proallargtypes[i]
                     order by i) 
                 end, ', '), '')
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
    || 'AS ''select null::upg_catalog.' || quote_ident(r.typname) || ''';'
FROM pg_proc p
JOIN pg_type r on (p.prorettype = r.oid)
JOIN pg_language l on (p.prolang = l.oid)
JOIN pg_namespace n on (p.pronamespace = n.oid)
WHERE n.nspname = 'pg_catalog'
  and proisagg = 'f'
  and proiswin = 'f'
  and p.oid not in (select unnest(array[typinput, typoutput, typreceive, typsend, typanalyze])
                    from pg_type)
  and p.oid not in (select castfunc from pg_cast)
  and p.oid not in (select conproc from pg_conversion)
  and p.oid not in (select unnest(array[oprrest, oprjoin]) from pg_operator)
  and p.oid not in (select unnest(array['pg_catalog.shell_in'::regproc, 'pg_catalog.shell_out'::regproc]))
  and lanname = 'sql'
order by 1;
*/
 CREATE FUNCTION upg_catalog."log"(upg_catalog."numeric") RETURNS upg_catalog."numeric" LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog."numeric"';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog."time", upg_catalog."interval", upg_catalog."time", upg_catalog."interval") RETURNS upg_catalog.bool LANGUAGE sql IMMUTABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog."time", upg_catalog."interval", upg_catalog."time", upg_catalog."time") RETURNS upg_catalog.bool LANGUAGE sql IMMUTABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog."time", upg_catalog."time", upg_catalog."time", upg_catalog."interval") RETURNS upg_catalog.bool LANGUAGE sql IMMUTABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog."timestamp", upg_catalog."interval", upg_catalog."timestamp", upg_catalog."interval") RETURNS upg_catalog.bool LANGUAGE sql IMMUTABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog."timestamp", upg_catalog."interval", upg_catalog."timestamp", upg_catalog."timestamp") RETURNS upg_catalog.bool LANGUAGE sql IMMUTABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog."timestamp", upg_catalog."timestamp", upg_catalog."timestamp", upg_catalog."interval") RETURNS upg_catalog.bool LANGUAGE sql IMMUTABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog.timestamptz, upg_catalog."interval", upg_catalog.timestamptz, upg_catalog."interval") RETURNS upg_catalog.bool LANGUAGE sql STABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog.timestamptz, upg_catalog."interval", upg_catalog.timestamptz, upg_catalog.timestamptz) RETURNS upg_catalog.bool LANGUAGE sql STABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlaps"(upg_catalog.timestamptz, upg_catalog.timestamptz, upg_catalog.timestamptz, upg_catalog."interval") RETURNS upg_catalog.bool LANGUAGE sql STABLE AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog."overlay"(upg_catalog.text, upg_catalog.text, upg_catalog.int4) RETURNS upg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog."overlay"(upg_catalog.text, upg_catalog.text, upg_catalog.int4, upg_catalog.int4) RETURNS upg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog."substring"(upg_catalog."bit", upg_catalog.int4) RETURNS upg_catalog."bit" LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog."bit"';
 CREATE FUNCTION upg_catalog."substring"(upg_catalog.text, upg_catalog.text, upg_catalog.text) RETURNS upg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog.age(upg_catalog."timestamp") RETURNS upg_catalog."interval" LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog."interval"';
 CREATE FUNCTION upg_catalog.age(upg_catalog.timestamptz) RETURNS upg_catalog."interval" LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog."interval"';
 CREATE FUNCTION upg_catalog.bit_length(upg_catalog."bit") RETURNS upg_catalog.int4 LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.int4';
 CREATE FUNCTION upg_catalog.bit_length(upg_catalog.bytea) RETURNS upg_catalog.int4 LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.int4';
 CREATE FUNCTION upg_catalog.bit_length(upg_catalog.text) RETURNS upg_catalog.int4 LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.int4';
 CREATE FUNCTION upg_catalog.col_description(upg_catalog.oid, upg_catalog.int4) RETURNS upg_catalog.text LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog.date_part(upg_catalog.text, upg_catalog.abstime) RETURNS upg_catalog.float8 LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog.float8';
 CREATE FUNCTION upg_catalog.date_part(upg_catalog.text, upg_catalog.date) RETURNS upg_catalog.float8 LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.float8';
 CREATE FUNCTION upg_catalog.date_part(upg_catalog.text, upg_catalog.reltime) RETURNS upg_catalog.float8 LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog.float8';
 CREATE FUNCTION upg_catalog.int8pl_inet(upg_catalog.int8, upg_catalog.inet) RETURNS upg_catalog.inet LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.inet';
 CREATE FUNCTION upg_catalog.integer_pl_date(upg_catalog.int4, upg_catalog.date) RETURNS upg_catalog.date LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.date';
 CREATE FUNCTION upg_catalog.interval_pl_date(upg_catalog."interval", upg_catalog.date) RETURNS upg_catalog."timestamp" LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog."timestamp"';
 CREATE FUNCTION upg_catalog.interval_pl_time(upg_catalog."interval", upg_catalog."time") RETURNS upg_catalog."time" LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog."time"';
 CREATE FUNCTION upg_catalog.interval_pl_timestamp(upg_catalog."interval", upg_catalog."timestamp") RETURNS upg_catalog."timestamp" LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog."timestamp"';
 CREATE FUNCTION upg_catalog.interval_pl_timestamptz(upg_catalog."interval", upg_catalog.timestamptz) RETURNS upg_catalog.timestamptz LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog.timestamptz';
 CREATE FUNCTION upg_catalog.interval_pl_timetz(upg_catalog."interval", upg_catalog.timetz) RETURNS upg_catalog.timetz LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.timetz';
 CREATE FUNCTION upg_catalog.lpad(upg_catalog.text, upg_catalog.int4) RETURNS upg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog.obj_description(upg_catalog.oid) RETURNS upg_catalog.text LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog.obj_description(upg_catalog.oid, upg_catalog.name) RETURNS upg_catalog.text LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog.path_contain_pt(upg_catalog.path, upg_catalog.point) RETURNS upg_catalog.bool LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.bool';
 CREATE FUNCTION upg_catalog.round(upg_catalog."numeric") RETURNS upg_catalog."numeric" LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog."numeric"';
 CREATE FUNCTION upg_catalog.rpad(upg_catalog.text, upg_catalog.int4) RETURNS upg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog.shobj_description(upg_catalog.oid, upg_catalog.name) RETURNS upg_catalog.text LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog.text';
 CREATE FUNCTION upg_catalog.timedate_pl(upg_catalog."time", upg_catalog.date) RETURNS upg_catalog."timestamp" LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog."timestamp"';
 CREATE FUNCTION upg_catalog.timestamptz(upg_catalog.date, upg_catalog."time") RETURNS upg_catalog.timestamptz LANGUAGE sql STABLE STRICT AS 'select null::upg_catalog.timestamptz';
 CREATE FUNCTION upg_catalog.timetzdate_pl(upg_catalog.timetz, upg_catalog.date) RETURNS upg_catalog.timestamptz LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.timestamptz';
 CREATE FUNCTION upg_catalog.to_timestamp(upg_catalog.float8) RETURNS upg_catalog.timestamptz LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog.timestamptz';
 CREATE FUNCTION upg_catalog.trunc(upg_catalog."numeric") RETURNS upg_catalog."numeric" LANGUAGE sql IMMUTABLE STRICT AS 'select null::upg_catalog."numeric"';
