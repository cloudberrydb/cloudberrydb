-- Operator Selectivity Functions defined in the 4.0 Catalog:
--
-- These are the Restriction Selectivity and Join Selectivity functions
-- for operators:
--   * Must have arguments (internal, oid, internal, int2)
--   * Must return float8
--   * Must be over real catalog types, not upg_catalog types
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
                           || 'pg_catalog.' || quote_ident(typname)
                     from pg_type t, generate_series(1, pronargs) i
                     where t.oid = proargtypes[i-1]
                     order by i), ', '), '')
    || ') RETURNS '
    || case when proretset then 'SETOF ' else '' end 
    || 'pg_catalog.' || quote_ident(r.typname) || ' '
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
            then '$$' || prosrc || '$$'
            else 'BROKEN (unsupported language)' end
    || ';'
FROM pg_proc p
JOIN pg_type r on (p.prorettype = r.oid)
JOIN pg_language l on (p.prolang = l.oid)
JOIN pg_namespace n on (p.pronamespace = n.oid)
WHERE n.nspname = 'pg_catalog'
  and p.oid in (select unnest(array[oprrest, oprjoin]) from pg_operator)
order by 1;
*/
 CREATE FUNCTION upg_catalog.areajoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'areajoinsel';
 CREATE FUNCTION upg_catalog.areasel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'areasel';
 CREATE FUNCTION upg_catalog.contjoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'contjoinsel';
 CREATE FUNCTION upg_catalog.contsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'contsel';
 CREATE FUNCTION upg_catalog.eqjoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'eqjoinsel';
 CREATE FUNCTION upg_catalog.eqsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'eqsel';
 CREATE FUNCTION upg_catalog.iclikejoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'iclikejoinsel';
 CREATE FUNCTION upg_catalog.iclikesel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'iclikesel';
 CREATE FUNCTION upg_catalog.icnlikejoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'icnlikejoinsel';
 CREATE FUNCTION upg_catalog.icnlikesel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'icnlikesel';
 CREATE FUNCTION upg_catalog.icregexeqjoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'icregexeqjoinsel';
 CREATE FUNCTION upg_catalog.icregexeqsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'icregexeqsel';
 CREATE FUNCTION upg_catalog.icregexnejoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'icregexnejoinsel';
 CREATE FUNCTION upg_catalog.icregexnesel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'icregexnesel';
 CREATE FUNCTION upg_catalog.likejoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'likejoinsel';
 CREATE FUNCTION upg_catalog.likesel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'likesel';
 CREATE FUNCTION upg_catalog.neqjoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'neqjoinsel';
 CREATE FUNCTION upg_catalog.neqsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'neqsel';
 CREATE FUNCTION upg_catalog.nlikejoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'nlikejoinsel';
 CREATE FUNCTION upg_catalog.nlikesel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'nlikesel';
 CREATE FUNCTION upg_catalog.positionjoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'positionjoinsel';
 CREATE FUNCTION upg_catalog.positionsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'positionsel';
 CREATE FUNCTION upg_catalog.regexeqjoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'regexeqjoinsel';
 CREATE FUNCTION upg_catalog.regexeqsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'regexeqsel';
 CREATE FUNCTION upg_catalog.regexnejoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'regexnejoinsel';
 CREATE FUNCTION upg_catalog.regexnesel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'regexnesel';
 CREATE FUNCTION upg_catalog.scalargtjoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'scalargtjoinsel';
 CREATE FUNCTION upg_catalog.scalargtsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'scalargtsel';
 CREATE FUNCTION upg_catalog.scalarltjoinsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int2) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'scalarltjoinsel';
 CREATE FUNCTION upg_catalog.scalarltsel(pg_catalog.internal, pg_catalog.oid, pg_catalog.internal, pg_catalog.int4) RETURNS pg_catalog.float8 LANGUAGE internal STABLE STRICT AS 'scalarltsel';
