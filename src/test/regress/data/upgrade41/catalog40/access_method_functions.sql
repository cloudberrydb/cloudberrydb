-- Access Method Functions defined in the 4.0 Catalog:
--
-- The Rules for Access method functions depend on the access method:
--   * Btree functions:
--       * Must have two arguments
--       * Must return integer
--       * Arguments over upg_catalog types, return must be pg_catalog type
--   * Hash functions:
--       * Must have one argument
--       * Must return integer
--       * Arguments over upg_catalog types, return must be pg_catalog type
--   * GIN/GIST functions:
--       * Have a mix of different functions
--       * "internal" must always refer to the pg_catalog type
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
                           || case when typname = 'internal' then 'pg_catalog.' else 'upg_catalog.' end
                           || quote_ident(typname)
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
  and p.oid in (select amproc from pg_amproc)
order by pronargs, 1;
*/
 CREATE FUNCTION upg_catalog.bitcmp(upg_catalog."bit", upg_catalog."bit") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bitcmp';
 CREATE FUNCTION upg_catalog.bpcharcmp(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bpcharcmp';
 CREATE FUNCTION upg_catalog.btabstimecmp(upg_catalog.abstime, upg_catalog.abstime) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btabstimecmp';
 CREATE FUNCTION upg_catalog.btarraycmp(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btarraycmp';
 CREATE FUNCTION upg_catalog.btboolcmp(upg_catalog.bool, upg_catalog.bool) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btboolcmp';
 CREATE FUNCTION upg_catalog.btbpchar_pattern_cmp(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttext_pattern_cmp';
 CREATE FUNCTION upg_catalog.btcharcmp(upg_catalog."char", upg_catalog."char") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btcharcmp';
 CREATE FUNCTION upg_catalog.btfloat48cmp(upg_catalog.float4, upg_catalog.float8) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btfloat48cmp';
 CREATE FUNCTION upg_catalog.btfloat4cmp(upg_catalog.float4, upg_catalog.float4) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btfloat4cmp';
 CREATE FUNCTION upg_catalog.btfloat84cmp(upg_catalog.float8, upg_catalog.float4) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btfloat84cmp';
 CREATE FUNCTION upg_catalog.btfloat8cmp(upg_catalog.float8, upg_catalog.float8) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btfloat8cmp';
 CREATE FUNCTION upg_catalog.btgpxlogloccmp(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btgpxlogloccmp';
 CREATE FUNCTION upg_catalog.btint24cmp(upg_catalog.int2, upg_catalog.int4) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint24cmp';
 CREATE FUNCTION upg_catalog.btint28cmp(upg_catalog.int2, upg_catalog.int8) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint28cmp';
 CREATE FUNCTION upg_catalog.btint2cmp(upg_catalog.int2, upg_catalog.int2) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint2cmp';
 CREATE FUNCTION upg_catalog.btint42cmp(upg_catalog.int4, upg_catalog.int2) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint42cmp';
 CREATE FUNCTION upg_catalog.btint48cmp(upg_catalog.int4, upg_catalog.int8) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint48cmp';
 CREATE FUNCTION upg_catalog.btint4cmp(upg_catalog.int4, upg_catalog.int4) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint4cmp';
 CREATE FUNCTION upg_catalog.btint82cmp(upg_catalog.int8, upg_catalog.int2) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint82cmp';
 CREATE FUNCTION upg_catalog.btint84cmp(upg_catalog.int8, upg_catalog.int4) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint84cmp';
 CREATE FUNCTION upg_catalog.btint8cmp(upg_catalog.int8, upg_catalog.int8) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint8cmp';
 CREATE FUNCTION upg_catalog.btname_pattern_cmp(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btname_pattern_cmp';
 CREATE FUNCTION upg_catalog.btnamecmp(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btnamecmp';
 CREATE FUNCTION upg_catalog.btoidcmp(upg_catalog.oid, upg_catalog.oid) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btoidcmp';
 CREATE FUNCTION upg_catalog.btoidvectorcmp(upg_catalog.oidvector, upg_catalog.oidvector) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btoidvectorcmp';
 CREATE FUNCTION upg_catalog.btreltimecmp(upg_catalog.reltime, upg_catalog.reltime) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'btreltimecmp';
 CREATE FUNCTION upg_catalog.bttext_pattern_cmp(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttext_pattern_cmp';
 CREATE FUNCTION upg_catalog.bttextcmp(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttextcmp';
 CREATE FUNCTION upg_catalog.bttidcmp(upg_catalog.tid, upg_catalog.tid) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttidcmp';
 CREATE FUNCTION upg_catalog.bttintervalcmp(upg_catalog.tinterval, upg_catalog.tinterval) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttintervalcmp';
 CREATE FUNCTION upg_catalog.byteacmp(upg_catalog.bytea, upg_catalog.bytea) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'byteacmp';
 CREATE FUNCTION upg_catalog.cash_cmp(upg_catalog.money, upg_catalog.money) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'cash_cmp';
 CREATE FUNCTION upg_catalog.date_cmp(upg_catalog.date, upg_catalog.date) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'date_cmp';
 CREATE FUNCTION upg_catalog.date_cmp_timestamp(upg_catalog.date, upg_catalog."timestamp") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'date_cmp_timestamp';
 CREATE FUNCTION upg_catalog.date_cmp_timestamptz(upg_catalog.date, upg_catalog.timestamptz) RETURNS pg_catalog.int4 LANGUAGE internal STABLE STRICT AS 'date_cmp_timestamptz';
 CREATE FUNCTION upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'ginarrayconsistent';
 CREATE FUNCTION upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal) RETURNS pg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'ginarrayextract';
 CREATE FUNCTION upg_catalog.gist_box_compress(pg_catalog.internal) RETURNS pg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_compress';
 CREATE FUNCTION upg_catalog.gist_box_consistent(pg_catalog.internal, upg_catalog.box, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_consistent';
 CREATE FUNCTION upg_catalog.gist_box_decompress(pg_catalog.internal) RETURNS pg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_decompress';
 CREATE FUNCTION upg_catalog.gist_box_penalty(pg_catalog.internal, pg_catalog.internal, pg_catalog.internal) RETURNS pg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_penalty';
 CREATE FUNCTION upg_catalog.gist_box_picksplit(pg_catalog.internal, pg_catalog.internal) RETURNS pg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_picksplit';
 CREATE FUNCTION upg_catalog.gist_box_same(upg_catalog.box, upg_catalog.box, pg_catalog.internal) RETURNS pg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_same';
 CREATE FUNCTION upg_catalog.gist_box_union(pg_catalog.internal, pg_catalog.internal) RETURNS pg_catalog.box LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_union';
 CREATE FUNCTION upg_catalog.gist_circle_compress(pg_catalog.internal) RETURNS pg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_circle_compress';
 CREATE FUNCTION upg_catalog.gist_circle_consistent(pg_catalog.internal, upg_catalog.circle, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'gist_circle_consistent';
 CREATE FUNCTION upg_catalog.gist_poly_compress(pg_catalog.internal) RETURNS pg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_poly_compress';
 CREATE FUNCTION upg_catalog.gist_poly_consistent(pg_catalog.internal, upg_catalog.polygon, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'gist_poly_consistent';
 CREATE FUNCTION upg_catalog.hash_aclitem(upg_catalog.aclitem) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hash_aclitem';
 CREATE FUNCTION upg_catalog.hash_numeric(upg_catalog."numeric") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hash_numeric';
 CREATE FUNCTION upg_catalog.hashbpchar(upg_catalog.bpchar) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashbpchar';
 CREATE FUNCTION upg_catalog.hashchar(upg_catalog."char") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashchar';
 CREATE FUNCTION upg_catalog.hashfloat4(upg_catalog.float4) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashfloat4';
 CREATE FUNCTION upg_catalog.hashfloat8(upg_catalog.float8) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashfloat8';
 CREATE FUNCTION upg_catalog.hashinet(upg_catalog.inet) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashinet';
 CREATE FUNCTION upg_catalog.hashint2(upg_catalog.int2) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashint2';
 CREATE FUNCTION upg_catalog.hashint2vector(upg_catalog.int2vector) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashint2vector';
 CREATE FUNCTION upg_catalog.hashint4(upg_catalog.int4) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashint4';
 CREATE FUNCTION upg_catalog.hashint8(upg_catalog.int8) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashint8';
 CREATE FUNCTION upg_catalog.hashmacaddr(upg_catalog.macaddr) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashmacaddr';
 CREATE FUNCTION upg_catalog.hashname(upg_catalog.name) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashname';
 CREATE FUNCTION upg_catalog.hashoid(upg_catalog.oid) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashoid';
 CREATE FUNCTION upg_catalog.hashoidvector(upg_catalog.oidvector) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashoidvector';
 CREATE FUNCTION upg_catalog.hashtext(upg_catalog.text) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashtext';
 CREATE FUNCTION upg_catalog.hashvarlena(pg_catalog.internal) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashvarlena';
 CREATE FUNCTION upg_catalog.interval_cmp(upg_catalog."interval", upg_catalog."interval") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'interval_cmp';
 CREATE FUNCTION upg_catalog.interval_hash(upg_catalog."interval") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'interval_hash';
 CREATE FUNCTION upg_catalog.macaddr_cmp(upg_catalog.macaddr, upg_catalog.macaddr) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_cmp';
 CREATE FUNCTION upg_catalog.network_cmp(upg_catalog.inet, upg_catalog.inet) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'network_cmp';
 CREATE FUNCTION upg_catalog.numeric_cmp(upg_catalog."numeric", upg_catalog."numeric") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_cmp';
 CREATE FUNCTION upg_catalog.time_cmp(upg_catalog."time", upg_catalog."time") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'time_cmp';
 CREATE FUNCTION upg_catalog.timestamp_cmp(upg_catalog."timestamp", upg_catalog."timestamp") RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_cmp';
 CREATE FUNCTION upg_catalog.timestamp_cmp_date(upg_catalog."timestamp", upg_catalog.date) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_cmp_date';
 CREATE FUNCTION upg_catalog.timestamp_cmp_timestamptz(upg_catalog."timestamp", upg_catalog.timestamptz) RETURNS pg_catalog.int4 LANGUAGE internal STABLE STRICT AS 'timestamp_cmp_timestamptz';
 CREATE FUNCTION upg_catalog.timestamptz_cmp(upg_catalog.timestamptz, upg_catalog.timestamptz) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_cmp';
 CREATE FUNCTION upg_catalog.timestamptz_cmp_date(upg_catalog.timestamptz, upg_catalog.date) RETURNS pg_catalog.int4 LANGUAGE internal STABLE STRICT AS 'timestamptz_cmp_date';
 CREATE FUNCTION upg_catalog.timestamptz_cmp_timestamp(upg_catalog.timestamptz, upg_catalog."timestamp") RETURNS pg_catalog.int4 LANGUAGE internal STABLE STRICT AS 'timestamptz_cmp_timestamp';
 CREATE FUNCTION upg_catalog.timetz_cmp(upg_catalog.timetz, upg_catalog.timetz) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'timetz_cmp';
 CREATE FUNCTION upg_catalog.timetz_hash(upg_catalog.timetz) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'timetz_hash';
 CREATE FUNCTION upg_catalog.varbitcmp(upg_catalog.varbit, upg_catalog.varbit) RETURNS pg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'bitcmp';
