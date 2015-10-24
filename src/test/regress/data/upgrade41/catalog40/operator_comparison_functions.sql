-- Operator Comparison Functions defined in the 4.0 Catalog:
--
-- These are the Comparison Functions for operators:
--   * Must have two arguments
--   * Must return boolean
--   * Arguments over upg_catalog types, return must be pg_catalog type
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
                           || 'upg_catalog.' || quote_ident(typname)
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
  and p.oid in (select oprcode from pg_operator 
                where oprname in ('<', '<=', '=', '>', '>=') 
                   or oid in (select amopopr from pg_amop))
order by 1;
*/
 CREATE FUNCTION upg_catalog.abstimeeq(upg_catalog.abstime, upg_catalog.abstime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimeeq';
 CREATE FUNCTION upg_catalog.abstimege(upg_catalog.abstime, upg_catalog.abstime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimege';
 CREATE FUNCTION upg_catalog.abstimegt(upg_catalog.abstime, upg_catalog.abstime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimegt';
 CREATE FUNCTION upg_catalog.abstimele(upg_catalog.abstime, upg_catalog.abstime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimele';
 CREATE FUNCTION upg_catalog.abstimelt(upg_catalog.abstime, upg_catalog.abstime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimelt';
 CREATE FUNCTION upg_catalog.aclitemeq(upg_catalog.aclitem, upg_catalog.aclitem) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'aclitem_eq';
 CREATE FUNCTION upg_catalog.array_eq(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'array_eq';
 CREATE FUNCTION upg_catalog.array_ge(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'array_ge';
 CREATE FUNCTION upg_catalog.array_gt(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'array_gt';
 CREATE FUNCTION upg_catalog.array_le(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'array_le';
 CREATE FUNCTION upg_catalog.array_lt(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'array_lt';
 CREATE FUNCTION upg_catalog.arraycontained(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'arraycontained';
 CREATE FUNCTION upg_catalog.arraycontains(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'arraycontains';
 CREATE FUNCTION upg_catalog.arrayoverlap(upg_catalog.anyarray, upg_catalog.anyarray) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'arrayoverlap';
 CREATE FUNCTION upg_catalog.biteq(upg_catalog."bit", upg_catalog."bit") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'biteq';
 CREATE FUNCTION upg_catalog.bitge(upg_catalog."bit", upg_catalog."bit") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bitge';
 CREATE FUNCTION upg_catalog.bitgt(upg_catalog."bit", upg_catalog."bit") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bitgt';
 CREATE FUNCTION upg_catalog.bitle(upg_catalog."bit", upg_catalog."bit") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bitle';
 CREATE FUNCTION upg_catalog.bitlt(upg_catalog."bit", upg_catalog."bit") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bitlt';
 CREATE FUNCTION upg_catalog.booleq(upg_catalog.bool, upg_catalog.bool) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'booleq';
 CREATE FUNCTION upg_catalog.boolge(upg_catalog.bool, upg_catalog.bool) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'boolge';
 CREATE FUNCTION upg_catalog.boolgt(upg_catalog.bool, upg_catalog.bool) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'boolgt';
 CREATE FUNCTION upg_catalog.boolle(upg_catalog.bool, upg_catalog.bool) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'boolle';
 CREATE FUNCTION upg_catalog.boollt(upg_catalog.bool, upg_catalog.bool) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'boollt';
 CREATE FUNCTION upg_catalog.box_above(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_above';
 CREATE FUNCTION upg_catalog.box_below(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_below';
 CREATE FUNCTION upg_catalog.box_contain(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_contain';
 CREATE FUNCTION upg_catalog.box_contained(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_contained';
 CREATE FUNCTION upg_catalog.box_eq(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_eq';
 CREATE FUNCTION upg_catalog.box_ge(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_ge';
 CREATE FUNCTION upg_catalog.box_gt(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_gt';
 CREATE FUNCTION upg_catalog.box_le(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_le';
 CREATE FUNCTION upg_catalog.box_left(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_left';
 CREATE FUNCTION upg_catalog.box_lt(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_lt';
 CREATE FUNCTION upg_catalog.box_overabove(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overabove';
 CREATE FUNCTION upg_catalog.box_overbelow(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overbelow';
 CREATE FUNCTION upg_catalog.box_overlap(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overlap';
 CREATE FUNCTION upg_catalog.box_overleft(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overleft';
 CREATE FUNCTION upg_catalog.box_overright(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overright';
 CREATE FUNCTION upg_catalog.box_right(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_right';
 CREATE FUNCTION upg_catalog.box_same(upg_catalog.box, upg_catalog.box) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'box_same';
 CREATE FUNCTION upg_catalog.bpchar_pattern_eq(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'texteq';
 CREATE FUNCTION upg_catalog.bpchar_pattern_ge(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_ge';
 CREATE FUNCTION upg_catalog.bpchar_pattern_gt(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_gt';
 CREATE FUNCTION upg_catalog.bpchar_pattern_le(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_le';
 CREATE FUNCTION upg_catalog.bpchar_pattern_lt(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_lt';
 CREATE FUNCTION upg_catalog.bpchareq(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bpchareq';
 CREATE FUNCTION upg_catalog.bpcharge(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bpcharge';
 CREATE FUNCTION upg_catalog.bpchargt(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bpchargt';
 CREATE FUNCTION upg_catalog.bpcharle(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bpcharle';
 CREATE FUNCTION upg_catalog.bpcharlt(upg_catalog.bpchar, upg_catalog.bpchar) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bpcharlt';
 CREATE FUNCTION upg_catalog.byteaeq(upg_catalog.bytea, upg_catalog.bytea) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'byteaeq';
 CREATE FUNCTION upg_catalog.byteage(upg_catalog.bytea, upg_catalog.bytea) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'byteage';
 CREATE FUNCTION upg_catalog.byteagt(upg_catalog.bytea, upg_catalog.bytea) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'byteagt';
 CREATE FUNCTION upg_catalog.byteale(upg_catalog.bytea, upg_catalog.bytea) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'byteale';
 CREATE FUNCTION upg_catalog.bytealt(upg_catalog.bytea, upg_catalog.bytea) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bytealt';
 CREATE FUNCTION upg_catalog.cash_eq(upg_catalog.money, upg_catalog.money) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_eq';
 CREATE FUNCTION upg_catalog.cash_ge(upg_catalog.money, upg_catalog.money) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_ge';
 CREATE FUNCTION upg_catalog.cash_gt(upg_catalog.money, upg_catalog.money) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_gt';
 CREATE FUNCTION upg_catalog.cash_le(upg_catalog.money, upg_catalog.money) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_le';
 CREATE FUNCTION upg_catalog.cash_lt(upg_catalog.money, upg_catalog.money) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_lt';
 CREATE FUNCTION upg_catalog.chareq(upg_catalog."char", upg_catalog."char") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'chareq';
 CREATE FUNCTION upg_catalog.charge(upg_catalog."char", upg_catalog."char") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'charge';
 CREATE FUNCTION upg_catalog.chargt(upg_catalog."char", upg_catalog."char") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'chargt';
 CREATE FUNCTION upg_catalog.charle(upg_catalog."char", upg_catalog."char") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'charle';
 CREATE FUNCTION upg_catalog.charlt(upg_catalog."char", upg_catalog."char") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'charlt';
 CREATE FUNCTION upg_catalog.cideq(upg_catalog.cid, upg_catalog.cid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'cideq';
 CREATE FUNCTION upg_catalog.circle_above(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_above';
 CREATE FUNCTION upg_catalog.circle_below(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_below';
 CREATE FUNCTION upg_catalog.circle_contain(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_contain';
 CREATE FUNCTION upg_catalog.circle_contained(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_contained';
 CREATE FUNCTION upg_catalog.circle_eq(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_eq';
 CREATE FUNCTION upg_catalog.circle_ge(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_ge';
 CREATE FUNCTION upg_catalog.circle_gt(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_gt';
 CREATE FUNCTION upg_catalog.circle_le(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_le';
 CREATE FUNCTION upg_catalog.circle_left(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_left';
 CREATE FUNCTION upg_catalog.circle_lt(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_lt';
 CREATE FUNCTION upg_catalog.circle_overabove(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overabove';
 CREATE FUNCTION upg_catalog.circle_overbelow(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overbelow';
 CREATE FUNCTION upg_catalog.circle_overlap(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overlap';
 CREATE FUNCTION upg_catalog.circle_overleft(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overleft';
 CREATE FUNCTION upg_catalog.circle_overright(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overright';
 CREATE FUNCTION upg_catalog.circle_right(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_right';
 CREATE FUNCTION upg_catalog.circle_same(upg_catalog.circle, upg_catalog.circle) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_same';
 CREATE FUNCTION upg_catalog.date_eq(upg_catalog.date, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_eq';
 CREATE FUNCTION upg_catalog.date_eq_timestamp(upg_catalog.date, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_eq_timestamp';
 CREATE FUNCTION upg_catalog.date_eq_timestamptz(upg_catalog.date, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'date_eq_timestamptz';
 CREATE FUNCTION upg_catalog.date_ge(upg_catalog.date, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_ge';
 CREATE FUNCTION upg_catalog.date_ge_timestamp(upg_catalog.date, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_ge_timestamp';
 CREATE FUNCTION upg_catalog.date_ge_timestamptz(upg_catalog.date, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'date_ge_timestamptz';
 CREATE FUNCTION upg_catalog.date_gt(upg_catalog.date, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_gt';
 CREATE FUNCTION upg_catalog.date_gt_timestamp(upg_catalog.date, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_gt_timestamp';
 CREATE FUNCTION upg_catalog.date_gt_timestamptz(upg_catalog.date, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'date_gt_timestamptz';
 CREATE FUNCTION upg_catalog.date_le(upg_catalog.date, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_le';
 CREATE FUNCTION upg_catalog.date_le_timestamp(upg_catalog.date, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_le_timestamp';
 CREATE FUNCTION upg_catalog.date_le_timestamptz(upg_catalog.date, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'date_le_timestamptz';
 CREATE FUNCTION upg_catalog.date_lt(upg_catalog.date, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_lt';
 CREATE FUNCTION upg_catalog.date_lt_timestamp(upg_catalog.date, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'date_lt_timestamp';
 CREATE FUNCTION upg_catalog.date_lt_timestamptz(upg_catalog.date, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'date_lt_timestamptz';
 CREATE FUNCTION upg_catalog.float48eq(upg_catalog.float4, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float48eq';
 CREATE FUNCTION upg_catalog.float48ge(upg_catalog.float4, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float48ge';
 CREATE FUNCTION upg_catalog.float48gt(upg_catalog.float4, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float48gt';
 CREATE FUNCTION upg_catalog.float48le(upg_catalog.float4, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float48le';
 CREATE FUNCTION upg_catalog.float48lt(upg_catalog.float4, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float48lt';
 CREATE FUNCTION upg_catalog.float4eq(upg_catalog.float4, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float4eq';
 CREATE FUNCTION upg_catalog.float4ge(upg_catalog.float4, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float4ge';
 CREATE FUNCTION upg_catalog.float4gt(upg_catalog.float4, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float4gt';
 CREATE FUNCTION upg_catalog.float4le(upg_catalog.float4, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float4le';
 CREATE FUNCTION upg_catalog.float4lt(upg_catalog.float4, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float4lt';
 CREATE FUNCTION upg_catalog.float84eq(upg_catalog.float8, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float84eq';
 CREATE FUNCTION upg_catalog.float84ge(upg_catalog.float8, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float84ge';
 CREATE FUNCTION upg_catalog.float84gt(upg_catalog.float8, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float84gt';
 CREATE FUNCTION upg_catalog.float84le(upg_catalog.float8, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float84le';
 CREATE FUNCTION upg_catalog.float84lt(upg_catalog.float8, upg_catalog.float4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float84lt';
 CREATE FUNCTION upg_catalog.float8eq(upg_catalog.float8, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float8eq';
 CREATE FUNCTION upg_catalog.float8ge(upg_catalog.float8, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float8ge';
 CREATE FUNCTION upg_catalog.float8gt(upg_catalog.float8, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float8gt';
 CREATE FUNCTION upg_catalog.float8le(upg_catalog.float8, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float8le';
 CREATE FUNCTION upg_catalog.float8lt(upg_catalog.float8, upg_catalog.float8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'float8lt';
 CREATE FUNCTION upg_catalog.gpxlogloceq(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxlogloceq';
 CREATE FUNCTION upg_catalog.gpxloglocge(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocge';
 CREATE FUNCTION upg_catalog.gpxloglocgt(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocgt';
 CREATE FUNCTION upg_catalog.gpxloglocle(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocle';
 CREATE FUNCTION upg_catalog.gpxlogloclt(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxlogloclt';
 CREATE FUNCTION upg_catalog.int24eq(upg_catalog.int2, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int24eq';
 CREATE FUNCTION upg_catalog.int24ge(upg_catalog.int2, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int24ge';
 CREATE FUNCTION upg_catalog.int24gt(upg_catalog.int2, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int24gt';
 CREATE FUNCTION upg_catalog.int24le(upg_catalog.int2, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int24le';
 CREATE FUNCTION upg_catalog.int24lt(upg_catalog.int2, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int24lt';
 CREATE FUNCTION upg_catalog.int28eq(upg_catalog.int2, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int28eq';
 CREATE FUNCTION upg_catalog.int28ge(upg_catalog.int2, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int28ge';
 CREATE FUNCTION upg_catalog.int28gt(upg_catalog.int2, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int28gt';
 CREATE FUNCTION upg_catalog.int28le(upg_catalog.int2, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int28le';
 CREATE FUNCTION upg_catalog.int28lt(upg_catalog.int2, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int28lt';
 CREATE FUNCTION upg_catalog.int2eq(upg_catalog.int2, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int2eq';
 CREATE FUNCTION upg_catalog.int2ge(upg_catalog.int2, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int2ge';
 CREATE FUNCTION upg_catalog.int2gt(upg_catalog.int2, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int2gt';
 CREATE FUNCTION upg_catalog.int2le(upg_catalog.int2, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int2le';
 CREATE FUNCTION upg_catalog.int2lt(upg_catalog.int2, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int2lt';
 CREATE FUNCTION upg_catalog.int2vectoreq(upg_catalog.int2vector, upg_catalog.int2vector) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int2vectoreq';
 CREATE FUNCTION upg_catalog.int42eq(upg_catalog.int4, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int42eq';
 CREATE FUNCTION upg_catalog.int42ge(upg_catalog.int4, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int42ge';
 CREATE FUNCTION upg_catalog.int42gt(upg_catalog.int4, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int42gt';
 CREATE FUNCTION upg_catalog.int42le(upg_catalog.int4, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int42le';
 CREATE FUNCTION upg_catalog.int42lt(upg_catalog.int4, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int42lt';
 CREATE FUNCTION upg_catalog.int48eq(upg_catalog.int4, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int48eq';
 CREATE FUNCTION upg_catalog.int48ge(upg_catalog.int4, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int48ge';
 CREATE FUNCTION upg_catalog.int48gt(upg_catalog.int4, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int48gt';
 CREATE FUNCTION upg_catalog.int48le(upg_catalog.int4, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int48le';
 CREATE FUNCTION upg_catalog.int48lt(upg_catalog.int4, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int48lt';
 CREATE FUNCTION upg_catalog.int4eq(upg_catalog.int4, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int4eq';
 CREATE FUNCTION upg_catalog.int4ge(upg_catalog.int4, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int4ge';
 CREATE FUNCTION upg_catalog.int4gt(upg_catalog.int4, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int4gt';
 CREATE FUNCTION upg_catalog.int4le(upg_catalog.int4, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int4le';
 CREATE FUNCTION upg_catalog.int4lt(upg_catalog.int4, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int4lt';
 CREATE FUNCTION upg_catalog.int82eq(upg_catalog.int8, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int82eq';
 CREATE FUNCTION upg_catalog.int82ge(upg_catalog.int8, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int82ge';
 CREATE FUNCTION upg_catalog.int82gt(upg_catalog.int8, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int82gt';
 CREATE FUNCTION upg_catalog.int82le(upg_catalog.int8, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int82le';
 CREATE FUNCTION upg_catalog.int82lt(upg_catalog.int8, upg_catalog.int2) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int82lt';
 CREATE FUNCTION upg_catalog.int84eq(upg_catalog.int8, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int84eq';
 CREATE FUNCTION upg_catalog.int84ge(upg_catalog.int8, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int84ge';
 CREATE FUNCTION upg_catalog.int84gt(upg_catalog.int8, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int84gt';
 CREATE FUNCTION upg_catalog.int84le(upg_catalog.int8, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int84le';
 CREATE FUNCTION upg_catalog.int84lt(upg_catalog.int8, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int84lt';
 CREATE FUNCTION upg_catalog.int8eq(upg_catalog.int8, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int8eq';
 CREATE FUNCTION upg_catalog.int8ge(upg_catalog.int8, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int8ge';
 CREATE FUNCTION upg_catalog.int8gt(upg_catalog.int8, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int8gt';
 CREATE FUNCTION upg_catalog.int8le(upg_catalog.int8, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int8le';
 CREATE FUNCTION upg_catalog.int8lt(upg_catalog.int8, upg_catalog.int8) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'int8lt';
 CREATE FUNCTION upg_catalog.interval_eq(upg_catalog."interval", upg_catalog."interval") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_eq';
 CREATE FUNCTION upg_catalog.interval_ge(upg_catalog."interval", upg_catalog."interval") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_ge';
 CREATE FUNCTION upg_catalog.interval_gt(upg_catalog."interval", upg_catalog."interval") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_gt';
 CREATE FUNCTION upg_catalog.interval_le(upg_catalog."interval", upg_catalog."interval") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_le';
 CREATE FUNCTION upg_catalog.interval_lt(upg_catalog."interval", upg_catalog."interval") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_lt';
 CREATE FUNCTION upg_catalog.line_eq(upg_catalog.line, upg_catalog.line) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'line_eq';
 CREATE FUNCTION upg_catalog.lseg_eq(upg_catalog.lseg, upg_catalog.lseg) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_eq';
 CREATE FUNCTION upg_catalog.lseg_ge(upg_catalog.lseg, upg_catalog.lseg) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_ge';
 CREATE FUNCTION upg_catalog.lseg_gt(upg_catalog.lseg, upg_catalog.lseg) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_gt';
 CREATE FUNCTION upg_catalog.lseg_le(upg_catalog.lseg, upg_catalog.lseg) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_le';
 CREATE FUNCTION upg_catalog.lseg_lt(upg_catalog.lseg, upg_catalog.lseg) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_lt';
 CREATE FUNCTION upg_catalog.macaddr_eq(upg_catalog.macaddr, upg_catalog.macaddr) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_eq';
 CREATE FUNCTION upg_catalog.macaddr_ge(upg_catalog.macaddr, upg_catalog.macaddr) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_ge';
 CREATE FUNCTION upg_catalog.macaddr_gt(upg_catalog.macaddr, upg_catalog.macaddr) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_gt';
 CREATE FUNCTION upg_catalog.macaddr_le(upg_catalog.macaddr, upg_catalog.macaddr) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_le';
 CREATE FUNCTION upg_catalog.macaddr_lt(upg_catalog.macaddr, upg_catalog.macaddr) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_lt';
 CREATE FUNCTION upg_catalog.name_pattern_eq(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_eq';
 CREATE FUNCTION upg_catalog.name_pattern_ge(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_ge';
 CREATE FUNCTION upg_catalog.name_pattern_gt(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_gt';
 CREATE FUNCTION upg_catalog.name_pattern_le(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_le';
 CREATE FUNCTION upg_catalog.name_pattern_lt(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_lt';
 CREATE FUNCTION upg_catalog.nameeq(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'nameeq';
 CREATE FUNCTION upg_catalog.namege(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'namege';
 CREATE FUNCTION upg_catalog.namegt(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'namegt';
 CREATE FUNCTION upg_catalog.namele(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'namele';
 CREATE FUNCTION upg_catalog.namelt(upg_catalog.name, upg_catalog.name) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'namelt';
 CREATE FUNCTION upg_catalog.network_eq(upg_catalog.inet, upg_catalog.inet) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'network_eq';
 CREATE FUNCTION upg_catalog.network_ge(upg_catalog.inet, upg_catalog.inet) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'network_ge';
 CREATE FUNCTION upg_catalog.network_gt(upg_catalog.inet, upg_catalog.inet) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'network_gt';
 CREATE FUNCTION upg_catalog.network_le(upg_catalog.inet, upg_catalog.inet) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'network_le';
 CREATE FUNCTION upg_catalog.network_lt(upg_catalog.inet, upg_catalog.inet) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'network_lt';
 CREATE FUNCTION upg_catalog.numeric_eq(upg_catalog."numeric", upg_catalog."numeric") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_eq';
 CREATE FUNCTION upg_catalog.numeric_ge(upg_catalog."numeric", upg_catalog."numeric") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_ge';
 CREATE FUNCTION upg_catalog.numeric_gt(upg_catalog."numeric", upg_catalog."numeric") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_gt';
 CREATE FUNCTION upg_catalog.numeric_le(upg_catalog."numeric", upg_catalog."numeric") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_le';
 CREATE FUNCTION upg_catalog.numeric_lt(upg_catalog."numeric", upg_catalog."numeric") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_lt';
 CREATE FUNCTION upg_catalog.oideq(upg_catalog.oid, upg_catalog.oid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oideq';
 CREATE FUNCTION upg_catalog.oidge(upg_catalog.oid, upg_catalog.oid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidge';
 CREATE FUNCTION upg_catalog.oidgt(upg_catalog.oid, upg_catalog.oid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidgt';
 CREATE FUNCTION upg_catalog.oidle(upg_catalog.oid, upg_catalog.oid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidle';
 CREATE FUNCTION upg_catalog.oidlt(upg_catalog.oid, upg_catalog.oid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidlt';
 CREATE FUNCTION upg_catalog.oidvectoreq(upg_catalog.oidvector, upg_catalog.oidvector) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectoreq';
 CREATE FUNCTION upg_catalog.oidvectorge(upg_catalog.oidvector, upg_catalog.oidvector) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorge';
 CREATE FUNCTION upg_catalog.oidvectorgt(upg_catalog.oidvector, upg_catalog.oidvector) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorgt';
 CREATE FUNCTION upg_catalog.oidvectorle(upg_catalog.oidvector, upg_catalog.oidvector) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorle';
 CREATE FUNCTION upg_catalog.oidvectorlt(upg_catalog.oidvector, upg_catalog.oidvector) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorlt';
 CREATE FUNCTION upg_catalog.path_n_eq(upg_catalog.path, upg_catalog.path) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_eq';
 CREATE FUNCTION upg_catalog.path_n_ge(upg_catalog.path, upg_catalog.path) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_ge';
 CREATE FUNCTION upg_catalog.path_n_gt(upg_catalog.path, upg_catalog.path) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_gt';
 CREATE FUNCTION upg_catalog.path_n_le(upg_catalog.path, upg_catalog.path) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_le';
 CREATE FUNCTION upg_catalog.path_n_lt(upg_catalog.path, upg_catalog.path) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_lt';
 CREATE FUNCTION upg_catalog.poly_above(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_above';
 CREATE FUNCTION upg_catalog.poly_below(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_below';
 CREATE FUNCTION upg_catalog.poly_contain(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_contain';
 CREATE FUNCTION upg_catalog.poly_contained(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_contained';
 CREATE FUNCTION upg_catalog.poly_left(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_left';
 CREATE FUNCTION upg_catalog.poly_overabove(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overabove';
 CREATE FUNCTION upg_catalog.poly_overbelow(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overbelow';
 CREATE FUNCTION upg_catalog.poly_overlap(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overlap';
 CREATE FUNCTION upg_catalog.poly_overleft(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overleft';
 CREATE FUNCTION upg_catalog.poly_overright(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overright';
 CREATE FUNCTION upg_catalog.poly_right(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_right';
 CREATE FUNCTION upg_catalog.poly_same(upg_catalog.polygon, upg_catalog.polygon) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_same';
 CREATE FUNCTION upg_catalog.reltimeeq(upg_catalog.reltime, upg_catalog.reltime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimeeq';
 CREATE FUNCTION upg_catalog.reltimege(upg_catalog.reltime, upg_catalog.reltime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimege';
 CREATE FUNCTION upg_catalog.reltimegt(upg_catalog.reltime, upg_catalog.reltime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimegt';
 CREATE FUNCTION upg_catalog.reltimele(upg_catalog.reltime, upg_catalog.reltime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimele';
 CREATE FUNCTION upg_catalog.reltimelt(upg_catalog.reltime, upg_catalog.reltime) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimelt';
 CREATE FUNCTION upg_catalog.text_ge(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_ge';
 CREATE FUNCTION upg_catalog.text_gt(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_gt';
 CREATE FUNCTION upg_catalog.text_le(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_le';
 CREATE FUNCTION upg_catalog.text_lt(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_lt';
 CREATE FUNCTION upg_catalog.text_pattern_ge(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_ge';
 CREATE FUNCTION upg_catalog.text_pattern_gt(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_gt';
 CREATE FUNCTION upg_catalog.text_pattern_le(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_le';
 CREATE FUNCTION upg_catalog.text_pattern_lt(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_lt';
 CREATE FUNCTION upg_catalog.texteq(upg_catalog.text, upg_catalog.text) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'texteq';
 CREATE FUNCTION upg_catalog.tideq(upg_catalog.tid, upg_catalog.tid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tideq';
 CREATE FUNCTION upg_catalog.tidge(upg_catalog.tid, upg_catalog.tid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tidge';
 CREATE FUNCTION upg_catalog.tidgt(upg_catalog.tid, upg_catalog.tid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tidgt';
 CREATE FUNCTION upg_catalog.tidle(upg_catalog.tid, upg_catalog.tid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tidle';
 CREATE FUNCTION upg_catalog.tidlt(upg_catalog.tid, upg_catalog.tid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tidlt';
 CREATE FUNCTION upg_catalog.time_eq(upg_catalog."time", upg_catalog."time") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'time_eq';
 CREATE FUNCTION upg_catalog.time_ge(upg_catalog."time", upg_catalog."time") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'time_ge';
 CREATE FUNCTION upg_catalog.time_gt(upg_catalog."time", upg_catalog."time") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'time_gt';
 CREATE FUNCTION upg_catalog.time_le(upg_catalog."time", upg_catalog."time") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'time_le';
 CREATE FUNCTION upg_catalog.time_lt(upg_catalog."time", upg_catalog."time") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'time_lt';
 CREATE FUNCTION upg_catalog.timestamp_eq(upg_catalog."timestamp", upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_eq';
 CREATE FUNCTION upg_catalog.timestamp_eq_date(upg_catalog."timestamp", upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_eq_date';
 CREATE FUNCTION upg_catalog.timestamp_eq_timestamptz(upg_catalog."timestamp", upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamp_eq_timestamptz';
 CREATE FUNCTION upg_catalog.timestamp_ge(upg_catalog."timestamp", upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ge';
 CREATE FUNCTION upg_catalog.timestamp_ge_date(upg_catalog."timestamp", upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ge_date';
 CREATE FUNCTION upg_catalog.timestamp_ge_timestamptz(upg_catalog."timestamp", upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamp_ge_timestamptz';
 CREATE FUNCTION upg_catalog.timestamp_gt(upg_catalog."timestamp", upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_gt';
 CREATE FUNCTION upg_catalog.timestamp_gt_date(upg_catalog."timestamp", upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_gt_date';
 CREATE FUNCTION upg_catalog.timestamp_gt_timestamptz(upg_catalog."timestamp", upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamp_gt_timestamptz';
 CREATE FUNCTION upg_catalog.timestamp_le(upg_catalog."timestamp", upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_le';
 CREATE FUNCTION upg_catalog.timestamp_le_date(upg_catalog."timestamp", upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_le_date';
 CREATE FUNCTION upg_catalog.timestamp_le_timestamptz(upg_catalog."timestamp", upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamp_le_timestamptz';
 CREATE FUNCTION upg_catalog.timestamp_lt(upg_catalog."timestamp", upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_lt';
 CREATE FUNCTION upg_catalog.timestamp_lt_date(upg_catalog."timestamp", upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_lt_date';
 CREATE FUNCTION upg_catalog.timestamp_lt_timestamptz(upg_catalog."timestamp", upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamp_lt_timestamptz';
 CREATE FUNCTION upg_catalog.timestamptz_eq(upg_catalog.timestamptz, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_eq';
 CREATE FUNCTION upg_catalog.timestamptz_eq_date(upg_catalog.timestamptz, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_eq_date';
 CREATE FUNCTION upg_catalog.timestamptz_eq_timestamp(upg_catalog.timestamptz, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_eq_timestamp';
 CREATE FUNCTION upg_catalog.timestamptz_ge(upg_catalog.timestamptz, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ge';
 CREATE FUNCTION upg_catalog.timestamptz_ge_date(upg_catalog.timestamptz, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_ge_date';
 CREATE FUNCTION upg_catalog.timestamptz_ge_timestamp(upg_catalog.timestamptz, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_ge_timestamp';
 CREATE FUNCTION upg_catalog.timestamptz_gt(upg_catalog.timestamptz, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_gt';
 CREATE FUNCTION upg_catalog.timestamptz_gt_date(upg_catalog.timestamptz, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_gt_date';
 CREATE FUNCTION upg_catalog.timestamptz_gt_timestamp(upg_catalog.timestamptz, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_gt_timestamp';
 CREATE FUNCTION upg_catalog.timestamptz_le(upg_catalog.timestamptz, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_le';
 CREATE FUNCTION upg_catalog.timestamptz_le_date(upg_catalog.timestamptz, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_le_date';
 CREATE FUNCTION upg_catalog.timestamptz_le_timestamp(upg_catalog.timestamptz, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_le_timestamp';
 CREATE FUNCTION upg_catalog.timestamptz_lt(upg_catalog.timestamptz, upg_catalog.timestamptz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_lt';
 CREATE FUNCTION upg_catalog.timestamptz_lt_date(upg_catalog.timestamptz, upg_catalog.date) RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_lt_date';
 CREATE FUNCTION upg_catalog.timestamptz_lt_timestamp(upg_catalog.timestamptz, upg_catalog."timestamp") RETURNS pg_catalog.bool LANGUAGE internal STABLE STRICT AS 'timestamptz_lt_timestamp';
 CREATE FUNCTION upg_catalog.timetz_eq(upg_catalog.timetz, upg_catalog.timetz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_eq';
 CREATE FUNCTION upg_catalog.timetz_ge(upg_catalog.timetz, upg_catalog.timetz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_ge';
 CREATE FUNCTION upg_catalog.timetz_gt(upg_catalog.timetz, upg_catalog.timetz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_gt';
 CREATE FUNCTION upg_catalog.timetz_le(upg_catalog.timetz, upg_catalog.timetz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_le';
 CREATE FUNCTION upg_catalog.timetz_lt(upg_catalog.timetz, upg_catalog.timetz) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_lt';
 CREATE FUNCTION upg_catalog.tintervaleq(upg_catalog.tinterval, upg_catalog.tinterval) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervaleq';
 CREATE FUNCTION upg_catalog.tintervalge(upg_catalog.tinterval, upg_catalog.tinterval) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalge';
 CREATE FUNCTION upg_catalog.tintervalgt(upg_catalog.tinterval, upg_catalog.tinterval) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalgt';
 CREATE FUNCTION upg_catalog.tintervalle(upg_catalog.tinterval, upg_catalog.tinterval) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalle';
 CREATE FUNCTION upg_catalog.tintervallt(upg_catalog.tinterval, upg_catalog.tinterval) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervallt';
 CREATE FUNCTION upg_catalog.varbiteq(upg_catalog.varbit, upg_catalog.varbit) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'biteq';
 CREATE FUNCTION upg_catalog.varbitge(upg_catalog.varbit, upg_catalog.varbit) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bitge';
 CREATE FUNCTION upg_catalog.varbitgt(upg_catalog.varbit, upg_catalog.varbit) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bitgt';
 CREATE FUNCTION upg_catalog.varbitle(upg_catalog.varbit, upg_catalog.varbit) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bitle';
 CREATE FUNCTION upg_catalog.varbitlt(upg_catalog.varbit, upg_catalog.varbit) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'bitlt';
 CREATE FUNCTION upg_catalog.xideq(upg_catalog.xid, upg_catalog.xid) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'xideq';
 CREATE FUNCTION upg_catalog.xideqint4(upg_catalog.xid, upg_catalog.int4) RETURNS pg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'xideq';

