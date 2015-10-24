-- Aggregates defined in the 4.0 Catalog:
--
-- Derived via the following SQL run within a 4.0 catalog
/*
\o /tmp/aggregates

SELECT 'CREATE AGGREGATE upg_catalog.' || quote_ident(p.proname) || '('
    || coalesce(array_to_string(array(
          select 'upg_catalog.' || quote_ident(typname)
          from pg_type t, generate_series(1, p.pronargs) i
          where t.oid = p.proargtypes[i-1]
          order by i), ', '), '')
    || ') ('
    || E'\n  SFUNC     = ' || quote_ident(sfunc.proname)
    || E',\n  STYPE     = upg_catalog.' || quote_ident(stype.typname)
    || case when prefunc.oid is not null then E',\n  PREFUNC   = ' || quote_ident(prefunc.proname) else '' end
    || case when finfunc.oid is not null then E',\n  FINALFUNC = ' || quote_ident(finfunc.proname) else '' end
    || case when agginitval is not null then E',\n  INITCOND  = ''' || agginitval || '''' else '' end
    || case when sortop.oid is not null then E',\n  SORTOP    = ' || quote_ident(sortop.oprname) else '' end
    || E'\n);'
FROM pg_aggregate a
     join pg_proc p on (a.aggfnoid = p.oid)
     join pg_proc sfunc on (a.aggtransfn = sfunc.oid)
     join pg_type stype on (a.aggtranstype = stype.oid)
     left join pg_proc prefunc on (a.aggprelimfn = prefunc.oid)
     left join pg_proc finfunc on (a.aggfinalfn = finfunc.oid)
     left join pg_operator sortop on (a.aggsortop = sortop.oid)
ORDER BY 1;
*/
 CREATE AGGREGATE upg_catalog."every"(upg_catalog.bool) (
   SFUNC     = booland_statefunc,
   STYPE     = upg_catalog.bool,
   PREFUNC   = booland_statefunc
 );
 CREATE AGGREGATE upg_catalog.array_sum(upg_catalog._int4) (
   SFUNC     = array_add,
   STYPE     = upg_catalog._int4,
   PREFUNC   = array_add,
   INITCOND  = '{}'
 );
 CREATE AGGREGATE upg_catalog.avg(upg_catalog."interval") (
   SFUNC     = interval_accum,
   STYPE     = upg_catalog._interval,
   PREFUNC   = interval_amalg,
   FINALFUNC = interval_avg,
   INITCOND  = '{0 second,0 second}'
 );
 CREATE AGGREGATE upg_catalog.avg(upg_catalog."numeric") (
   SFUNC     = numeric_avg_accum,
   STYPE     = upg_catalog.bytea,
   PREFUNC   = numeric_avg_amalg,
   FINALFUNC = numeric_avg,
   INITCOND  = ''
 );
 CREATE AGGREGATE upg_catalog.avg(upg_catalog.float4) (
   SFUNC     = float4_avg_accum,
   STYPE     = upg_catalog.bytea,
   PREFUNC   = float8_avg_amalg,
   FINALFUNC = float8_avg,
   INITCOND  = ''
 );
 CREATE AGGREGATE upg_catalog.avg(upg_catalog.float8) (
   SFUNC     = float8_avg_accum,
   STYPE     = upg_catalog.bytea,
   PREFUNC   = float8_avg_amalg,
   FINALFUNC = float8_avg,
   INITCOND  = ''
 );
 CREATE AGGREGATE upg_catalog.avg(upg_catalog.int2) (
   SFUNC     = int2_avg_accum,
   STYPE     = upg_catalog.bytea,
   PREFUNC   = int8_avg_amalg,
   FINALFUNC = int8_avg,
   INITCOND  = ''
 );
 CREATE AGGREGATE upg_catalog.avg(upg_catalog.int4) (
   SFUNC     = int4_avg_accum,
   STYPE     = upg_catalog.bytea,
   PREFUNC   = int8_avg_amalg,
   FINALFUNC = int8_avg,
   INITCOND  = ''
 );
 CREATE AGGREGATE upg_catalog.avg(upg_catalog.int8) (
   SFUNC     = int8_avg_accum,
   STYPE     = upg_catalog.bytea,
   PREFUNC   = int8_avg_amalg,
   FINALFUNC = int8_avg,
   INITCOND  = ''
 );
 CREATE AGGREGATE upg_catalog.bit_and(upg_catalog."bit") (
   SFUNC     = bitand,
   STYPE     = upg_catalog."bit",
   PREFUNC   = bitand
 );
 CREATE AGGREGATE upg_catalog.bit_and(upg_catalog.int2) (
   SFUNC     = int2and,
   STYPE     = upg_catalog.int2,
   PREFUNC   = int2and
 );
 CREATE AGGREGATE upg_catalog.bit_and(upg_catalog.int4) (
   SFUNC     = int4and,
   STYPE     = upg_catalog.int4,
   PREFUNC   = int4and
 );
 CREATE AGGREGATE upg_catalog.bit_and(upg_catalog.int8) (
   SFUNC     = int8and,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8and
 );
 CREATE AGGREGATE upg_catalog.bit_or(upg_catalog."bit") (
   SFUNC     = bitor,
   STYPE     = upg_catalog."bit",
   PREFUNC   = bitor
 );
 CREATE AGGREGATE upg_catalog.bit_or(upg_catalog.int2) (
   SFUNC     = int2or,
   STYPE     = upg_catalog.int2,
   PREFUNC   = int2or
 );
 CREATE AGGREGATE upg_catalog.bit_or(upg_catalog.int4) (
   SFUNC     = int4or,
   STYPE     = upg_catalog.int4,
   PREFUNC   = int4or
 );
 CREATE AGGREGATE upg_catalog.bit_or(upg_catalog.int8) (
   SFUNC     = int8or,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8or
 );
 CREATE AGGREGATE upg_catalog.bool_and(upg_catalog.bool) (
   SFUNC     = booland_statefunc,
   STYPE     = upg_catalog.bool,
   PREFUNC   = booland_statefunc
 );
 CREATE AGGREGATE upg_catalog.bool_or(upg_catalog.bool) (
   SFUNC     = boolor_statefunc,
   STYPE     = upg_catalog.bool,
   PREFUNC   = boolor_statefunc
 );
 CREATE AGGREGATE upg_catalog.corr(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_corr,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.count() (
   SFUNC     = int8inc,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8pl,
   INITCOND  = '0'
 );
 CREATE AGGREGATE upg_catalog.count(upg_catalog."any") (
   SFUNC     = int8inc_any,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8pl,
   INITCOND  = '0'
 );
 CREATE AGGREGATE upg_catalog.covar_pop(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_covar_pop,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.covar_samp(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_covar_samp,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog."interval") (
   SFUNC     = interval_larger,
   STYPE     = upg_catalog."interval",
   PREFUNC   = interval_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog."numeric") (
   SFUNC     = numeric_larger,
   STYPE     = upg_catalog."numeric",
   PREFUNC   = numeric_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog."time") (
   SFUNC     = time_larger,
   STYPE     = upg_catalog."time",
   PREFUNC   = time_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog."timestamp") (
   SFUNC     = timestamp_larger,
   STYPE     = upg_catalog."timestamp",
   PREFUNC   = timestamp_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.abstime) (
   SFUNC     = int4larger,
   STYPE     = upg_catalog.abstime,
   PREFUNC   = int4larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.anyarray) (
   SFUNC     = array_larger,
   STYPE     = upg_catalog.anyarray,
   PREFUNC   = array_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.bpchar) (
   SFUNC     = bpchar_larger,
   STYPE     = upg_catalog.bpchar,
   PREFUNC   = bpchar_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.date) (
   SFUNC     = date_larger,
   STYPE     = upg_catalog.date,
   PREFUNC   = date_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.float4) (
   SFUNC     = float4larger,
   STYPE     = upg_catalog.float4,
   PREFUNC   = float4larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.float8) (
   SFUNC     = float8larger,
   STYPE     = upg_catalog.float8,
   PREFUNC   = float8larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.gpxlogloc) (
   SFUNC     = gpxlogloclarger,
   STYPE     = upg_catalog.gpxlogloc,
   PREFUNC   = gpxlogloclarger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.int2) (
   SFUNC     = int2larger,
   STYPE     = upg_catalog.int2,
   PREFUNC   = int2larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.int4) (
   SFUNC     = int4larger,
   STYPE     = upg_catalog.int4,
   PREFUNC   = int4larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.int8) (
   SFUNC     = int8larger,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.money) (
   SFUNC     = cashlarger,
   STYPE     = upg_catalog.money,
   PREFUNC   = cashlarger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.oid) (
   SFUNC     = oidlarger,
   STYPE     = upg_catalog.oid,
   PREFUNC   = oidlarger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.text) (
   SFUNC     = text_larger,
   STYPE     = upg_catalog.text,
   PREFUNC   = text_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.tid) (
   SFUNC     = tidlarger,
   STYPE     = upg_catalog.tid,
   PREFUNC   = tidlarger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.timestamptz) (
   SFUNC     = timestamptz_larger,
   STYPE     = upg_catalog.timestamptz,
   PREFUNC   = timestamptz_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.max(upg_catalog.timetz) (
   SFUNC     = timetz_larger,
   STYPE     = upg_catalog.timetz,
   PREFUNC   = timetz_larger,
   SORTOP    = ">"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog."interval") (
   SFUNC     = interval_smaller,
   STYPE     = upg_catalog."interval",
   PREFUNC   = interval_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog."numeric") (
   SFUNC     = numeric_smaller,
   STYPE     = upg_catalog."numeric",
   PREFUNC   = numeric_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog."time") (
   SFUNC     = time_smaller,
   STYPE     = upg_catalog."time",
   PREFUNC   = time_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog."timestamp") (
   SFUNC     = timestamp_smaller,
   STYPE     = upg_catalog."timestamp",
   PREFUNC   = timestamp_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.abstime) (
   SFUNC     = int4smaller,
   STYPE     = upg_catalog.abstime,
   PREFUNC   = int4smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.anyarray) (
   SFUNC     = array_smaller,
   STYPE     = upg_catalog.anyarray,
   PREFUNC   = array_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.bpchar) (
   SFUNC     = bpchar_smaller,
   STYPE     = upg_catalog.bpchar,
   PREFUNC   = bpchar_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.date) (
   SFUNC     = date_smaller,
   STYPE     = upg_catalog.date,
   PREFUNC   = date_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.float4) (
   SFUNC     = float4smaller,
   STYPE     = upg_catalog.float4,
   PREFUNC   = float4smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.float8) (
   SFUNC     = float8smaller,
   STYPE     = upg_catalog.float8,
   PREFUNC   = float8smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.gpxlogloc) (
   SFUNC     = gpxloglocsmaller,
   STYPE     = upg_catalog.gpxlogloc,
   PREFUNC   = gpxloglocsmaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.int2) (
   SFUNC     = int2smaller,
   STYPE     = upg_catalog.int2,
   PREFUNC   = int2smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.int4) (
   SFUNC     = int4smaller,
   STYPE     = upg_catalog.int4,
   PREFUNC   = int4smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.int8) (
   SFUNC     = int8smaller,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.money) (
   SFUNC     = cashsmaller,
   STYPE     = upg_catalog.money,
   PREFUNC   = cashsmaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.oid) (
   SFUNC     = oidsmaller,
   STYPE     = upg_catalog.oid,
   PREFUNC   = oidsmaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.text) (
   SFUNC     = text_smaller,
   STYPE     = upg_catalog.text,
   PREFUNC   = text_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.tid) (
   SFUNC     = tidsmaller,
   STYPE     = upg_catalog.tid,
   PREFUNC   = tidsmaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.timestamptz) (
   SFUNC     = timestamptz_smaller,
   STYPE     = upg_catalog.timestamptz,
   PREFUNC   = timestamptz_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.min(upg_catalog.timetz) (
   SFUNC     = timetz_smaller,
   STYPE     = upg_catalog.timetz,
   PREFUNC   = timetz_smaller,
   SORTOP    = "<"
 );
 CREATE AGGREGATE upg_catalog.mregr_coef(upg_catalog.float8, upg_catalog._float8) (
   SFUNC     = float8_mregr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_mregr_combine,
   FINALFUNC = float8_mregr_coef,
   INITCOND  = '{0}'
 );
 CREATE AGGREGATE upg_catalog.mregr_r2(upg_catalog.float8, upg_catalog._float8) (
   SFUNC     = float8_mregr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_mregr_combine,
   FINALFUNC = float8_mregr_r2,
   INITCOND  = '{0}'
 );
 CREATE AGGREGATE upg_catalog.nb_classify(upg_catalog._text, upg_catalog.int8, upg_catalog._int8, upg_catalog._int8) (
   SFUNC     = nb_classify_accum,
   STYPE     = upg_catalog.nb_classification,
   PREFUNC   = nb_classify_combine,
   FINALFUNC = nb_classify_final
 );
 CREATE AGGREGATE upg_catalog.pivot_sum(upg_catalog._text, upg_catalog.text, upg_catalog.float8) (
   SFUNC     = float8_pivot_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_matrix_accum
 );
 CREATE AGGREGATE upg_catalog.pivot_sum(upg_catalog._text, upg_catalog.text, upg_catalog.int4) (
   SFUNC     = int4_pivot_accum,
   STYPE     = upg_catalog._int4,
   PREFUNC   = int8_matrix_accum
 );
 CREATE AGGREGATE upg_catalog.pivot_sum(upg_catalog._text, upg_catalog.text, upg_catalog.int8) (
   SFUNC     = int8_pivot_accum,
   STYPE     = upg_catalog._int8,
   PREFUNC   = int8_matrix_accum
 );
 CREATE AGGREGATE upg_catalog.regr_avgx(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_regr_avgx,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.regr_avgy(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_regr_avgy,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.regr_count(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = int8inc_float8_float8,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8pl,
   INITCOND  = '0'
 );
 CREATE AGGREGATE upg_catalog.regr_intercept(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_regr_intercept,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.regr_r2(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_regr_r2,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.regr_slope(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_regr_slope,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.regr_sxx(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_regr_sxx,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.regr_sxy(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_regr_sxy,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.regr_syy(upg_catalog.float8, upg_catalog.float8) (
   SFUNC     = float8_regr_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_regr_amalg,
   FINALFUNC = float8_regr_syy,
   INITCOND  = '{0,0,0,0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev(upg_catalog."numeric") (
   SFUNC     = numeric_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev(upg_catalog.float4) (
   SFUNC     = float4_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev(upg_catalog.float8) (
   SFUNC     = float8_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev(upg_catalog.int2) (
   SFUNC     = int2_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev(upg_catalog.int4) (
   SFUNC     = int4_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev(upg_catalog.int8) (
   SFUNC     = int8_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_pop(upg_catalog."numeric") (
   SFUNC     = numeric_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_pop(upg_catalog.float4) (
   SFUNC     = float4_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_stddev_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_pop(upg_catalog.float8) (
   SFUNC     = float8_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_stddev_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_pop(upg_catalog.int2) (
   SFUNC     = int2_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_pop(upg_catalog.int4) (
   SFUNC     = int4_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_pop(upg_catalog.int8) (
   SFUNC     = int8_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_samp(upg_catalog."numeric") (
   SFUNC     = numeric_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_samp(upg_catalog.float4) (
   SFUNC     = float4_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_samp(upg_catalog.float8) (
   SFUNC     = float8_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_samp(upg_catalog.int2) (
   SFUNC     = int2_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_samp(upg_catalog.int4) (
   SFUNC     = int4_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.stddev_samp(upg_catalog.int8) (
   SFUNC     = int8_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_stddev_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog."interval") (
   SFUNC     = interval_pl,
   STYPE     = upg_catalog."interval",
   PREFUNC   = interval_pl
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog."numeric") (
   SFUNC     = numeric_add,
   STYPE     = upg_catalog."numeric",
   PREFUNC   = numeric_add
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog._float8) (
   SFUNC     = float8_matrix_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_matrix_accum
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog._int2) (
   SFUNC     = int2_matrix_accum,
   STYPE     = upg_catalog._int8,
   PREFUNC   = int8_matrix_accum
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog._int4) (
   SFUNC     = int4_matrix_accum,
   STYPE     = upg_catalog._int8,
   PREFUNC   = int8_matrix_accum
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog._int8) (
   SFUNC     = int8_matrix_accum,
   STYPE     = upg_catalog._int8,
   PREFUNC   = int8_matrix_accum
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog.float4) (
   SFUNC     = float4pl,
   STYPE     = upg_catalog.float4,
   PREFUNC   = float4pl
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog.float8) (
   SFUNC     = float8pl,
   STYPE     = upg_catalog.float8,
   PREFUNC   = float8pl
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog.int2) (
   SFUNC     = int2_sum,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8pl
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog.int4) (
   SFUNC     = int4_sum,
   STYPE     = upg_catalog.int8,
   PREFUNC   = int8pl
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog.int8) (
   SFUNC     = int8_sum,
   STYPE     = upg_catalog."numeric",
   PREFUNC   = numeric_add
 );
 CREATE AGGREGATE upg_catalog.sum(upg_catalog.money) (
   SFUNC     = cash_pl,
   STYPE     = upg_catalog.money,
   PREFUNC   = cash_pl
 );
 CREATE AGGREGATE upg_catalog.var_pop(upg_catalog."numeric") (
   SFUNC     = numeric_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_pop(upg_catalog.float4) (
   SFUNC     = float4_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_var_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_pop(upg_catalog.float8) (
   SFUNC     = float8_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_var_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_pop(upg_catalog.int2) (
   SFUNC     = int2_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_pop(upg_catalog.int4) (
   SFUNC     = int4_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_pop(upg_catalog.int8) (
   SFUNC     = int8_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_pop,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_samp(upg_catalog."numeric") (
   SFUNC     = numeric_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_samp(upg_catalog.float4) (
   SFUNC     = float4_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_samp(upg_catalog.float8) (
   SFUNC     = float8_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_samp(upg_catalog.int2) (
   SFUNC     = int2_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_samp(upg_catalog.int4) (
   SFUNC     = int4_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.var_samp(upg_catalog.int8) (
   SFUNC     = int8_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.variance(upg_catalog."numeric") (
   SFUNC     = numeric_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.variance(upg_catalog.float4) (
   SFUNC     = float4_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.variance(upg_catalog.float8) (
   SFUNC     = float8_accum,
   STYPE     = upg_catalog._float8,
   PREFUNC   = float8_amalg,
   FINALFUNC = float8_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.variance(upg_catalog.int2) (
   SFUNC     = int2_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.variance(upg_catalog.int4) (
   SFUNC     = int4_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_samp,
   INITCOND  = '{0,0,0}'
 );
 CREATE AGGREGATE upg_catalog.variance(upg_catalog.int8) (
   SFUNC     = int8_accum,
   STYPE     = upg_catalog._numeric,
   PREFUNC   = numeric_amalg,
   FINALFUNC = numeric_var_samp,
   INITCOND  = '{0,0,0}'
 );
