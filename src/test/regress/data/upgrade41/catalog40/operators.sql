-- Operators defined in the 4.0 Catalog:
--
-- Derived via the following SQL run within a 4.0 catalog
/*
\o /tmp/operators
SELECT 'CREATE OPERATOR upg_catalog.' || o.oprname || '('
    || E'\n  PROCEDURE = upg_catalog.' || quote_ident(p.proname)
    || case when tleft.typname is not null 
            then E',\n  LEFTARG = upg_catalog.' || quote_ident(tleft.typname) else '' end
    || case when tright.typname is not null 
            then E',\n  RIGHTARG = upg_catalog.' || quote_ident(tright.typname) else '' end
    || case when com.oprname is not null
            then E',\n  COMMUTATOR = ' || com.oprname else '' end
    || case when neg.oprname is not null
            then E',\n  NEGATOR = ' || neg.oprname else '' end
    || case when rest.proname is not null
            then E',\n  RESTRICT = upg_catalog.' || quote_ident(rest.proname) else '' end
    || case when pjoin.proname is not null
            then E',\n  JOIN = upg_catalog.' || quote_ident(pjoin.proname) else '' end
    || case when o.oprcanhash 
            then E',\n  HASHES' else '' end
    || case when sort1.oprname is not null
            then E',\n  SORT1 = ' || sort1.oprname else '' end
    || case when sort2.oprname is not null
            then E',\n  SORT2 = ' || sort2.oprname else '' end
    || case when ltcmp.oprname is not null
            then E',\n  LTCMP = ' || ltcmp.oprname else '' end
    || case when gtcmp.oprname is not null
            then E',\n  GTCMP = ' || gtcmp.oprname else '' end
    || E'\n);'
FROM pg_operator o
     join pg_namespace n on (o.oprnamespace = n.oid)
     join pg_proc p on (o.oprcode = p.oid)
     left join pg_type tleft on (o.oprleft = tleft.oid)
     left join pg_type tright on (o.oprright = tright.oid)
     left join pg_operator com on (o.oprcom = com.oid and o.oid > com.oid)
     left join pg_operator neg on (o.oprnegate = neg.oid and o.oid > neg.oid)
     left join pg_proc rest on (o.oprrest = rest.oid)
     left join pg_proc pjoin on (o.oprjoin = pjoin.oid)
     left join pg_operator sort1 on (o.oprlsortop = sort1.oid)
     left join pg_operator sort2 on (o.oprrsortop = sort2.oid)
     left join pg_operator ltcmp on (o.oprltcmpop = ltcmp.oid)
     left join pg_operator gtcmp on (o.oprgtcmpop = gtcmp.oid)
WHERE n.nspname = 'pg_catalog'
ORDER BY 1;
*/
 CREATE OPERATOR upg_catalog.!!(
   PROCEDURE = upg_catalog.numeric_fac,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.!!=(
   PROCEDURE = upg_catalog.int4notin,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.text
 );
 CREATE OPERATOR upg_catalog.!!=(
   PROCEDURE = upg_catalog.oidnotin,
   LEFTARG = upg_catalog.oid,
   RIGHTARG = upg_catalog.text
 );
 CREATE OPERATOR upg_catalog.!(
   PROCEDURE = upg_catalog.numeric_fac,
   LEFTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.!~(
   PROCEDURE = upg_catalog.bpcharregexne,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~,
   RESTRICT = upg_catalog.regexnesel,
   JOIN = upg_catalog.regexnejoinsel
 );
 CREATE OPERATOR upg_catalog.!~(
   PROCEDURE = upg_catalog.nameregexne,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~,
   RESTRICT = upg_catalog.regexnesel,
   JOIN = upg_catalog.regexnejoinsel
 );
 CREATE OPERATOR upg_catalog.!~(
   PROCEDURE = upg_catalog.textregexne,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~,
   RESTRICT = upg_catalog.regexnesel,
   JOIN = upg_catalog.regexnejoinsel
 );
 CREATE OPERATOR upg_catalog.!~*(
   PROCEDURE = upg_catalog.bpcharicregexne,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~*,
   RESTRICT = upg_catalog.icregexnesel,
   JOIN = upg_catalog.icregexnejoinsel
 );
 CREATE OPERATOR upg_catalog.!~*(
   PROCEDURE = upg_catalog.nameicregexne,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~*,
   RESTRICT = upg_catalog.icregexnesel,
   JOIN = upg_catalog.icregexnejoinsel
 );
 CREATE OPERATOR upg_catalog.!~*(
   PROCEDURE = upg_catalog.texticregexne,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~*,
   RESTRICT = upg_catalog.icregexnesel,
   JOIN = upg_catalog.icregexnejoinsel
 );
 CREATE OPERATOR upg_catalog.!~~(
   PROCEDURE = upg_catalog.bpcharnlike,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~~,
   RESTRICT = upg_catalog.nlikesel,
   JOIN = upg_catalog.nlikejoinsel
 );
 CREATE OPERATOR upg_catalog.!~~(
   PROCEDURE = upg_catalog.byteanlike,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea,
   NEGATOR = ~~,
   RESTRICT = upg_catalog.nlikesel,
   JOIN = upg_catalog.nlikejoinsel
 );
 CREATE OPERATOR upg_catalog.!~~(
   PROCEDURE = upg_catalog.namenlike,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~~,
   RESTRICT = upg_catalog.nlikesel,
   JOIN = upg_catalog.nlikejoinsel
 );
 CREATE OPERATOR upg_catalog.!~~(
   PROCEDURE = upg_catalog.textnlike,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~~,
   RESTRICT = upg_catalog.nlikesel,
   JOIN = upg_catalog.nlikejoinsel
 );
 CREATE OPERATOR upg_catalog.!~~*(
   PROCEDURE = upg_catalog.bpcharicnlike,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~~*,
   RESTRICT = upg_catalog.icnlikesel,
   JOIN = upg_catalog.icnlikejoinsel
 );
 CREATE OPERATOR upg_catalog.!~~*(
   PROCEDURE = upg_catalog.nameicnlike,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~~*,
   RESTRICT = upg_catalog.icnlikesel,
   JOIN = upg_catalog.icnlikejoinsel
 );
 CREATE OPERATOR upg_catalog.!~~*(
   PROCEDURE = upg_catalog.texticnlike,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   NEGATOR = ~~*,
   RESTRICT = upg_catalog.icnlikesel,
   JOIN = upg_catalog.icnlikejoinsel
 );
 CREATE OPERATOR upg_catalog.##(
   PROCEDURE = upg_catalog.close_lb,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.##(
   PROCEDURE = upg_catalog.close_ls,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.##(
   PROCEDURE = upg_catalog.close_lseg,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.##(
   PROCEDURE = upg_catalog.close_pb,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.##(
   PROCEDURE = upg_catalog.close_pl,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.##(
   PROCEDURE = upg_catalog.close_ps,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.##(
   PROCEDURE = upg_catalog.close_sb,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.##(
   PROCEDURE = upg_catalog.close_sl,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.bitxor,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = #
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.box_intersect,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.int2xor,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = #
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.int4xor,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = #
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.int8xor,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = #
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.line_interpt,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.line,
   COMMUTATOR = #
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.lseg_interpt,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = #
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.path_npoints,
   RIGHTARG = upg_catalog.path
 );
 CREATE OPERATOR upg_catalog.#(
   PROCEDURE = upg_catalog.poly_npoints,
   RIGHTARG = upg_catalog.polygon
 );
 CREATE OPERATOR upg_catalog.#<(
   PROCEDURE = upg_catalog.tintervallenlt,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.reltime,
   NEGATOR = #>=
 );
 CREATE OPERATOR upg_catalog.#<=(
   PROCEDURE = upg_catalog.tintervallenle,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.reltime,
   NEGATOR = #>
 );
 CREATE OPERATOR upg_catalog.#<>(
   PROCEDURE = upg_catalog.tintervallenne,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.reltime,
   NEGATOR = #=
 );
 CREATE OPERATOR upg_catalog.#=(
   PROCEDURE = upg_catalog.tintervalleneq,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.reltime,
   NEGATOR = #<>
 );
 CREATE OPERATOR upg_catalog.#>(
   PROCEDURE = upg_catalog.tintervallengt,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.reltime,
   NEGATOR = #<=
 );
 CREATE OPERATOR upg_catalog.#>=(
   PROCEDURE = upg_catalog.tintervallenge,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.reltime,
   NEGATOR = #<
 );
 CREATE OPERATOR upg_catalog.%(
   PROCEDURE = upg_catalog.int24mod,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.%(
   PROCEDURE = upg_catalog.int2mod,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog.%(
   PROCEDURE = upg_catalog.int42mod,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog.%(
   PROCEDURE = upg_catalog.int4mod,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.%(
   PROCEDURE = upg_catalog.int8mod,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.%(
   PROCEDURE = upg_catalog.numeric_mod,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric"
 );
 CREATE OPERATOR upg_catalog.&&(
   PROCEDURE = upg_catalog.arrayoverlap,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = &&,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.&&(
   PROCEDURE = upg_catalog.box_overlap,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = &&,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.&&(
   PROCEDURE = upg_catalog.circle_overlap,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = &&,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.&&(
   PROCEDURE = upg_catalog.poly_overlap,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = &&,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.&&(
   PROCEDURE = upg_catalog.tintervalov,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval,
   COMMUTATOR = &&
 );
 CREATE OPERATOR upg_catalog.&(
   PROCEDURE = upg_catalog.bitand,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = &
 );
 CREATE OPERATOR upg_catalog.&(
   PROCEDURE = upg_catalog.inetand,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet
 );
 CREATE OPERATOR upg_catalog.&(
   PROCEDURE = upg_catalog.int2and,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = &
 );
 CREATE OPERATOR upg_catalog.&(
   PROCEDURE = upg_catalog.int4and,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = &
 );
 CREATE OPERATOR upg_catalog.&(
   PROCEDURE = upg_catalog.int8and,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = &
 );
 CREATE OPERATOR upg_catalog.&<(
   PROCEDURE = upg_catalog.box_overleft,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.&<(
   PROCEDURE = upg_catalog.circle_overleft,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.&<(
   PROCEDURE = upg_catalog.poly_overleft,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.&<|(
   PROCEDURE = upg_catalog.box_overbelow,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.&<|(
   PROCEDURE = upg_catalog.circle_overbelow,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.&<|(
   PROCEDURE = upg_catalog.poly_overbelow,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.&>(
   PROCEDURE = upg_catalog.box_overright,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.&>(
   PROCEDURE = upg_catalog.circle_overright,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.&>(
   PROCEDURE = upg_catalog.poly_overright,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.box_mul,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.cash_mul_flt4,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.cash_mul_flt8,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.cash_mul_int2,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.cash_mul_int4,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.circle_mul_pt,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.float48mul,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.float4mul,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.float84mul,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.float8mul,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.flt4_mul_cash,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.flt8_mul_cash,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int24mul,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int2_mul_cash,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int2mul,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int42mul,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int48mul,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int4_mul_cash,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int4mul,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int84mul,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.int8mul,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.interval_mul,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.mul_d_interval,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.numeric_mul,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric",
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.path_mul_pt,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.*(
   PROCEDURE = upg_catalog.point_mul,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = *
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.aclinsert,
   LEFTARG = upg_catalog._aclitem,
   RIGHTARG = upg_catalog.aclitem
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.box_add,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.cash_pl,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.circle_add_pt,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.date_pl_interval,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.date_pli,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.datetime_pl,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."time",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.datetimetz_pl,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.timetz,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.float48pl,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.float4pl,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.float4up,
   RIGHTARG = upg_catalog.float4
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.float84pl,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.float8pl,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.float8up,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.inetpl,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int24pl,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int2pl,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int2up,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int42pl,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int48pl,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int4pl,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int4up,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int84pl,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int8pl,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int8pl_inet,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.int8up,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.integer_pl_date,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.interval_pl,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.interval_pl_date,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.interval_pl_time,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."time",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.interval_pl_timestamp,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.interval_pl_timestamptz,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.interval_pl_timetz,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog.timetz,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.numeric_add,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.numeric_uplus,
   RIGHTARG = upg_catalog."numeric"
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.path_add,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.path_add_pt,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.point_add,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.time_pl_interval,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.timedate_pl,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.timepl,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.reltime
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.timestamp_pl_interval,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.timestamptz_pl_interval,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.timetz_pl_interval,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.+(
   PROCEDURE = upg_catalog.timetzdate_pl,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = +
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.aclremove,
   LEFTARG = upg_catalog._aclitem,
   RIGHTARG = upg_catalog.aclitem
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.box_sub,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.cash_mi,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.money
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.circle_sub_pt,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.date_mi,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.date
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.date_mi_interval,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."interval"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.date_mii,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.float48mi,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.float4mi,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.float4um,
   RIGHTARG = upg_catalog.float4
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.float84mi,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.float8mi,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.float8um,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.inetmi,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.inetmi_int8,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int24mi,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int2mi,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int2um,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int42mi,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int48mi,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int4mi,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int4um,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int84mi,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int8mi,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.int8um,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.interval_mi,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."interval"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.interval_um,
   RIGHTARG = upg_catalog."interval"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.numeric_sub,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.numeric_uminus,
   RIGHTARG = upg_catalog."numeric"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.path_sub_pt,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.point_sub,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.time_mi_interval,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."interval"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.time_mi_time,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."time"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.timemi,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.reltime
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.timestamp_mi,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."timestamp"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.timestamp_mi_interval,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."interval"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.timestamptz_mi,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.timestamptz
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.timestamptz_mi_interval,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog."interval"
 );
 CREATE OPERATOR upg_catalog.-(
   PROCEDURE = upg_catalog.timetz_mi_interval,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog."interval"
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.box_div,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.cash_div_flt4,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.float4
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.cash_div_flt8,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.cash_div_int2,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.cash_div_int4,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.circle_div_pt,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.float48div,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.float4div,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.float84div,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.float8div,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.int24div,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.int2div,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.int42div,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.int48div,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.int4div,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.int84div,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.int8div,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.interval_div,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.numeric_div,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric"
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.path_div_pt,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog./(
   PROCEDURE = upg_catalog.point_div,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point
 );
 CREATE OPERATOR upg_catalog.<#>(
   PROCEDURE = upg_catalog.mktinterval,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.abstime
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.abstimelt,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.abstime,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.array_lt,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.bitlt,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.boollt,
   LEFTARG = upg_catalog.bool,
   RIGHTARG = upg_catalog.bool,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.box_lt,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.bpcharlt,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.bytealt,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.cash_lt,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.charlt,
   LEFTARG = upg_catalog."char",
   RIGHTARG = upg_catalog."char",
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.circle_lt,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.date_lt,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.date_lt_timestamp,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.date_lt_timestamptz,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.float48lt,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.float4lt,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.float84lt,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.float8lt,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.gpxlogloclt,
   LEFTARG = upg_catalog.gpxlogloc,
   RIGHTARG = upg_catalog.gpxlogloc,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int24lt,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int28lt,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int2lt,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int42lt,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int48lt,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int4lt,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int82lt,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int84lt,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.int8lt,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.interval_lt,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.lseg_lt,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = >,
   NEGATOR = >=
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.macaddr_lt,
   LEFTARG = upg_catalog.macaddr,
   RIGHTARG = upg_catalog.macaddr,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.namelt,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.network_lt,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.numeric_lt,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric",
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.oidlt,
   LEFTARG = upg_catalog.oid,
   RIGHTARG = upg_catalog.oid,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.oidvectorlt,
   LEFTARG = upg_catalog.oidvector,
   RIGHTARG = upg_catalog.oidvector,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.path_n_lt,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = >
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.reltimelt,
   LEFTARG = upg_catalog.reltime,
   RIGHTARG = upg_catalog.reltime,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.text_lt,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.tidlt,
   LEFTARG = upg_catalog.tid,
   RIGHTARG = upg_catalog.tid,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.time_lt,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."time",
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.timestamp_lt,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.timestamp_lt_date,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.timestamp_lt_timestamptz,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.timestamptz_lt,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.timestamptz_lt_date,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.timestamptz_lt_timestamp,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.timetz_lt,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog.timetz,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.tintervallt,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<(
   PROCEDURE = upg_catalog.varbitlt,
   LEFTARG = upg_catalog.varbit,
   RIGHTARG = upg_catalog.varbit,
   COMMUTATOR = >,
   NEGATOR = >=,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.box_distance,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = <->
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.circle_distance,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = <->
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_cpoly,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.polygon
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_lb,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_pb,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_pc,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.circle
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_pl,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_ppath,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.path
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_ps,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_sb,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.dist_sl,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.line_distance,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.line,
   COMMUTATOR = <->
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.lseg_distance,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = <->
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.path_distance,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = <->
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.point_distance,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = <->
 );
 CREATE OPERATOR upg_catalog.<->(
   PROCEDURE = upg_catalog.poly_distance,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = <->
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.bitshiftleft,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.box_left,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.circle_left,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.int2shl,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.int4shl,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.int8shl,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.network_sub,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = >>
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.point_left,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.poly_left,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.<<(
   PROCEDURE = upg_catalog.tintervalct,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval
 );
 CREATE OPERATOR upg_catalog.<<=(
   PROCEDURE = upg_catalog.network_subeq,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = >>=
 );
 CREATE OPERATOR upg_catalog.<<|(
   PROCEDURE = upg_catalog.box_below,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.<<|(
   PROCEDURE = upg_catalog.circle_below,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.<<|(
   PROCEDURE = upg_catalog.poly_below,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.abstimele,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.abstime,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.array_le,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.bitle,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.boolle,
   LEFTARG = upg_catalog.bool,
   RIGHTARG = upg_catalog.bool,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.box_le,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.bpcharle,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.byteale,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.cash_le,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.charle,
   LEFTARG = upg_catalog."char",
   RIGHTARG = upg_catalog."char",
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.circle_le,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.date_le,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.date_le_timestamp,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.date_le_timestamptz,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.float48le,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.float4le,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.float84le,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.float8le,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.gpxloglocle,
   LEFTARG = upg_catalog.gpxlogloc,
   RIGHTARG = upg_catalog.gpxlogloc,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int24le,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int28le,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int2le,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int42le,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int48le,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int4le,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int82le,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int84le,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.int8le,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.interval_le,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.lseg_le,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = >=,
   NEGATOR = >
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.macaddr_le,
   LEFTARG = upg_catalog.macaddr,
   RIGHTARG = upg_catalog.macaddr,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.namele,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.network_le,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.numeric_le,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric",
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.oidle,
   LEFTARG = upg_catalog.oid,
   RIGHTARG = upg_catalog.oid,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.oidvectorle,
   LEFTARG = upg_catalog.oidvector,
   RIGHTARG = upg_catalog.oidvector,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.path_n_le,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = >=
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.reltimele,
   LEFTARG = upg_catalog.reltime,
   RIGHTARG = upg_catalog.reltime,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.text_le,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.tidle,
   LEFTARG = upg_catalog.tid,
   RIGHTARG = upg_catalog.tid,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.time_le,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."time",
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.timestamp_le,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.timestamp_le_date,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.timestamp_le_timestamptz,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.timestamptz_le,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.timestamptz_le_date,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.timestamptz_le_timestamp,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.timetz_le,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog.timetz,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.tintervalle,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<=(
   PROCEDURE = upg_catalog.varbitle,
   LEFTARG = upg_catalog.varbit,
   RIGHTARG = upg_catalog.varbit,
   COMMUTATOR = >=,
   NEGATOR = >,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.abstimene,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.abstime,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.array_ne,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.bitne,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.boolne,
   LEFTARG = upg_catalog.bool,
   RIGHTARG = upg_catalog.bool,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.bpcharne,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.byteane,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.cash_ne,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.charne,
   LEFTARG = upg_catalog."char",
   RIGHTARG = upg_catalog."char",
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.circle_ne,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.date_ne,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.date_ne_timestamp,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.date_ne_timestamptz,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.float48ne,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.float4ne,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.float84ne,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.float8ne,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.gpxloglocne,
   LEFTARG = upg_catalog.gpxlogloc,
   RIGHTARG = upg_catalog.gpxlogloc,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int24ne,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int28ne,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int2ne,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int42ne,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int48ne,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int4ne,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int82ne,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int84ne,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.int8ne,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.interval_ne,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.lseg_ne,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.macaddr_ne,
   LEFTARG = upg_catalog.macaddr,
   RIGHTARG = upg_catalog.macaddr,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.namene,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.network_ne,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.numeric_ne,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric",
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.oidne,
   LEFTARG = upg_catalog.oid,
   RIGHTARG = upg_catalog.oid,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.oidvectorne,
   LEFTARG = upg_catalog.oidvector,
   RIGHTARG = upg_catalog.oidvector,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.point_ne,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = <>,
   NEGATOR = ~=,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.reltimene,
   LEFTARG = upg_catalog.reltime,
   RIGHTARG = upg_catalog.reltime,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.textne,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.tidne,
   LEFTARG = upg_catalog.tid,
   RIGHTARG = upg_catalog.tid,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.time_ne,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."time",
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.timestamp_ne,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.timestamp_ne_date,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.timestamp_ne_timestamptz,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.timestamptz_ne,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.timestamptz_ne_date,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.timestamptz_ne_timestamp,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.timetz_ne,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog.timetz,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.tintervalne,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<>(
   PROCEDURE = upg_catalog.varbitne,
   LEFTARG = upg_catalog.varbit,
   RIGHTARG = upg_catalog.varbit,
   COMMUTATOR = <>,
   NEGATOR = =,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.<?>(
   PROCEDURE = upg_catalog.intinterval,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.tinterval
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.arraycontained,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = @>,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.box_contained,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = @>,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.circle_contained,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = @>,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.on_pb,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.on_pl,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.on_ppath,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = @>
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.on_ps,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.on_sb,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.on_sl,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.poly_contained,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = @>,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.pt_contained_circle,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = @>
 );
 CREATE OPERATOR upg_catalog.<@(
   PROCEDURE = upg_catalog.pt_contained_poly,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = @>
 );
 CREATE OPERATOR upg_catalog.<^(
   PROCEDURE = upg_catalog.box_below_eq,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.<^(
   PROCEDURE = upg_catalog.point_below,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.abstimeeq,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.abstime,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.aclitemeq,
   LEFTARG = upg_catalog.aclitem,
   RIGHTARG = upg_catalog.aclitem,
   COMMUTATOR = =,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.array_eq,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.biteq,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.booleq,
   LEFTARG = upg_catalog.bool,
   RIGHTARG = upg_catalog.bool,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.box_eq,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = =,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.bpchareq,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.byteaeq,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.cash_eq,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.chareq,
   LEFTARG = upg_catalog."char",
   RIGHTARG = upg_catalog."char",
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.cideq,
   LEFTARG = upg_catalog.cid,
   RIGHTARG = upg_catalog.cid,
   COMMUTATOR = =,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.circle_eq,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.date_eq,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.date_eq_timestamp,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.date_eq_timestamptz,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.float48eq,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.float4eq,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.float84eq,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.float8eq,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.gpxlogloceq,
   LEFTARG = upg_catalog.gpxlogloc,
   RIGHTARG = upg_catalog.gpxlogloc,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int24eq,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int28eq,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int2eq,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int2vectoreq,
   LEFTARG = upg_catalog.int2vector,
   RIGHTARG = upg_catalog.int2vector,
   COMMUTATOR = =,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int42eq,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int48eq,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int4eq,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int82eq,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int84eq,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.int8eq,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.interval_eq,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.line_eq,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.line,
   COMMUTATOR = =,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.lseg_eq,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.macaddr_eq,
   LEFTARG = upg_catalog.macaddr,
   RIGHTARG = upg_catalog.macaddr,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.nameeq,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.network_eq,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.numeric_eq,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric",
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.oideq,
   LEFTARG = upg_catalog.oid,
   RIGHTARG = upg_catalog.oid,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.oidvectoreq,
   LEFTARG = upg_catalog.oidvector,
   RIGHTARG = upg_catalog.oidvector,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.path_n_eq,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = =,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.reltimeeq,
   LEFTARG = upg_catalog.reltime,
   RIGHTARG = upg_catalog.reltime,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.texteq,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.tideq,
   LEFTARG = upg_catalog.tid,
   RIGHTARG = upg_catalog.tid,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.time_eq,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."time",
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.timestamp_eq,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.timestamp_eq_date,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.timestamp_eq_timestamptz,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.timestamptz_eq,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.timestamptz_eq_date,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.timestamptz_eq_timestamp,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.timetz_eq,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog.timetz,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.tintervaleq,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.varbiteq,
   LEFTARG = upg_catalog.varbit,
   RIGHTARG = upg_catalog.varbit,
   COMMUTATOR = =,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   SORT1 = <,
   SORT2 = <,
   LTCMP = <,
   GTCMP = >
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.xideq,
   LEFTARG = upg_catalog.xid,
   RIGHTARG = upg_catalog.xid,
   COMMUTATOR = =,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES
 );
 CREATE OPERATOR upg_catalog.=(
   PROCEDURE = upg_catalog.xideqint4,
   LEFTARG = upg_catalog.xid,
   RIGHTARG = upg_catalog.int4,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.abstimegt,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.abstime,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.array_gt,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.bitgt,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.boolgt,
   LEFTARG = upg_catalog.bool,
   RIGHTARG = upg_catalog.bool,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.box_gt,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.bpchargt,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.byteagt,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.cash_gt,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.chargt,
   LEFTARG = upg_catalog."char",
   RIGHTARG = upg_catalog."char",
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.circle_gt,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.date_gt,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.date_gt_timestamp,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.date_gt_timestamptz,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.float48gt,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.float4gt,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.float84gt,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.float8gt,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.gpxloglocgt,
   LEFTARG = upg_catalog.gpxlogloc,
   RIGHTARG = upg_catalog.gpxlogloc,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int24gt,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int28gt,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int2gt,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int42gt,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int48gt,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int4gt,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int82gt,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int84gt,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.int8gt,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.interval_gt,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.lseg_gt,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = <,
   NEGATOR = <=
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.macaddr_gt,
   LEFTARG = upg_catalog.macaddr,
   RIGHTARG = upg_catalog.macaddr,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.namegt,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.network_gt,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.numeric_gt,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric",
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.oidgt,
   LEFTARG = upg_catalog.oid,
   RIGHTARG = upg_catalog.oid,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.oidvectorgt,
   LEFTARG = upg_catalog.oidvector,
   RIGHTARG = upg_catalog.oidvector,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.path_n_gt,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = <
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.reltimegt,
   LEFTARG = upg_catalog.reltime,
   RIGHTARG = upg_catalog.reltime,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.text_gt,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.tidgt,
   LEFTARG = upg_catalog.tid,
   RIGHTARG = upg_catalog.tid,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.time_gt,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."time",
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.timestamp_gt,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.timestamp_gt_date,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.timestamp_gt_timestamptz,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.timestamptz_gt,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.timestamptz_gt_date,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.timestamptz_gt_timestamp,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.timetz_gt,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog.timetz,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.tintervalgt,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>(
   PROCEDURE = upg_catalog.varbitgt,
   LEFTARG = upg_catalog.varbit,
   RIGHTARG = upg_catalog.varbit,
   COMMUTATOR = <,
   NEGATOR = <=,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.abstimege,
   LEFTARG = upg_catalog.abstime,
   RIGHTARG = upg_catalog.abstime,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.array_ge,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.bitge,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.boolge,
   LEFTARG = upg_catalog.bool,
   RIGHTARG = upg_catalog.bool,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.box_ge,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.bpcharge,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.byteage,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.cash_ge,
   LEFTARG = upg_catalog.money,
   RIGHTARG = upg_catalog.money,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.charge,
   LEFTARG = upg_catalog."char",
   RIGHTARG = upg_catalog."char",
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.circle_ge,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.date_ge,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.date_ge_timestamp,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.date_ge_timestamptz,
   LEFTARG = upg_catalog.date,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.float48ge,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.float4ge,
   LEFTARG = upg_catalog.float4,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.float84ge,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float4,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.float8ge,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.gpxloglocge,
   LEFTARG = upg_catalog.gpxlogloc,
   RIGHTARG = upg_catalog.gpxlogloc,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int24ge,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int28ge,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int2ge,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int42ge,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int48ge,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int4ge,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int82ge,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int84ge,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.int8ge,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.interval_ge,
   LEFTARG = upg_catalog."interval",
   RIGHTARG = upg_catalog."interval",
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.lseg_ge,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = <=,
   NEGATOR = <
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.macaddr_ge,
   LEFTARG = upg_catalog.macaddr,
   RIGHTARG = upg_catalog.macaddr,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.namege,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.network_ge,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.numeric_ge,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric",
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.oidge,
   LEFTARG = upg_catalog.oid,
   RIGHTARG = upg_catalog.oid,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.oidvectorge,
   LEFTARG = upg_catalog.oidvector,
   RIGHTARG = upg_catalog.oidvector,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.path_n_ge,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = <=
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.reltimege,
   LEFTARG = upg_catalog.reltime,
   RIGHTARG = upg_catalog.reltime,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.text_ge,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.tidge,
   LEFTARG = upg_catalog.tid,
   RIGHTARG = upg_catalog.tid,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.time_ge,
   LEFTARG = upg_catalog."time",
   RIGHTARG = upg_catalog."time",
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.timestamp_ge,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.timestamp_ge_date,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.timestamp_ge_timestamptz,
   LEFTARG = upg_catalog."timestamp",
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.timestamptz_ge,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.timestamptz,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.timestamptz_ge_date,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog.date,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.timestamptz_ge_timestamp,
   LEFTARG = upg_catalog.timestamptz,
   RIGHTARG = upg_catalog."timestamp",
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.timetz_ge,
   LEFTARG = upg_catalog.timetz,
   RIGHTARG = upg_catalog.timetz,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.tintervalge,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>=(
   PROCEDURE = upg_catalog.varbitge,
   LEFTARG = upg_catalog.varbit,
   RIGHTARG = upg_catalog.varbit,
   COMMUTATOR = <=,
   NEGATOR = <,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.bitshiftright,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.box_right,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.circle_right,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.int2shr,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.int4shr,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.int8shr,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.network_sup,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = <<
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.point_right,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.>>(
   PROCEDURE = upg_catalog.poly_right,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.>>=(
   PROCEDURE = upg_catalog.network_supeq,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet,
   COMMUTATOR = <<=
 );
 CREATE OPERATOR upg_catalog.>^(
   PROCEDURE = upg_catalog.box_above_eq,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.>^(
   PROCEDURE = upg_catalog.point_above,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.?#(
   PROCEDURE = upg_catalog.box_overlap,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.areasel,
   JOIN = upg_catalog.areajoinsel
 );
 CREATE OPERATOR upg_catalog.?#(
   PROCEDURE = upg_catalog.inter_lb,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.?#(
   PROCEDURE = upg_catalog.inter_sb,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.?#(
   PROCEDURE = upg_catalog.inter_sl,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.?#(
   PROCEDURE = upg_catalog.line_intersect,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.line,
   COMMUTATOR = ?#
 );
 CREATE OPERATOR upg_catalog.?#(
   PROCEDURE = upg_catalog.lseg_intersect,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = ?#
 );
 CREATE OPERATOR upg_catalog.?#(
   PROCEDURE = upg_catalog.path_inter,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.path
 );
 CREATE OPERATOR upg_catalog.?-(
   PROCEDURE = upg_catalog.line_horizontal,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.?-(
   PROCEDURE = upg_catalog.lseg_horizontal,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.?-(
   PROCEDURE = upg_catalog.point_horiz,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = ?-
 );
 CREATE OPERATOR upg_catalog.?-|(
   PROCEDURE = upg_catalog.line_perp,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.line,
   COMMUTATOR = ?-|
 );
 CREATE OPERATOR upg_catalog.?-|(
   PROCEDURE = upg_catalog.lseg_perp,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = ?-|
 );
 CREATE OPERATOR upg_catalog.?|(
   PROCEDURE = upg_catalog.line_vertical,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.?|(
   PROCEDURE = upg_catalog.lseg_vertical,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.?|(
   PROCEDURE = upg_catalog.point_vert,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = ?|
 );
 CREATE OPERATOR upg_catalog.?||(
   PROCEDURE = upg_catalog.line_parallel,
   LEFTARG = upg_catalog.line,
   RIGHTARG = upg_catalog.line,
   COMMUTATOR = ?||
 );
 CREATE OPERATOR upg_catalog.?||(
   PROCEDURE = upg_catalog.lseg_parallel,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.lseg,
   COMMUTATOR = ?||
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.box_contained,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = ~,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.circle_contained,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = ~,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.float4abs,
   RIGHTARG = upg_catalog.float4
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.float8abs,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.int2abs,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.int4abs,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.int8abs,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.numeric_abs,
   RIGHTARG = upg_catalog."numeric"
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.on_pb,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.on_pl,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.on_ppath,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.path,
   COMMUTATOR = ~
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.on_ps,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.on_sb,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.on_sl,
   LEFTARG = upg_catalog.lseg,
   RIGHTARG = upg_catalog.line
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.poly_contained,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = ~,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.pt_contained_circle,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = ~
 );
 CREATE OPERATOR upg_catalog.@(
   PROCEDURE = upg_catalog.pt_contained_poly,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = ~
 );
 CREATE OPERATOR upg_catalog.@-@(
   PROCEDURE = upg_catalog.lseg_length,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.@-@(
   PROCEDURE = upg_catalog.path_length,
   RIGHTARG = upg_catalog.path
 );
 CREATE OPERATOR upg_catalog.@>(
   PROCEDURE = upg_catalog.aclcontains,
   LEFTARG = upg_catalog._aclitem,
   RIGHTARG = upg_catalog.aclitem
 );
 CREATE OPERATOR upg_catalog.@>(
   PROCEDURE = upg_catalog.arraycontains,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray,
   COMMUTATOR = <@,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.@>(
   PROCEDURE = upg_catalog.box_contain,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = <@,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.@>(
   PROCEDURE = upg_catalog.circle_contain,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = <@,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.@>(
   PROCEDURE = upg_catalog.circle_contain_pt,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = <@
 );
 CREATE OPERATOR upg_catalog.@>(
   PROCEDURE = upg_catalog.path_contain_pt,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = <@
 );
 CREATE OPERATOR upg_catalog.@>(
   PROCEDURE = upg_catalog.poly_contain,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = <@,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.@>(
   PROCEDURE = upg_catalog.poly_contain_pt,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = <@
 );
 CREATE OPERATOR upg_catalog.@@(
   PROCEDURE = upg_catalog.box_center,
   RIGHTARG = upg_catalog.box
 );
 CREATE OPERATOR upg_catalog.@@(
   PROCEDURE = upg_catalog.circle_center,
   RIGHTARG = upg_catalog.circle
 );
 CREATE OPERATOR upg_catalog.@@(
   PROCEDURE = upg_catalog.lseg_center,
   RIGHTARG = upg_catalog.lseg
 );
 CREATE OPERATOR upg_catalog.@@(
   PROCEDURE = upg_catalog.path_center,
   RIGHTARG = upg_catalog.path
 );
 CREATE OPERATOR upg_catalog.@@(
   PROCEDURE = upg_catalog.poly_center,
   RIGHTARG = upg_catalog.polygon
 );
 CREATE OPERATOR upg_catalog.^(
   PROCEDURE = upg_catalog.dpow,
   LEFTARG = upg_catalog.float8,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog.^(
   PROCEDURE = upg_catalog.numeric_power,
   LEFTARG = upg_catalog."numeric",
   RIGHTARG = upg_catalog."numeric"
 );
 CREATE OPERATOR upg_catalog.|&>(
   PROCEDURE = upg_catalog.box_overabove,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.|&>(
   PROCEDURE = upg_catalog.circle_overabove,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.|&>(
   PROCEDURE = upg_catalog.poly_overabove,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.|(
   PROCEDURE = upg_catalog.bitor,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit",
   COMMUTATOR = |
 );
 CREATE OPERATOR upg_catalog.|(
   PROCEDURE = upg_catalog.inetor,
   LEFTARG = upg_catalog.inet,
   RIGHTARG = upg_catalog.inet
 );
 CREATE OPERATOR upg_catalog.|(
   PROCEDURE = upg_catalog.int2or,
   LEFTARG = upg_catalog.int2,
   RIGHTARG = upg_catalog.int2,
   COMMUTATOR = |
 );
 CREATE OPERATOR upg_catalog.|(
   PROCEDURE = upg_catalog.int4or,
   LEFTARG = upg_catalog.int4,
   RIGHTARG = upg_catalog.int4,
   COMMUTATOR = |
 );
 CREATE OPERATOR upg_catalog.|(
   PROCEDURE = upg_catalog.int8or,
   LEFTARG = upg_catalog.int8,
   RIGHTARG = upg_catalog.int8,
   COMMUTATOR = |
 );
 CREATE OPERATOR upg_catalog.|(
   PROCEDURE = upg_catalog.tintervalstart,
   RIGHTARG = upg_catalog.tinterval
 );
 CREATE OPERATOR upg_catalog.|/(
   PROCEDURE = upg_catalog.dsqrt,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog.|>>(
   PROCEDURE = upg_catalog.box_above,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.|>>(
   PROCEDURE = upg_catalog.circle_above,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.|>>(
   PROCEDURE = upg_catalog.poly_above,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   RESTRICT = upg_catalog.positionsel,
   JOIN = upg_catalog.positionjoinsel
 );
 CREATE OPERATOR upg_catalog.||(
   PROCEDURE = upg_catalog.array_append,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyelement
 );
 CREATE OPERATOR upg_catalog.||(
   PROCEDURE = upg_catalog.array_cat,
   LEFTARG = upg_catalog.anyarray,
   RIGHTARG = upg_catalog.anyarray
 );
 CREATE OPERATOR upg_catalog.||(
   PROCEDURE = upg_catalog.array_prepend,
   LEFTARG = upg_catalog.anyelement,
   RIGHTARG = upg_catalog.anyarray
 );
 CREATE OPERATOR upg_catalog.||(
   PROCEDURE = upg_catalog.bitcat,
   LEFTARG = upg_catalog."bit",
   RIGHTARG = upg_catalog."bit"
 );
 CREATE OPERATOR upg_catalog.||(
   PROCEDURE = upg_catalog.byteacat,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea
 );
 CREATE OPERATOR upg_catalog.||(
   PROCEDURE = upg_catalog.textcat,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text
 );
 CREATE OPERATOR upg_catalog.||/(
   PROCEDURE = upg_catalog.dcbrt,
   RIGHTARG = upg_catalog.float8
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.aclcontains,
   LEFTARG = upg_catalog._aclitem,
   RIGHTARG = upg_catalog.aclitem
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.bitnot,
   RIGHTARG = upg_catalog."bit"
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.box_contain,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = @,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.bpcharregexeq,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~,
   RESTRICT = upg_catalog.regexeqsel,
   JOIN = upg_catalog.regexeqjoinsel
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.circle_contain,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = @,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.circle_contain_pt,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = @
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.inetnot,
   RIGHTARG = upg_catalog.inet
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.int2not,
   RIGHTARG = upg_catalog.int2
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.int4not,
   RIGHTARG = upg_catalog.int4
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.int8not,
   RIGHTARG = upg_catalog.int8
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.nameregexeq,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~,
   RESTRICT = upg_catalog.regexeqsel,
   JOIN = upg_catalog.regexeqjoinsel
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.path_contain_pt,
   LEFTARG = upg_catalog.path,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = @
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.poly_contain,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = @,
   RESTRICT = upg_catalog.contsel,
   JOIN = upg_catalog.contjoinsel
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.poly_contain_pt,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = @
 );
 CREATE OPERATOR upg_catalog.~(
   PROCEDURE = upg_catalog.textregexeq,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~,
   RESTRICT = upg_catalog.regexeqsel,
   JOIN = upg_catalog.regexeqjoinsel
 );
 CREATE OPERATOR upg_catalog.~*(
   PROCEDURE = upg_catalog.bpcharicregexeq,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~*,
   RESTRICT = upg_catalog.icregexeqsel,
   JOIN = upg_catalog.icregexeqjoinsel
 );
 CREATE OPERATOR upg_catalog.~*(
   PROCEDURE = upg_catalog.nameicregexeq,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~*,
   RESTRICT = upg_catalog.icregexeqsel,
   JOIN = upg_catalog.icregexeqjoinsel
 );
 CREATE OPERATOR upg_catalog.~*(
   PROCEDURE = upg_catalog.texticregexeq,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~*,
   RESTRICT = upg_catalog.icregexeqsel,
   JOIN = upg_catalog.icregexeqjoinsel
 );
 CREATE OPERATOR upg_catalog.~<=~(
   PROCEDURE = upg_catalog.bpchar_pattern_le,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = ~>=~,
   NEGATOR = ~>~,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.~<=~(
   PROCEDURE = upg_catalog.name_pattern_le,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = ~>=~,
   NEGATOR = ~>~,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.~<=~(
   PROCEDURE = upg_catalog.text_pattern_le,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = ~>=~,
   NEGATOR = ~>~,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.~<>~(
   PROCEDURE = upg_catalog.bpchar_pattern_ne,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = ~<>~,
   NEGATOR = ~=~,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.~<>~(
   PROCEDURE = upg_catalog.name_pattern_ne,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = ~<>~,
   NEGATOR = ~=~,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.~<>~(
   PROCEDURE = upg_catalog.textne,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = ~<>~,
   NEGATOR = ~=~,
   RESTRICT = upg_catalog.neqsel,
   JOIN = upg_catalog.neqjoinsel
 );
 CREATE OPERATOR upg_catalog.~<~(
   PROCEDURE = upg_catalog.bpchar_pattern_lt,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = ~>~,
   NEGATOR = ~>=~,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.~<~(
   PROCEDURE = upg_catalog.name_pattern_lt,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = ~>~,
   NEGATOR = ~>=~,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.~<~(
   PROCEDURE = upg_catalog.text_pattern_lt,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = ~>~,
   NEGATOR = ~>=~,
   RESTRICT = upg_catalog.scalarltsel,
   JOIN = upg_catalog.scalarltjoinsel
 );
 CREATE OPERATOR upg_catalog.~=(
   PROCEDURE = upg_catalog.box_same,
   LEFTARG = upg_catalog.box,
   RIGHTARG = upg_catalog.box,
   COMMUTATOR = ~=,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.~=(
   PROCEDURE = upg_catalog.circle_same,
   LEFTARG = upg_catalog.circle,
   RIGHTARG = upg_catalog.circle,
   COMMUTATOR = ~=,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.~=(
   PROCEDURE = upg_catalog.point_eq,
   LEFTARG = upg_catalog.point,
   RIGHTARG = upg_catalog.point,
   COMMUTATOR = ~=,
   NEGATOR = <>,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.~=(
   PROCEDURE = upg_catalog.poly_same,
   LEFTARG = upg_catalog.polygon,
   RIGHTARG = upg_catalog.polygon,
   COMMUTATOR = ~=,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.~=(
   PROCEDURE = upg_catalog.tintervalsame,
   LEFTARG = upg_catalog.tinterval,
   RIGHTARG = upg_catalog.tinterval,
   COMMUTATOR = ~=,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel
 );
 CREATE OPERATOR upg_catalog.~=~(
   PROCEDURE = upg_catalog.bpchar_pattern_eq,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = ~=~,
   NEGATOR = ~<>~,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = ~<~,
   SORT2 = ~<~,
   LTCMP = ~<~,
   GTCMP = ~>~
 );
 CREATE OPERATOR upg_catalog.~=~(
   PROCEDURE = upg_catalog.name_pattern_eq,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = ~=~,
   NEGATOR = ~<>~,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = ~<~,
   SORT2 = ~<~,
   LTCMP = ~<~,
   GTCMP = ~>~
 );
 CREATE OPERATOR upg_catalog.~=~(
   PROCEDURE = upg_catalog.texteq,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = ~=~,
   NEGATOR = ~<>~,
   RESTRICT = upg_catalog.eqsel,
   JOIN = upg_catalog.eqjoinsel,
   HASHES,
   SORT1 = ~<~,
   SORT2 = ~<~,
   LTCMP = ~<~,
   GTCMP = ~>~
 );
 CREATE OPERATOR upg_catalog.~>=~(
   PROCEDURE = upg_catalog.bpchar_pattern_ge,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = ~<=~,
   NEGATOR = ~<~,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.~>=~(
   PROCEDURE = upg_catalog.name_pattern_ge,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = ~<=~,
   NEGATOR = ~<~,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.~>=~(
   PROCEDURE = upg_catalog.text_pattern_ge,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = ~<=~,
   NEGATOR = ~<~,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.~>~(
   PROCEDURE = upg_catalog.bpchar_pattern_gt,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.bpchar,
   COMMUTATOR = ~<~,
   NEGATOR = ~<=~,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.~>~(
   PROCEDURE = upg_catalog.name_pattern_gt,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.name,
   COMMUTATOR = ~<~,
   NEGATOR = ~<=~,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.~>~(
   PROCEDURE = upg_catalog.text_pattern_gt,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   COMMUTATOR = ~<~,
   NEGATOR = ~<=~,
   RESTRICT = upg_catalog.scalargtsel,
   JOIN = upg_catalog.scalargtjoinsel
 );
 CREATE OPERATOR upg_catalog.~~(
   PROCEDURE = upg_catalog.bpcharlike,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~~,
   RESTRICT = upg_catalog.likesel,
   JOIN = upg_catalog.likejoinsel
 );
 CREATE OPERATOR upg_catalog.~~(
   PROCEDURE = upg_catalog.bytealike,
   LEFTARG = upg_catalog.bytea,
   RIGHTARG = upg_catalog.bytea,
   NEGATOR = !~~,
   RESTRICT = upg_catalog.likesel,
   JOIN = upg_catalog.likejoinsel
 );
 CREATE OPERATOR upg_catalog.~~(
   PROCEDURE = upg_catalog.namelike,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~~,
   RESTRICT = upg_catalog.likesel,
   JOIN = upg_catalog.likejoinsel
 );
 CREATE OPERATOR upg_catalog.~~(
   PROCEDURE = upg_catalog.textlike,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~~,
   RESTRICT = upg_catalog.likesel,
   JOIN = upg_catalog.likejoinsel
 );
 CREATE OPERATOR upg_catalog.~~*(
   PROCEDURE = upg_catalog.bpchariclike,
   LEFTARG = upg_catalog.bpchar,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~~*,
   RESTRICT = upg_catalog.iclikesel,
   JOIN = upg_catalog.iclikejoinsel
 );
 CREATE OPERATOR upg_catalog.~~*(
   PROCEDURE = upg_catalog.nameiclike,
   LEFTARG = upg_catalog.name,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~~*,
   RESTRICT = upg_catalog.iclikesel,
   JOIN = upg_catalog.iclikejoinsel
 );
 CREATE OPERATOR upg_catalog.~~*(
   PROCEDURE = upg_catalog.texticlike,
   LEFTARG = upg_catalog.text,
   RIGHTARG = upg_catalog.text,
   NEGATOR = !~~*,
   RESTRICT = upg_catalog.iclikesel,
   JOIN = upg_catalog.iclikejoinsel
 );
