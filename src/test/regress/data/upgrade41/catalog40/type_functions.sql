-- Type I/O Functions defined in the 4.0 Catalog
--
-- See upgrade_functions40.sql for non shell types
--
-- Derived via the following SQL run within a 4.0 catalog
/*
SELECT 
    'CREATE FUNCTION upg_catalog.' || quote_ident(proname)
    || '(' 
    || coalesce(
        array_to_string( array(
            select coalesce(proargnames[i] || ' ','') 
                || case when p.oid in (select unnest(array[typoutput, typsend]) from pg_type)
                        then 'upg_catalog.'
                        else 'pg_catalog.' end
                || quote_ident(typname)
            from pg_type t, generate_series(1, pronargs) i
            where t.oid = proargtypes[i-1]
            order by i), ', '),
        '')
    || ') RETURNS '
    || case when proretset then 'SETOF ' else '' end 
    || case when p.oid in (select unnest(array[typinput, typreceive]) from pg_type)
            then 'upg_catalog.'
            else 'pg_catalog.' end
    || quote_ident(r.typname) || ' '
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
  and proisagg = 'f'
  and proiswin = 'f'
  and p.oid in (select unnest(array[typinput, typoutput, typreceive, typsend, typanalyze])
                from pg_type)
order by 1
;
*/
 CREATE FUNCTION upg_catalog.abstimein(pg_catalog.cstring) RETURNS upg_catalog.abstime LANGUAGE internal STABLE STRICT AS 'abstimein';
 CREATE FUNCTION upg_catalog.abstimeout(upg_catalog.abstime) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'abstimeout';
 CREATE FUNCTION upg_catalog.abstimerecv(pg_catalog.internal) RETURNS upg_catalog.abstime LANGUAGE internal IMMUTABLE STRICT AS 'abstimerecv';
 CREATE FUNCTION upg_catalog.abstimesend(upg_catalog.abstime) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'abstimesend';
 CREATE FUNCTION upg_catalog.aclitemin(pg_catalog.cstring) RETURNS upg_catalog.aclitem LANGUAGE internal STABLE STRICT AS 'aclitemin';
 CREATE FUNCTION upg_catalog.aclitemout(upg_catalog.aclitem) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'aclitemout';
 CREATE FUNCTION upg_catalog.any_in(pg_catalog.cstring) RETURNS upg_catalog."any" LANGUAGE internal IMMUTABLE STRICT AS 'any_in';
 CREATE FUNCTION upg_catalog.any_out(upg_catalog."any") RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'any_out';
 CREATE FUNCTION upg_catalog.anyarray_in(pg_catalog.cstring) RETURNS upg_catalog.anyarray LANGUAGE internal IMMUTABLE STRICT AS 'anyarray_in';
 CREATE FUNCTION upg_catalog.anyarray_out(upg_catalog.anyarray) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'anyarray_out';
 CREATE FUNCTION upg_catalog.anyarray_recv(pg_catalog.internal) RETURNS upg_catalog.anyarray LANGUAGE internal STABLE STRICT AS 'anyarray_recv';
 CREATE FUNCTION upg_catalog.anyarray_send(upg_catalog.anyarray) RETURNS pg_catalog.bytea LANGUAGE internal STABLE STRICT AS 'anyarray_send';
 CREATE FUNCTION upg_catalog.anyelement_in(pg_catalog.cstring) RETURNS upg_catalog.anyelement LANGUAGE internal IMMUTABLE STRICT AS 'anyelement_in';
 CREATE FUNCTION upg_catalog.anyelement_out(upg_catalog.anyelement) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'anyelement_out';
 CREATE FUNCTION upg_catalog.array_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.anyarray LANGUAGE internal STABLE STRICT AS 'array_in';
 CREATE FUNCTION upg_catalog.array_out(upg_catalog.anyarray) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'array_out';
 CREATE FUNCTION upg_catalog.array_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.anyarray LANGUAGE internal STABLE STRICT AS 'array_recv';
 CREATE FUNCTION upg_catalog.array_send(upg_catalog.anyarray) RETURNS pg_catalog.bytea LANGUAGE internal STABLE STRICT AS 'array_send';
 CREATE FUNCTION upg_catalog.bit_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."bit" LANGUAGE internal IMMUTABLE STRICT AS 'bit_in';
 CREATE FUNCTION upg_catalog.bit_out(upg_catalog."bit") RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'bit_out';
 CREATE FUNCTION upg_catalog.bit_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."bit" LANGUAGE internal IMMUTABLE STRICT AS 'bit_recv';
 CREATE FUNCTION upg_catalog.bit_send(upg_catalog."bit") RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'bit_send';
 CREATE FUNCTION upg_catalog.boolin(pg_catalog.cstring) RETURNS upg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'boolin';
 CREATE FUNCTION upg_catalog.boolout(upg_catalog.bool) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'boolout';
 CREATE FUNCTION upg_catalog.boolrecv(pg_catalog.internal) RETURNS upg_catalog.bool LANGUAGE internal IMMUTABLE STRICT AS 'boolrecv';
 CREATE FUNCTION upg_catalog.boolsend(upg_catalog.bool) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'boolsend';
 CREATE FUNCTION upg_catalog.box_in(pg_catalog.cstring) RETURNS upg_catalog.box LANGUAGE internal IMMUTABLE STRICT AS 'box_in';
 CREATE FUNCTION upg_catalog.box_out(upg_catalog.box) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'box_out';
 CREATE FUNCTION upg_catalog.box_recv(pg_catalog.internal) RETURNS upg_catalog.box LANGUAGE internal IMMUTABLE STRICT AS 'box_recv';
 CREATE FUNCTION upg_catalog.box_send(upg_catalog.box) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'box_send';
 CREATE FUNCTION upg_catalog.bpcharin(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.bpchar LANGUAGE internal IMMUTABLE STRICT AS 'bpcharin';
 CREATE FUNCTION upg_catalog.bpcharout(upg_catalog.bpchar) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'bpcharout';
 CREATE FUNCTION upg_catalog.bpcharrecv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.bpchar LANGUAGE internal STABLE STRICT AS 'bpcharrecv';
 CREATE FUNCTION upg_catalog.bpcharsend(upg_catalog.bpchar) RETURNS pg_catalog.bytea LANGUAGE internal STABLE STRICT AS 'bpcharsend';
 CREATE FUNCTION upg_catalog.byteain(pg_catalog.cstring) RETURNS upg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'byteain';
 CREATE FUNCTION upg_catalog.byteaout(upg_catalog.bytea) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'byteaout';
 CREATE FUNCTION upg_catalog.bytearecv(pg_catalog.internal) RETURNS upg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'bytearecv';
 CREATE FUNCTION upg_catalog.byteasend(upg_catalog.bytea) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'byteasend';
 CREATE FUNCTION upg_catalog.cash_in(pg_catalog.cstring) RETURNS upg_catalog.money LANGUAGE internal IMMUTABLE STRICT AS 'cash_in';
 CREATE FUNCTION upg_catalog.cash_out(upg_catalog.money) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'cash_out';
 CREATE FUNCTION upg_catalog.cash_recv(pg_catalog.internal) RETURNS upg_catalog.money LANGUAGE internal IMMUTABLE STRICT AS 'cash_recv';
 CREATE FUNCTION upg_catalog.cash_send(upg_catalog.money) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'cash_send';
 CREATE FUNCTION upg_catalog.charin(pg_catalog.cstring) RETURNS upg_catalog."char" LANGUAGE internal IMMUTABLE STRICT AS 'charin';
 CREATE FUNCTION upg_catalog.charout(upg_catalog."char") RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'charout';
 CREATE FUNCTION upg_catalog.charrecv(pg_catalog.internal) RETURNS upg_catalog."char" LANGUAGE internal IMMUTABLE STRICT AS 'charrecv';
 CREATE FUNCTION upg_catalog.charsend(upg_catalog."char") RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'charsend';
 CREATE FUNCTION upg_catalog.cidin(pg_catalog.cstring) RETURNS upg_catalog.cid LANGUAGE internal IMMUTABLE STRICT AS 'cidin';
 CREATE FUNCTION upg_catalog.cidout(upg_catalog.cid) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'cidout';
 CREATE FUNCTION upg_catalog.cidr_in(pg_catalog.cstring) RETURNS upg_catalog.cidr LANGUAGE internal IMMUTABLE STRICT AS 'cidr_in';
 CREATE FUNCTION upg_catalog.cidr_out(upg_catalog.cidr) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'cidr_out';
 CREATE FUNCTION upg_catalog.cidr_recv(pg_catalog.internal) RETURNS upg_catalog.cidr LANGUAGE internal IMMUTABLE STRICT AS 'cidr_recv';
 CREATE FUNCTION upg_catalog.cidr_send(upg_catalog.cidr) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'cidr_send';
 CREATE FUNCTION upg_catalog.cidrecv(pg_catalog.internal) RETURNS upg_catalog.cid LANGUAGE internal IMMUTABLE STRICT AS 'cidrecv';
 CREATE FUNCTION upg_catalog.cidsend(upg_catalog.cid) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'cidsend';
 CREATE FUNCTION upg_catalog.circle_in(pg_catalog.cstring) RETURNS upg_catalog.circle LANGUAGE internal IMMUTABLE STRICT AS 'circle_in';
 CREATE FUNCTION upg_catalog.circle_out(upg_catalog.circle) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'circle_out';
 CREATE FUNCTION upg_catalog.circle_recv(pg_catalog.internal) RETURNS upg_catalog.circle LANGUAGE internal IMMUTABLE STRICT AS 'circle_recv';
 CREATE FUNCTION upg_catalog.circle_send(upg_catalog.circle) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'circle_send';
 CREATE FUNCTION upg_catalog.cstring_in(pg_catalog.cstring) RETURNS upg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'cstring_in';
 CREATE FUNCTION upg_catalog.cstring_out(upg_catalog.cstring) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'cstring_out';
 CREATE FUNCTION upg_catalog.cstring_recv(pg_catalog.internal) RETURNS upg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'cstring_recv';
 CREATE FUNCTION upg_catalog.cstring_send(upg_catalog.cstring) RETURNS pg_catalog.bytea LANGUAGE internal STABLE STRICT AS 'cstring_send';
 CREATE FUNCTION upg_catalog.date_in(pg_catalog.cstring) RETURNS upg_catalog.date LANGUAGE internal STABLE STRICT AS 'date_in';
 CREATE FUNCTION upg_catalog.date_out(upg_catalog.date) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'date_out';
 CREATE FUNCTION upg_catalog.date_recv(pg_catalog.internal) RETURNS upg_catalog.date LANGUAGE internal IMMUTABLE STRICT AS 'date_recv';
 CREATE FUNCTION upg_catalog.date_send(upg_catalog.date) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'date_send';
 CREATE FUNCTION upg_catalog.domain_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."any" LANGUAGE internal VOLATILE AS 'domain_in';
 CREATE FUNCTION upg_catalog.domain_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."any" LANGUAGE internal VOLATILE AS 'domain_recv';
 CREATE FUNCTION upg_catalog.float4in(pg_catalog.cstring) RETURNS upg_catalog.float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4in';
 CREATE FUNCTION upg_catalog.float4out(upg_catalog.float4) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'float4out';
 CREATE FUNCTION upg_catalog.float4recv(pg_catalog.internal) RETURNS upg_catalog.float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4recv';
 CREATE FUNCTION upg_catalog.float4send(upg_catalog.float4) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'float4send';
 CREATE FUNCTION upg_catalog.float8in(pg_catalog.cstring) RETURNS upg_catalog.float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8in';
 CREATE FUNCTION upg_catalog.float8out(upg_catalog.float8) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'float8out';
 CREATE FUNCTION upg_catalog.float8recv(pg_catalog.internal) RETURNS upg_catalog.float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8recv';
 CREATE FUNCTION upg_catalog.float8send(upg_catalog.float8) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'float8send';
 CREATE FUNCTION upg_catalog.gpaotidin(pg_catalog.cstring) RETURNS upg_catalog.gpaotid LANGUAGE internal IMMUTABLE STRICT AS 'gpaotidin';
 CREATE FUNCTION upg_catalog.gpaotidout(upg_catalog.gpaotid) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'gpaotidout';
 CREATE FUNCTION upg_catalog.gpaotidrecv(pg_catalog.internal) RETURNS upg_catalog.gpaotid LANGUAGE internal IMMUTABLE STRICT AS 'gpaotidrecv';
 CREATE FUNCTION upg_catalog.gpaotidsend(upg_catalog.gpaotid) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'gpaotidsend';
 CREATE FUNCTION upg_catalog.gpxloglocin(pg_catalog.cstring) RETURNS upg_catalog.gpxlogloc LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocin';
 CREATE FUNCTION upg_catalog.gpxloglocout(upg_catalog.gpxlogloc) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocout';
 CREATE FUNCTION upg_catalog.gpxloglocrecv(pg_catalog.internal) RETURNS upg_catalog.gpxlogloc LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocrecv';
 CREATE FUNCTION upg_catalog.gpxloglocsend(upg_catalog.gpxlogloc) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocsend';
 CREATE FUNCTION upg_catalog.inet_in(pg_catalog.cstring) RETURNS upg_catalog.inet LANGUAGE internal IMMUTABLE STRICT AS 'inet_in';
 CREATE FUNCTION upg_catalog.inet_out(upg_catalog.inet) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'inet_out';
 CREATE FUNCTION upg_catalog.inet_recv(pg_catalog.internal) RETURNS upg_catalog.inet LANGUAGE internal IMMUTABLE STRICT AS 'inet_recv';
 CREATE FUNCTION upg_catalog.inet_send(upg_catalog.inet) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'inet_send';
 CREATE FUNCTION upg_catalog.int2in(pg_catalog.cstring) RETURNS upg_catalog.int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2in';
 CREATE FUNCTION upg_catalog.int2out(upg_catalog.int2) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'int2out';
 CREATE FUNCTION upg_catalog.int2recv(pg_catalog.internal) RETURNS upg_catalog.int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2recv';
 CREATE FUNCTION upg_catalog.int2send(upg_catalog.int2) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'int2send';
 CREATE FUNCTION upg_catalog.int2vectorin(pg_catalog.cstring) RETURNS upg_catalog.int2vector LANGUAGE internal IMMUTABLE STRICT AS 'int2vectorin';
 CREATE FUNCTION upg_catalog.int2vectorout(upg_catalog.int2vector) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'int2vectorout';
 CREATE FUNCTION upg_catalog.int2vectorrecv(pg_catalog.internal) RETURNS upg_catalog.int2vector LANGUAGE internal IMMUTABLE STRICT AS 'int2vectorrecv';
 CREATE FUNCTION upg_catalog.int2vectorsend(upg_catalog.int2vector) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'int2vectorsend';
 CREATE FUNCTION upg_catalog.int4in(pg_catalog.cstring) RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4in';
 CREATE FUNCTION upg_catalog.int4out(upg_catalog.int4) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'int4out';
 CREATE FUNCTION upg_catalog.int4recv(pg_catalog.internal) RETURNS upg_catalog.int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4recv';
 CREATE FUNCTION upg_catalog.int4send(upg_catalog.int4) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'int4send';
 CREATE FUNCTION upg_catalog.int8in(pg_catalog.cstring) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8in';
 CREATE FUNCTION upg_catalog.int8out(upg_catalog.int8) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'int8out';
 CREATE FUNCTION upg_catalog.int8recv(pg_catalog.internal) RETURNS upg_catalog.int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8recv';
 CREATE FUNCTION upg_catalog.int8send(upg_catalog.int8) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'int8send';
 CREATE FUNCTION upg_catalog.internal_in(pg_catalog.cstring) RETURNS upg_catalog.internal LANGUAGE internal IMMUTABLE STRICT AS 'internal_in';
 CREATE FUNCTION upg_catalog.internal_out(upg_catalog.internal) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'internal_out';
 CREATE FUNCTION upg_catalog.interval_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."interval" LANGUAGE internal STABLE STRICT AS 'interval_in';
 CREATE FUNCTION upg_catalog.interval_out(upg_catalog."interval") RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'interval_out';
 CREATE FUNCTION upg_catalog.interval_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_recv';
 CREATE FUNCTION upg_catalog.interval_send(upg_catalog."interval") RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'interval_send';
 CREATE FUNCTION upg_catalog.language_handler_in(pg_catalog.cstring) RETURNS upg_catalog.language_handler LANGUAGE internal IMMUTABLE STRICT AS 'language_handler_in';
 CREATE FUNCTION upg_catalog.language_handler_out(upg_catalog.language_handler) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'language_handler_out';
 CREATE FUNCTION upg_catalog.line_in(pg_catalog.cstring) RETURNS upg_catalog.line LANGUAGE internal IMMUTABLE STRICT AS 'line_in';
 CREATE FUNCTION upg_catalog.line_out(upg_catalog.line) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'line_out';
 CREATE FUNCTION upg_catalog.line_recv(pg_catalog.internal) RETURNS upg_catalog.line LANGUAGE internal IMMUTABLE STRICT AS 'line_recv';
 CREATE FUNCTION upg_catalog.line_send(upg_catalog.line) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'line_send';
 CREATE FUNCTION upg_catalog.lseg_in(pg_catalog.cstring) RETURNS upg_catalog.lseg LANGUAGE internal IMMUTABLE STRICT AS 'lseg_in';
 CREATE FUNCTION upg_catalog.lseg_out(upg_catalog.lseg) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'lseg_out';
 CREATE FUNCTION upg_catalog.lseg_recv(pg_catalog.internal) RETURNS upg_catalog.lseg LANGUAGE internal IMMUTABLE STRICT AS 'lseg_recv';
 CREATE FUNCTION upg_catalog.lseg_send(upg_catalog.lseg) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'lseg_send';
 CREATE FUNCTION upg_catalog.macaddr_in(pg_catalog.cstring) RETURNS upg_catalog.macaddr LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_in';
 CREATE FUNCTION upg_catalog.macaddr_out(upg_catalog.macaddr) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_out';
 CREATE FUNCTION upg_catalog.macaddr_recv(pg_catalog.internal) RETURNS upg_catalog.macaddr LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_recv';
 CREATE FUNCTION upg_catalog.macaddr_send(upg_catalog.macaddr) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_send';
 CREATE FUNCTION upg_catalog.namein(pg_catalog.cstring) RETURNS upg_catalog.name LANGUAGE internal IMMUTABLE STRICT AS 'namein';
 CREATE FUNCTION upg_catalog.nameout(upg_catalog.name) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'nameout';
 CREATE FUNCTION upg_catalog.namerecv(pg_catalog.internal) RETURNS upg_catalog.name LANGUAGE internal STABLE STRICT AS 'namerecv';
 CREATE FUNCTION upg_catalog.namesend(upg_catalog.name) RETURNS pg_catalog.bytea LANGUAGE internal STABLE STRICT AS 'namesend';
 CREATE FUNCTION upg_catalog.numeric_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_in';
 CREATE FUNCTION upg_catalog.numeric_out(upg_catalog."numeric") RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'numeric_out';
 CREATE FUNCTION upg_catalog.numeric_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_recv';
 CREATE FUNCTION upg_catalog.numeric_send(upg_catalog."numeric") RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'numeric_send';
 CREATE FUNCTION upg_catalog.oidin(pg_catalog.cstring) RETURNS upg_catalog.oid LANGUAGE internal IMMUTABLE STRICT AS 'oidin';
 CREATE FUNCTION upg_catalog.oidout(upg_catalog.oid) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'oidout';
 CREATE FUNCTION upg_catalog.oidrecv(pg_catalog.internal) RETURNS upg_catalog.oid LANGUAGE internal IMMUTABLE STRICT AS 'oidrecv';
 CREATE FUNCTION upg_catalog.oidsend(upg_catalog.oid) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'oidsend';
 CREATE FUNCTION upg_catalog.oidvectorin(pg_catalog.cstring) RETURNS upg_catalog.oidvector LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorin';
 CREATE FUNCTION upg_catalog.oidvectorout(upg_catalog.oidvector) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorout';
 CREATE FUNCTION upg_catalog.oidvectorrecv(pg_catalog.internal) RETURNS upg_catalog.oidvector LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorrecv';
 CREATE FUNCTION upg_catalog.oidvectorsend(upg_catalog.oidvector) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorsend';
 CREATE FUNCTION upg_catalog.opaque_in(pg_catalog.cstring) RETURNS upg_catalog.opaque LANGUAGE internal IMMUTABLE STRICT AS 'opaque_in';
 CREATE FUNCTION upg_catalog.opaque_out(upg_catalog.opaque) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'opaque_out';
 CREATE FUNCTION upg_catalog.path_in(pg_catalog.cstring) RETURNS upg_catalog.path LANGUAGE internal IMMUTABLE STRICT AS 'path_in';
 CREATE FUNCTION upg_catalog.path_out(upg_catalog.path) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'path_out';
 CREATE FUNCTION upg_catalog.path_recv(pg_catalog.internal) RETURNS upg_catalog.path LANGUAGE internal IMMUTABLE STRICT AS 'path_recv';
 CREATE FUNCTION upg_catalog.path_send(upg_catalog.path) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'path_send';
 CREATE FUNCTION upg_catalog.point_in(pg_catalog.cstring) RETURNS upg_catalog.point LANGUAGE internal IMMUTABLE STRICT AS 'point_in';
 CREATE FUNCTION upg_catalog.point_out(upg_catalog.point) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'point_out';
 CREATE FUNCTION upg_catalog.point_recv(pg_catalog.internal) RETURNS upg_catalog.point LANGUAGE internal IMMUTABLE STRICT AS 'point_recv';
 CREATE FUNCTION upg_catalog.point_send(upg_catalog.point) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'point_send';
 CREATE FUNCTION upg_catalog.poly_in(pg_catalog.cstring) RETURNS upg_catalog.polygon LANGUAGE internal IMMUTABLE STRICT AS 'poly_in';
 CREATE FUNCTION upg_catalog.poly_out(upg_catalog.polygon) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'poly_out';
 CREATE FUNCTION upg_catalog.poly_recv(pg_catalog.internal) RETURNS upg_catalog.polygon LANGUAGE internal IMMUTABLE STRICT AS 'poly_recv';
 CREATE FUNCTION upg_catalog.poly_send(upg_catalog.polygon) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'poly_send';
 CREATE FUNCTION upg_catalog.record_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.record LANGUAGE internal VOLATILE STRICT AS 'record_in';
 CREATE FUNCTION upg_catalog.record_out(upg_catalog.record) RETURNS pg_catalog.cstring LANGUAGE internal VOLATILE STRICT AS 'record_out';
 CREATE FUNCTION upg_catalog.record_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.record LANGUAGE internal VOLATILE STRICT AS 'record_recv';
 CREATE FUNCTION upg_catalog.record_send(upg_catalog.record) RETURNS pg_catalog.bytea LANGUAGE internal VOLATILE STRICT AS 'record_send';
 CREATE FUNCTION upg_catalog.regclassin(pg_catalog.cstring) RETURNS upg_catalog.regclass LANGUAGE internal STABLE STRICT AS 'regclassin';
 CREATE FUNCTION upg_catalog.regclassout(upg_catalog.regclass) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'regclassout';
 CREATE FUNCTION upg_catalog.regclassrecv(pg_catalog.internal) RETURNS upg_catalog.regclass LANGUAGE internal IMMUTABLE STRICT AS 'regclassrecv';
 CREATE FUNCTION upg_catalog.regclasssend(upg_catalog.regclass) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'regclasssend';
 CREATE FUNCTION upg_catalog.regoperatorin(pg_catalog.cstring) RETURNS upg_catalog.regoperator LANGUAGE internal STABLE STRICT AS 'regoperatorin';
 CREATE FUNCTION upg_catalog.regoperatorout(upg_catalog.regoperator) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'regoperatorout';
 CREATE FUNCTION upg_catalog.regoperatorrecv(pg_catalog.internal) RETURNS upg_catalog.regoperator LANGUAGE internal IMMUTABLE STRICT AS 'regoperatorrecv';
 CREATE FUNCTION upg_catalog.regoperatorsend(upg_catalog.regoperator) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'regoperatorsend';
 CREATE FUNCTION upg_catalog.regoperin(pg_catalog.cstring) RETURNS upg_catalog.regoper LANGUAGE internal STABLE STRICT AS 'regoperin';
 CREATE FUNCTION upg_catalog.regoperout(upg_catalog.regoper) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'regoperout';
 CREATE FUNCTION upg_catalog.regoperrecv(pg_catalog.internal) RETURNS upg_catalog.regoper LANGUAGE internal IMMUTABLE STRICT AS 'regoperrecv';
 CREATE FUNCTION upg_catalog.regopersend(upg_catalog.regoper) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'regopersend';
 CREATE FUNCTION upg_catalog.regprocedurein(pg_catalog.cstring) RETURNS upg_catalog.regprocedure LANGUAGE internal STABLE STRICT AS 'regprocedurein';
 CREATE FUNCTION upg_catalog.regprocedureout(upg_catalog.regprocedure) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'regprocedureout';
 CREATE FUNCTION upg_catalog.regprocedurerecv(pg_catalog.internal) RETURNS upg_catalog.regprocedure LANGUAGE internal IMMUTABLE STRICT AS 'regprocedurerecv';
 CREATE FUNCTION upg_catalog.regproceduresend(upg_catalog.regprocedure) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'regproceduresend';
 CREATE FUNCTION upg_catalog.regprocin(pg_catalog.cstring) RETURNS upg_catalog.regproc LANGUAGE internal STABLE STRICT AS 'regprocin';
 CREATE FUNCTION upg_catalog.regprocout(upg_catalog.regproc) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'regprocout';
 CREATE FUNCTION upg_catalog.regprocrecv(pg_catalog.internal) RETURNS upg_catalog.regproc LANGUAGE internal IMMUTABLE STRICT AS 'regprocrecv';
 CREATE FUNCTION upg_catalog.regprocsend(upg_catalog.regproc) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'regprocsend';
 CREATE FUNCTION upg_catalog.regtypein(pg_catalog.cstring) RETURNS upg_catalog.regtype LANGUAGE internal STABLE STRICT AS 'regtypein';
 CREATE FUNCTION upg_catalog.regtypeout(upg_catalog.regtype) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'regtypeout';
 CREATE FUNCTION upg_catalog.regtyperecv(pg_catalog.internal) RETURNS upg_catalog.regtype LANGUAGE internal IMMUTABLE STRICT AS 'regtyperecv';
 CREATE FUNCTION upg_catalog.regtypesend(upg_catalog.regtype) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'regtypesend';
 CREATE FUNCTION upg_catalog.reltimein(pg_catalog.cstring) RETURNS upg_catalog.reltime LANGUAGE internal STABLE STRICT AS 'reltimein';
 CREATE FUNCTION upg_catalog.reltimeout(upg_catalog.reltime) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'reltimeout';
 CREATE FUNCTION upg_catalog.reltimerecv(pg_catalog.internal) RETURNS upg_catalog.reltime LANGUAGE internal IMMUTABLE STRICT AS 'reltimerecv';
 CREATE FUNCTION upg_catalog.reltimesend(upg_catalog.reltime) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'reltimesend';
 CREATE FUNCTION upg_catalog.shell_in(pg_catalog.cstring) RETURNS upg_catalog.opaque LANGUAGE internal IMMUTABLE STRICT AS 'shell_in';
 CREATE FUNCTION upg_catalog.shell_out(upg_catalog.opaque) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'shell_out';
 CREATE FUNCTION upg_catalog.smgrin(pg_catalog.cstring) RETURNS upg_catalog.smgr LANGUAGE internal STABLE STRICT AS 'smgrin';
 CREATE FUNCTION upg_catalog.smgrout(upg_catalog.smgr) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'smgrout';
 CREATE FUNCTION upg_catalog.textin(pg_catalog.cstring) RETURNS upg_catalog.text LANGUAGE internal IMMUTABLE STRICT AS 'textin';
 CREATE FUNCTION upg_catalog.textout(upg_catalog.text) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'textout';
 CREATE FUNCTION upg_catalog.textrecv(pg_catalog.internal) RETURNS upg_catalog.text LANGUAGE internal STABLE STRICT AS 'textrecv';
 CREATE FUNCTION upg_catalog.textsend(upg_catalog.text) RETURNS pg_catalog.bytea LANGUAGE internal STABLE STRICT AS 'textsend';
 CREATE FUNCTION upg_catalog.tidin(pg_catalog.cstring) RETURNS upg_catalog.tid LANGUAGE internal IMMUTABLE STRICT AS 'tidin';
 CREATE FUNCTION upg_catalog.tidout(upg_catalog.tid) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'tidout';
 CREATE FUNCTION upg_catalog.tidrecv(pg_catalog.internal) RETURNS upg_catalog.tid LANGUAGE internal IMMUTABLE STRICT AS 'tidrecv';
 CREATE FUNCTION upg_catalog.tidsend(upg_catalog.tid) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'tidsend';
 CREATE FUNCTION upg_catalog.time_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."time" LANGUAGE internal STABLE STRICT AS 'time_in';
 CREATE FUNCTION upg_catalog.time_out(upg_catalog."time") RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'time_out';
 CREATE FUNCTION upg_catalog.time_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."time" LANGUAGE internal IMMUTABLE STRICT AS 'time_recv';
 CREATE FUNCTION upg_catalog.time_send(upg_catalog."time") RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'time_send';
 CREATE FUNCTION upg_catalog.timestamp_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."timestamp" LANGUAGE internal STABLE STRICT AS 'timestamp_in';
 CREATE FUNCTION upg_catalog.timestamp_out(upg_catalog."timestamp") RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'timestamp_out';
 CREATE FUNCTION upg_catalog.timestamp_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_recv';
 CREATE FUNCTION upg_catalog.timestamp_send(upg_catalog."timestamp") RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_send';
 CREATE FUNCTION upg_catalog.timestamptz_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.timestamptz LANGUAGE internal STABLE STRICT AS 'timestamptz_in';
 CREATE FUNCTION upg_catalog.timestamptz_out(upg_catalog.timestamptz) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'timestamptz_out';
 CREATE FUNCTION upg_catalog.timestamptz_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_recv';
 CREATE FUNCTION upg_catalog.timestamptz_send(upg_catalog.timestamptz) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_send';
 CREATE FUNCTION upg_catalog.timetz_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.timetz LANGUAGE internal STABLE STRICT AS 'timetz_in';
 CREATE FUNCTION upg_catalog.timetz_out(upg_catalog.timetz) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'timetz_out';
 CREATE FUNCTION upg_catalog.timetz_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_recv';
 CREATE FUNCTION upg_catalog.timetz_send(upg_catalog.timetz) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'timetz_send';
 CREATE FUNCTION upg_catalog.tintervalin(pg_catalog.cstring) RETURNS upg_catalog.tinterval LANGUAGE internal STABLE STRICT AS 'tintervalin';
 CREATE FUNCTION upg_catalog.tintervalout(upg_catalog.tinterval) RETURNS pg_catalog.cstring LANGUAGE internal STABLE STRICT AS 'tintervalout';
 CREATE FUNCTION upg_catalog.tintervalrecv(pg_catalog.internal) RETURNS upg_catalog.tinterval LANGUAGE internal IMMUTABLE STRICT AS 'tintervalrecv';
 CREATE FUNCTION upg_catalog.tintervalsend(upg_catalog.tinterval) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'tintervalsend';
 CREATE FUNCTION upg_catalog.trigger_in(pg_catalog.cstring) RETURNS upg_catalog.trigger LANGUAGE internal IMMUTABLE STRICT AS 'trigger_in';
 CREATE FUNCTION upg_catalog.trigger_out(upg_catalog.trigger) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'trigger_out';
 CREATE FUNCTION upg_catalog.unknownin(pg_catalog.cstring) RETURNS upg_catalog.unknown LANGUAGE internal IMMUTABLE STRICT AS 'unknownin';
 CREATE FUNCTION upg_catalog.unknownout(upg_catalog.unknown) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'unknownout';
 CREATE FUNCTION upg_catalog.unknownrecv(pg_catalog.internal) RETURNS upg_catalog.unknown LANGUAGE internal IMMUTABLE STRICT AS 'unknownrecv';
 CREATE FUNCTION upg_catalog.unknownsend(upg_catalog.unknown) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'unknownsend';
 CREATE FUNCTION upg_catalog.varbit_in(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.varbit LANGUAGE internal IMMUTABLE STRICT AS 'varbit_in';
 CREATE FUNCTION upg_catalog.varbit_out(upg_catalog.varbit) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'varbit_out';
 CREATE FUNCTION upg_catalog.varbit_recv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog.varbit LANGUAGE internal IMMUTABLE STRICT AS 'varbit_recv';
 CREATE FUNCTION upg_catalog.varbit_send(upg_catalog.varbit) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'varbit_send';
 CREATE FUNCTION upg_catalog.varcharin(pg_catalog.cstring, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."varchar" LANGUAGE internal IMMUTABLE STRICT AS 'varcharin';
 CREATE FUNCTION upg_catalog.varcharout(upg_catalog."varchar") RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'varcharout';
 CREATE FUNCTION upg_catalog.varcharrecv(pg_catalog.internal, pg_catalog.oid, pg_catalog.int4) RETURNS upg_catalog."varchar" LANGUAGE internal STABLE STRICT AS 'varcharrecv';
 CREATE FUNCTION upg_catalog.varcharsend(upg_catalog."varchar") RETURNS pg_catalog.bytea LANGUAGE internal STABLE STRICT AS 'varcharsend';
 CREATE FUNCTION upg_catalog.void_in(pg_catalog.cstring) RETURNS upg_catalog.void LANGUAGE internal IMMUTABLE STRICT AS 'void_in';
 CREATE FUNCTION upg_catalog.void_out(upg_catalog.void) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'void_out';
 CREATE FUNCTION upg_catalog.xidin(pg_catalog.cstring) RETURNS upg_catalog.xid LANGUAGE internal IMMUTABLE STRICT AS 'xidin';
 CREATE FUNCTION upg_catalog.xidout(upg_catalog.xid) RETURNS pg_catalog.cstring LANGUAGE internal IMMUTABLE STRICT AS 'xidout';
 CREATE FUNCTION upg_catalog.xidrecv(pg_catalog.internal) RETURNS upg_catalog.xid LANGUAGE internal IMMUTABLE STRICT AS 'xidrecv';
 CREATE FUNCTION upg_catalog.xidsend(upg_catalog.xid) RETURNS pg_catalog.bytea LANGUAGE internal IMMUTABLE STRICT AS 'xidsend';
