-- OIDS 1 - 99 

 CREATE FUNCTION boolin(cstring) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'boolin' WITH (OID=1242, DESCRIPTION="I/O");

 CREATE FUNCTION boolout(bool) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'boolout' WITH (OID=1243, DESCRIPTION="I/O");

 CREATE FUNCTION byteain(cstring) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'byteain' WITH (OID=1244, DESCRIPTION="I/O");

 CREATE FUNCTION byteaout(bytea) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'byteaout' WITH (OID=31, DESCRIPTION="I/O");

 CREATE FUNCTION charin(cstring) RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'charin' WITH (OID=1245, DESCRIPTION="I/O");

 CREATE FUNCTION charout("char") RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'charout' WITH (OID=33, DESCRIPTION="I/O");

 CREATE FUNCTION namein(cstring) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'namein' WITH (OID=34, DESCRIPTION="I/O");

 CREATE FUNCTION nameout(name) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'nameout' WITH (OID=35, DESCRIPTION="I/O");

 CREATE FUNCTION int2in(cstring) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2in' WITH (OID=38, DESCRIPTION="I/O");

 CREATE FUNCTION int2out(int2) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'int2out' WITH (OID=39, DESCRIPTION="I/O");

 CREATE FUNCTION int2vectorin(cstring) RETURNS int2vector LANGUAGE internal IMMUTABLE STRICT AS 'int2vectorin' WITH (OID=40, DESCRIPTION="I/O");

 CREATE FUNCTION int2vectorout(int2vector) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'int2vectorout' WITH (OID=41, DESCRIPTION="I/O");

 CREATE FUNCTION int4in(cstring) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4in' WITH (OID=42, DESCRIPTION="I/O");

 CREATE FUNCTION int4out(int4) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'int4out' WITH (OID=43, DESCRIPTION="I/O");

 CREATE FUNCTION regprocin(cstring) RETURNS regproc LANGUAGE internal STABLE STRICT AS 'regprocin' WITH (OID=44, DESCRIPTION="I/O");

 CREATE FUNCTION regprocout(regproc) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'regprocout' WITH (OID=45, DESCRIPTION="I/O");

 CREATE FUNCTION textin(cstring) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'textin' WITH (OID=46, DESCRIPTION="I/O");

 CREATE FUNCTION textout(text) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'textout' WITH (OID=47, DESCRIPTION="I/O");

 CREATE FUNCTION tidin(cstring) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'tidin' WITH (OID=48, DESCRIPTION="I/O");

 CREATE FUNCTION tidout(tid) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'tidout' WITH (OID=49, DESCRIPTION="I/O");

 CREATE FUNCTION xidin(cstring) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'xidin' WITH (OID=50, DESCRIPTION="I/O");

 CREATE FUNCTION xidout(xid) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'xidout' WITH (OID=51, DESCRIPTION="I/O");

 CREATE FUNCTION cidin(cstring) RETURNS cid LANGUAGE internal IMMUTABLE STRICT AS 'cidin' WITH (OID=52, DESCRIPTION="I/O");

 CREATE FUNCTION cidout(cid) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'cidout' WITH (OID=53, DESCRIPTION="I/O");

 CREATE FUNCTION oidvectorin(cstring) RETURNS oidvector LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorin' WITH (OID=54, DESCRIPTION="I/O");

 CREATE FUNCTION oidvectorout(oidvector) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorout' WITH (OID=55, DESCRIPTION="I/O");

 CREATE FUNCTION boollt(bool, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'boollt' WITH (OID=56, DESCRIPTION="less-than");

 CREATE FUNCTION boolgt(bool, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'boolgt' WITH (OID=57, DESCRIPTION="greater-than");

 CREATE FUNCTION booleq(bool, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'booleq' WITH (OID=60, DESCRIPTION="equal");

 CREATE FUNCTION chareq("char", "char") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'chareq' WITH (OID=61, DESCRIPTION="equal");

 CREATE FUNCTION nameeq(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'nameeq' WITH (OID=62, DESCRIPTION="equal");

 CREATE FUNCTION int2eq(int2, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int2eq' WITH (OID=63, DESCRIPTION="equal");

 CREATE FUNCTION int2lt(int2, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int2lt' WITH (OID=64, DESCRIPTION="less-than");

 CREATE FUNCTION int4eq(int4, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int4eq' WITH (OID=65, DESCRIPTION="equal");

 CREATE FUNCTION int4lt(int4, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int4lt' WITH (OID=66, DESCRIPTION="less-than");

 CREATE FUNCTION texteq(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texteq' WITH (OID=67, DESCRIPTION="equal");

 CREATE FUNCTION xideq(xid, xid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'xideq' WITH (OID=68, DESCRIPTION="equal");

 CREATE FUNCTION cideq(cid, cid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'cideq' WITH (OID=69, DESCRIPTION="equal");

 CREATE FUNCTION charne("char", "char") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'charne' WITH (OID=70, DESCRIPTION="not equal");

 CREATE FUNCTION charlt("char", "char") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'charlt' WITH (OID=1246, DESCRIPTION="less-than");

 CREATE FUNCTION charle("char", "char") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'charle' WITH (OID=72, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION chargt("char", "char") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'chargt' WITH (OID=73, DESCRIPTION="greater-than");

 CREATE FUNCTION charge("char", "char") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'charge' WITH (OID=74, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int4("char") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'chartoi4' WITH (OID=77, DESCRIPTION="convert char to int4");

 CREATE FUNCTION "char"(int4) RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'i4tochar' WITH (OID=78, DESCRIPTION="convert int4 to char");

 CREATE FUNCTION nameregexeq(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'nameregexeq' WITH (OID=79, DESCRIPTION="matches regex., case-sensitive");

 CREATE FUNCTION nameregexne(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'nameregexne' WITH (OID=1252, DESCRIPTION="does not match regex., case-sensitive");

 CREATE FUNCTION textregexeq(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textregexeq' WITH (OID=1254, DESCRIPTION="matches regex., case-sensitive");

 CREATE FUNCTION textregexne(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textregexne' WITH (OID=1256, DESCRIPTION="does not match regex., case-sensitive");

 CREATE FUNCTION textlen(text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'textlen' WITH (OID=1257, DESCRIPTION="length");

 CREATE FUNCTION textcat(text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'textcat' WITH (OID=1258, DESCRIPTION="concatenate");

 CREATE FUNCTION boolne(bool, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'boolne' WITH (OID=84, DESCRIPTION="not equal");

 CREATE FUNCTION version() RETURNS text LANGUAGE internal STABLE STRICT AS 'pgsql_version' WITH (OID=89, DESCRIPTION="PostgreSQL version string");

-- OIDS 100 - 199

 CREATE FUNCTION eqsel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'eqsel' WITH (OID=101, DESCRIPTION="restriction selectivity of = and related operators");

 CREATE FUNCTION neqsel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'neqsel' WITH (OID=102, DESCRIPTION="restriction selectivity of <> and related operators");

 CREATE FUNCTION scalarltsel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'scalarltsel' WITH (OID=103, DESCRIPTION="restriction selectivity of < and related operators on scalar datatypes");

 CREATE FUNCTION scalargtsel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'scalargtsel' WITH (OID=104, DESCRIPTION="restriction selectivity of > and related operators on scalar datatypes");

 CREATE FUNCTION eqjoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'eqjoinsel' WITH (OID=105, DESCRIPTION="join selectivity of = and related operators");

 CREATE FUNCTION neqjoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'neqjoinsel' WITH (OID=106, DESCRIPTION="join selectivity of <> and related operators");

 CREATE FUNCTION scalarltjoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'scalarltjoinsel' WITH (OID=107, DESCRIPTION="join selectivity of < and related operators on scalar datatypes");

 CREATE FUNCTION scalargtjoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'scalargtjoinsel' WITH (OID=108, DESCRIPTION="join selectivity of > and related operators on scalar datatypes");

 CREATE FUNCTION unknownin(cstring) RETURNS unknown LANGUAGE internal IMMUTABLE STRICT AS 'unknownin' WITH (OID=109, DESCRIPTION="I/O");

 CREATE FUNCTION unknownout(unknown) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'unknownout' WITH (OID=110, DESCRIPTION="I/O");

 CREATE FUNCTION numeric_fac(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_fac' WITH (OID=111);

 CREATE FUNCTION text(int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'int4_text' WITH (OID=112, DESCRIPTION="convert int4 to text");

 CREATE FUNCTION text(int2) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'int2_text' WITH (OID=113, DESCRIPTION="convert int2 to text");

 CREATE FUNCTION text(oid) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'oid_text' WITH (OID=114, DESCRIPTION="convert oid to text");

 CREATE FUNCTION box_above_eq(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_above_eq' WITH (OID=115, DESCRIPTION="is above (allows touching)");

 CREATE FUNCTION box_below_eq(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_below_eq' WITH (OID=116, DESCRIPTION="is below (allows touching)");

 CREATE FUNCTION point_in(cstring) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'point_in' WITH (OID=117, DESCRIPTION="I/O");

 CREATE FUNCTION point_out(point) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'point_out' WITH (OID=118, DESCRIPTION="I/O");

 CREATE FUNCTION lseg_in(cstring) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lseg_in' WITH (OID=119, DESCRIPTION="I/O");

 CREATE FUNCTION lseg_out(lseg) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'lseg_out' WITH (OID=120, DESCRIPTION="I/O");

 CREATE FUNCTION path_in(cstring) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_in' WITH (OID=121, DESCRIPTION="I/O");

 CREATE FUNCTION path_out(path) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'path_out' WITH (OID=122, DESCRIPTION="I/O");

 CREATE FUNCTION box_in(cstring) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'box_in' WITH (OID=123, DESCRIPTION="I/O");

 CREATE FUNCTION box_out(box) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'box_out' WITH (OID=124, DESCRIPTION="I/O");

 CREATE FUNCTION box_overlap(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overlap' WITH (OID=125, DESCRIPTION="overlaps");

 CREATE FUNCTION box_ge(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_ge' WITH (OID=126, DESCRIPTION="greater-than-or-equal by area");

 CREATE FUNCTION box_gt(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_gt' WITH (OID=127, DESCRIPTION="greater-than by area");

 CREATE FUNCTION box_eq(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_eq' WITH (OID=128, DESCRIPTION="equal by area");

 CREATE FUNCTION box_lt(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_lt' WITH (OID=129, DESCRIPTION="less-than by area");

 CREATE FUNCTION box_le(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_le' WITH (OID=130, DESCRIPTION="less-than-or-equal by area");

 CREATE FUNCTION point_above(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_above' WITH (OID=131, DESCRIPTION="is above");

 CREATE FUNCTION point_left(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_left' WITH (OID=132, DESCRIPTION="is left of");

 CREATE FUNCTION point_right(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_right' WITH (OID=133, DESCRIPTION="is right of");

 CREATE FUNCTION point_below(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_below' WITH (OID=134, DESCRIPTION="is below");

 CREATE FUNCTION point_eq(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_eq' WITH (OID=135, DESCRIPTION="same as?");

 CREATE FUNCTION on_pb(point, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'on_pb' WITH (OID=136, DESCRIPTION="point inside box?");

 CREATE FUNCTION on_ppath(point, path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'on_ppath' WITH (OID=137, DESCRIPTION="point within closed path, or point on open path");

 CREATE FUNCTION box_center(box) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'box_center' WITH (OID=138, DESCRIPTION="center of");

 CREATE FUNCTION areasel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'areasel' WITH (OID=139, DESCRIPTION="restriction selectivity for area-comparison operators");

 CREATE FUNCTION areajoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'areajoinsel' WITH (OID=140, DESCRIPTION="join selectivity for area-comparison operators");

 CREATE FUNCTION int4mul(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4mul' WITH (OID=141, DESCRIPTION="multiply");

 CREATE FUNCTION int4ne(int4, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int4ne' WITH (OID=144, DESCRIPTION="not equal");

 CREATE FUNCTION int2ne(int2, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int2ne' WITH (OID=145, DESCRIPTION="not equal");

 CREATE FUNCTION int2gt(int2, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int2gt' WITH (OID=146, DESCRIPTION="greater-than");

 CREATE FUNCTION int4gt(int4, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int4gt' WITH (OID=147, DESCRIPTION="greater-than");

 CREATE FUNCTION int2le(int2, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int2le' WITH (OID=148, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int4le(int4, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int4le' WITH (OID=149, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int4ge(int4, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int4ge' WITH (OID=150, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int2ge(int2, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int2ge' WITH (OID=151, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int2mul(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2mul' WITH (OID=152, DESCRIPTION="multiply");

 CREATE FUNCTION int2div(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2div' WITH (OID=153, DESCRIPTION="divide");

 CREATE FUNCTION int4div(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4div' WITH (OID=154, DESCRIPTION="divide");

 CREATE FUNCTION int2mod(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2mod' WITH (OID=155, DESCRIPTION="modulus");

 CREATE FUNCTION int4mod(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4mod' WITH (OID=156, DESCRIPTION="modulus");

 CREATE FUNCTION textne(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textne' WITH (OID=157, DESCRIPTION="not equal");

 CREATE FUNCTION int24eq(int2, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int24eq' WITH (OID=158, DESCRIPTION="equal");

 CREATE FUNCTION int42eq(int4, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int42eq' WITH (OID=159, DESCRIPTION="equal");

 CREATE FUNCTION int24lt(int2, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int24lt' WITH (OID=160, DESCRIPTION="less-than");

 CREATE FUNCTION int42lt(int4, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int42lt' WITH (OID=161, DESCRIPTION="less-than");

 CREATE FUNCTION int24gt(int2, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int24gt' WITH (OID=162, DESCRIPTION="greater-than");

 CREATE FUNCTION int42gt(int4, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int42gt' WITH (OID=163, DESCRIPTION="greater-than");

 CREATE FUNCTION int24ne(int2, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int24ne' WITH (OID=164, DESCRIPTION="not equal");

 CREATE FUNCTION int42ne(int4, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int42ne' WITH (OID=165, DESCRIPTION="not equal");

 CREATE FUNCTION int24le(int2, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int24le' WITH (OID=166, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int42le(int4, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int42le' WITH (OID=167, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int24ge(int2, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int24ge' WITH (OID=168, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int42ge(int4, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int42ge' WITH (OID=169, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int24mul(int2, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int24mul' WITH (OID=170, DESCRIPTION="multiply");

 CREATE FUNCTION int42mul(int4, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int42mul' WITH (OID=171, DESCRIPTION="multiply");

 CREATE FUNCTION int24div(int2, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int24div' WITH (OID=172, DESCRIPTION="divide");

 CREATE FUNCTION int42div(int4, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int42div' WITH (OID=173, DESCRIPTION="divide");

 CREATE FUNCTION int24mod(int2, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int24mod' WITH (OID=174, DESCRIPTION="modulus");

 CREATE FUNCTION int42mod(int4, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int42mod' WITH (OID=175, DESCRIPTION="modulus");

 CREATE FUNCTION int2pl(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2pl' WITH (OID=176, DESCRIPTION="add");

 CREATE FUNCTION int4pl(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4pl' WITH (OID=177, DESCRIPTION="add");

 CREATE FUNCTION int24pl(int2, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int24pl' WITH (OID=178, DESCRIPTION="add");

 CREATE FUNCTION int42pl(int4, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int42pl' WITH (OID=179, DESCRIPTION="add");

 CREATE FUNCTION int2mi(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2mi' WITH (OID=180, DESCRIPTION="subtract");

 CREATE FUNCTION int4mi(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4mi' WITH (OID=181, DESCRIPTION="subtract");

 CREATE FUNCTION int24mi(int2, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int24mi' WITH (OID=182, DESCRIPTION="subtract");

 CREATE FUNCTION int42mi(int4, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int42mi' WITH (OID=183, DESCRIPTION="subtract");

 CREATE FUNCTION oideq(oid, oid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oideq' WITH (OID=184, DESCRIPTION="equal");

 CREATE FUNCTION oidne(oid, oid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidne' WITH (OID=185, DESCRIPTION="not equal");

 CREATE FUNCTION box_same(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_same' WITH (OID=186, DESCRIPTION="same as?");

 CREATE FUNCTION box_contain(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_contain' WITH (OID=187, DESCRIPTION="contains?");

 CREATE FUNCTION box_left(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_left' WITH (OID=188, DESCRIPTION="is left of");

 CREATE FUNCTION box_overleft(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overleft' WITH (OID=189, DESCRIPTION="overlaps or is left of");

 CREATE FUNCTION box_overright(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overright' WITH (OID=190, DESCRIPTION="overlaps or is right of");

 CREATE FUNCTION box_right(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_right' WITH (OID=191, DESCRIPTION="is right of");

 CREATE FUNCTION box_contained(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_contained' WITH (OID=192, DESCRIPTION="is contained by?");

-- OIDS 200 - 299

 CREATE FUNCTION float4in(cstring) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4in' WITH (OID=200, DESCRIPTION="I/O");

 CREATE FUNCTION float4out(float4) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'float4out' WITH (OID=201, DESCRIPTION="I/O");

 CREATE FUNCTION float4mul(float4, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4mul' WITH (OID=202, DESCRIPTION="multiply");

 CREATE FUNCTION float4div(float4, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4div' WITH (OID=203, DESCRIPTION="divide");

 CREATE FUNCTION float4pl(float4, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4pl' WITH (OID=204, DESCRIPTION="add");

 CREATE FUNCTION float4mi(float4, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4mi' WITH (OID=205, DESCRIPTION="subtract");

 CREATE FUNCTION float4um(float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4um' WITH (OID=206, DESCRIPTION="negate");

 CREATE FUNCTION float4abs(float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4abs' WITH (OID=207, DESCRIPTION="absolute value");

 CREATE FUNCTION float4_accum(_float8, float4) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float4_accum' WITH (OID=208, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION float4_decum(_float8, float4) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float4_decum' WITH (OID=6024, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION float4_avg_accum(bytea, float4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'float4_avg_accum' WITH (OID=3106, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION float4_avg_decum(bytea, float4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'float4_avg_decum' WITH (OID=3107, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION float4larger(float4, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4larger' WITH (OID=209, DESCRIPTION="larger of two");

 CREATE FUNCTION float4smaller(float4, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4smaller' WITH (OID=211, DESCRIPTION="smaller of two");

 CREATE FUNCTION int4um(int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4um' WITH (OID=212, DESCRIPTION="negate");

 CREATE FUNCTION int2um(int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2um' WITH (OID=213, DESCRIPTION="negate");

 CREATE FUNCTION float8in(cstring) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8in' WITH (OID=214, DESCRIPTION="I/O");

 CREATE FUNCTION float8out(float8) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'float8out' WITH (OID=215, DESCRIPTION="I/O");

 CREATE FUNCTION float8mul(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8mul' WITH (OID=216, DESCRIPTION="multiply");

 CREATE FUNCTION float8div(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8div' WITH (OID=217, DESCRIPTION="divide");

 CREATE FUNCTION float8pl(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8pl' WITH (OID=218, DESCRIPTION="add");

 CREATE FUNCTION float8mi(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8mi' WITH (OID=219, DESCRIPTION="subtract");

 CREATE FUNCTION float8um(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8um' WITH (OID=220, DESCRIPTION="negate");

 CREATE FUNCTION float8abs(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8abs' WITH (OID=221, DESCRIPTION="absolute value");

 CREATE FUNCTION float8_accum(_float8, float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_accum' WITH (OID=222, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION float8_decum(_float8, float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_decum' WITH (OID=6025, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION float8_avg_accum(bytea, float8) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'float8_avg_accum' WITH (OID=3108, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION float8_avg_decum(bytea, float8) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'float8_avg_decum' WITH (OID=3109, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION float8larger(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8larger' WITH (OID=223, DESCRIPTION="larger of two");

 CREATE FUNCTION float8smaller(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8smaller' WITH (OID=224, DESCRIPTION="smaller of two");

 CREATE FUNCTION lseg_center(lseg) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lseg_center' WITH (OID=225, DESCRIPTION="center of");

 CREATE FUNCTION path_center(path) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'path_center' WITH (OID=226, DESCRIPTION="center of");

 CREATE FUNCTION poly_center(polygon) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'poly_center' WITH (OID=227, DESCRIPTION="center of");

 CREATE FUNCTION dround(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dround' WITH (OID=228, DESCRIPTION="round to nearest integer");

 CREATE FUNCTION dtrunc(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dtrunc' WITH (OID=229, DESCRIPTION="truncate to integer");

 CREATE FUNCTION ceil(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dceil' WITH (OID=2308, DESCRIPTION="smallest integer >= value");

 CREATE FUNCTION ceiling(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dceil' WITH (OID=2320, DESCRIPTION="smallest integer >= value");

 CREATE FUNCTION floor(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dfloor' WITH (OID=2309, DESCRIPTION="largest integer <= value");

 CREATE FUNCTION sign(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dsign' WITH (OID=2310, DESCRIPTION="sign of value");

 CREATE FUNCTION dsqrt(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dsqrt' WITH (OID=230, DESCRIPTION="square root");

 CREATE FUNCTION dcbrt(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dcbrt' WITH (OID=231, DESCRIPTION="cube root");

 CREATE FUNCTION dpow(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dpow' WITH (OID=232, DESCRIPTION="exponentiation (x^y)");

 CREATE FUNCTION dexp(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dexp' WITH (OID=233, DESCRIPTION="natural exponential (e^x)");

 CREATE FUNCTION dlog1(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dlog1' WITH (OID=234, DESCRIPTION="natural logarithm");

 CREATE FUNCTION float8(int2) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'i2tod' WITH (OID=235, DESCRIPTION="convert int2 to float8");

 CREATE FUNCTION float4(int2) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'i2tof' WITH (OID=236, DESCRIPTION="convert int2 to float4");

 CREATE FUNCTION int2(float8) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'dtoi2' WITH (OID=237, DESCRIPTION="convert float8 to int2");

 CREATE FUNCTION int2(float4) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'ftoi2' WITH (OID=238, DESCRIPTION="convert float4 to int2");

 CREATE FUNCTION line_distance(line, line) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'line_distance' WITH (OID=239, DESCRIPTION="distance between");

 CREATE FUNCTION abstimein(cstring) RETURNS abstime LANGUAGE internal STABLE STRICT AS 'abstimein' WITH (OID=240, DESCRIPTION="I/O");

 CREATE FUNCTION abstimeout(abstime) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'abstimeout' WITH (OID=241, DESCRIPTION="I/O");

 CREATE FUNCTION reltimein(cstring) RETURNS reltime LANGUAGE internal STABLE STRICT AS 'reltimein' WITH (OID=242, DESCRIPTION="I/O");

 CREATE FUNCTION reltimeout(reltime) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'reltimeout' WITH (OID=243, DESCRIPTION="I/O");

 CREATE FUNCTION timepl(abstime, reltime) RETURNS abstime LANGUAGE internal IMMUTABLE STRICT AS 'timepl' WITH (OID=244, DESCRIPTION="add");

 CREATE FUNCTION timemi(abstime, reltime) RETURNS abstime LANGUAGE internal IMMUTABLE STRICT AS 'timemi' WITH (OID=245, DESCRIPTION="subtract");

 CREATE FUNCTION tintervalin(cstring) RETURNS tinterval LANGUAGE internal STABLE STRICT AS 'tintervalin' WITH (OID=246, DESCRIPTION="I/O");

 CREATE FUNCTION tintervalout(tinterval) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'tintervalout' WITH (OID=247, DESCRIPTION="I/O");

 CREATE FUNCTION intinterval(abstime, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'intinterval' WITH (OID=248, DESCRIPTION="abstime in tinterval");

 CREATE FUNCTION tintervalrel(tinterval) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'tintervalrel' WITH (OID=249, DESCRIPTION="tinterval to reltime");

 CREATE FUNCTION timenow() RETURNS abstime LANGUAGE internal STABLE STRICT AS 'timenow' WITH (OID=250, DESCRIPTION="Current date and time (abstime)");

 CREATE FUNCTION abstimeeq(abstime, abstime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimeeq' WITH (OID=251, DESCRIPTION="equal");

 CREATE FUNCTION abstimene(abstime, abstime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimene' WITH (OID=252, DESCRIPTION="not equal");

 CREATE FUNCTION abstimelt(abstime, abstime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimelt' WITH (OID=253, DESCRIPTION="less-than");

 CREATE FUNCTION abstimegt(abstime, abstime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimegt' WITH (OID=254, DESCRIPTION="greater-than");

 CREATE FUNCTION abstimele(abstime, abstime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimele' WITH (OID=255, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION abstimege(abstime, abstime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'abstimege' WITH (OID=256, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION reltimeeq(reltime, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimeeq' WITH (OID=257, DESCRIPTION="equal");

 CREATE FUNCTION reltimene(reltime, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimene' WITH (OID=258, DESCRIPTION="not equal");

 CREATE FUNCTION reltimelt(reltime, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimelt' WITH (OID=259, DESCRIPTION="less-than");

 CREATE FUNCTION reltimegt(reltime, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimegt' WITH (OID=260, DESCRIPTION="greater-than");

 CREATE FUNCTION reltimele(reltime, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimele' WITH (OID=261, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION reltimege(reltime, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'reltimege' WITH (OID=262, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION tintervalsame(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalsame' WITH (OID=263, DESCRIPTION="same as?");

 CREATE FUNCTION tintervalct(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalct' WITH (OID=264, DESCRIPTION="contains?");

 CREATE FUNCTION tintervalov(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalov' WITH (OID=265, DESCRIPTION="overlaps");

 CREATE FUNCTION tintervalleneq(tinterval, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalleneq' WITH (OID=266, DESCRIPTION="length equal");

 CREATE FUNCTION tintervallenne(tinterval, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervallenne' WITH (OID=267, DESCRIPTION="length not equal to");

 CREATE FUNCTION tintervallenlt(tinterval, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervallenlt' WITH (OID=268, DESCRIPTION="length less-than");

 CREATE FUNCTION tintervallengt(tinterval, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervallengt' WITH (OID=269, DESCRIPTION="length greater-than");

 CREATE FUNCTION tintervallenle(tinterval, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervallenle' WITH (OID=270, DESCRIPTION="length less-than-or-equal");

 CREATE FUNCTION tintervallenge(tinterval, reltime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervallenge' WITH (OID=271, DESCRIPTION="length greater-than-or-equal");

 CREATE FUNCTION tintervalstart(tinterval) RETURNS abstime LANGUAGE internal IMMUTABLE STRICT AS 'tintervalstart' WITH (OID=272, DESCRIPTION="start of interval");

 CREATE FUNCTION tintervalend(tinterval) RETURNS abstime LANGUAGE internal IMMUTABLE STRICT AS 'tintervalend' WITH (OID=273, DESCRIPTION="end of interval");

 CREATE FUNCTION timeofday() RETURNS text LANGUAGE internal VOLATILE STRICT AS 'timeofday' WITH (OID=274, DESCRIPTION="Current date and time - increments during transactions");

 CREATE FUNCTION isfinite(abstime) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'abstime_finite' WITH (OID=275, DESCRIPTION="finite abstime?");

 CREATE FUNCTION inter_sl(lseg, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'inter_sl' WITH (OID=277, DESCRIPTION="intersect?");

 CREATE FUNCTION inter_lb(line, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'inter_lb' WITH (OID=278, DESCRIPTION="intersect?");

 CREATE FUNCTION float48mul(float4, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float48mul' WITH (OID=279, DESCRIPTION="multiply");

 CREATE FUNCTION float48div(float4, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float48div' WITH (OID=280, DESCRIPTION="divide");

 CREATE FUNCTION float48pl(float4, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float48pl' WITH (OID=281, DESCRIPTION="add");

 CREATE FUNCTION float48mi(float4, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float48mi' WITH (OID=282, DESCRIPTION="subtract");

 CREATE FUNCTION float84mul(float8, float4) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float84mul' WITH (OID=283, DESCRIPTION="multiply");

 CREATE FUNCTION float84div(float8, float4) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float84div' WITH (OID=284, DESCRIPTION="divide");

 CREATE FUNCTION float84pl(float8, float4) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float84pl' WITH (OID=285, DESCRIPTION="add");

 CREATE FUNCTION float84mi(float8, float4) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float84mi' WITH (OID=286, DESCRIPTION="subtract");

 CREATE FUNCTION float4eq(float4, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float4eq' WITH (OID=287, DESCRIPTION="equal");

 CREATE FUNCTION float4ne(float4, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float4ne' WITH (OID=288, DESCRIPTION="not equal");

 CREATE FUNCTION float4lt(float4, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float4lt' WITH (OID=289, DESCRIPTION="less-than");

 CREATE FUNCTION float4le(float4, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float4le' WITH (OID=290, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION float4gt(float4, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float4gt' WITH (OID=291, DESCRIPTION="greater-than");

 CREATE FUNCTION float4ge(float4, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float4ge' WITH (OID=292, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION float8eq(float8, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float8eq' WITH (OID=293, DESCRIPTION="equal");

 CREATE FUNCTION float8ne(float8, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float8ne' WITH (OID=294, DESCRIPTION="not equal");

 CREATE FUNCTION float8lt(float8, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float8lt' WITH (OID=295, DESCRIPTION="less-than");

 CREATE FUNCTION float8le(float8, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float8le' WITH (OID=296, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION float8gt(float8, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float8gt' WITH (OID=297, DESCRIPTION="greater-than");

 CREATE FUNCTION float8ge(float8, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float8ge' WITH (OID=298, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION float48eq(float4, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float48eq' WITH (OID=299, DESCRIPTION="equal");

-- OIDS 300 - 399

 CREATE FUNCTION float48ne(float4, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float48ne' WITH (OID=300, DESCRIPTION="not equal");

 CREATE FUNCTION float48lt(float4, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float48lt' WITH (OID=301, DESCRIPTION="less-than");

 CREATE FUNCTION float48le(float4, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float48le' WITH (OID=302, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION float48gt(float4, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float48gt' WITH (OID=303, DESCRIPTION="greater-than");

 CREATE FUNCTION float48ge(float4, float8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float48ge' WITH (OID=304, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION float84eq(float8, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float84eq' WITH (OID=305, DESCRIPTION="equal");

 CREATE FUNCTION float84ne(float8, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float84ne' WITH (OID=306, DESCRIPTION="not equal");

 CREATE FUNCTION float84lt(float8, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float84lt' WITH (OID=307, DESCRIPTION="less-than");

 CREATE FUNCTION float84le(float8, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float84le' WITH (OID=308, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION float84gt(float8, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float84gt' WITH (OID=309, DESCRIPTION="greater-than");

 CREATE FUNCTION float84ge(float8, float4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'float84ge' WITH (OID=310, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION float8(float4) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'ftod' WITH (OID=311, DESCRIPTION="convert float4 to float8");

 CREATE FUNCTION float4(float8) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'dtof' WITH (OID=312, DESCRIPTION="convert float8 to float4");

 CREATE FUNCTION int4(int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'i2toi4' WITH (OID=313, DESCRIPTION="convert int2 to int4");

 CREATE FUNCTION int2(int4) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'i4toi2' WITH (OID=314, DESCRIPTION="convert int4 to int2");

 CREATE FUNCTION int2vectoreq(int2vector, int2vector) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int2vectoreq' WITH (OID=315, DESCRIPTION="equal");

 CREATE FUNCTION float8(int4) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'i4tod' WITH (OID=316, DESCRIPTION="convert int4 to float8");

 CREATE FUNCTION int4(float8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'dtoi4' WITH (OID=317, DESCRIPTION="convert float8 to int4");

 CREATE FUNCTION float4(int4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'i4tof' WITH (OID=318, DESCRIPTION="convert int4 to float4");

 CREATE FUNCTION int4(float4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'ftoi4' WITH (OID=319, DESCRIPTION="convert float4 to int4");

 CREATE FUNCTION btgettuple(internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'btgettuple' WITH (OID=330, DESCRIPTION="btree(internal)");
-- #define BTGETTUPLE_OID 330

 CREATE FUNCTION btgetmulti(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'btgetmulti' WITH (OID=636, DESCRIPTION="btree(internal)");
-- #define BTGETMULTI_OID 636

 CREATE FUNCTION btinsert(internal, internal, internal, internal, internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'btinsert' WITH (OID=331, DESCRIPTION="btree(internal)");
-- #define BTINSERT_OID 331

 CREATE FUNCTION btbeginscan(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'btbeginscan' WITH (OID=333, DESCRIPTION="btree(internal)");
-- #define BTBEGINSCAN_OID 333

 CREATE FUNCTION btrescan(internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'btrescan' WITH (OID=334, DESCRIPTION="btree(internal)");
-- #define BTRESCAN_OID 334

 CREATE FUNCTION btendscan(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'btendscan' WITH (OID=335, DESCRIPTION="btree(internal)");
-- #define BTENDSCAN_OID 335

 CREATE FUNCTION btmarkpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'btmarkpos' WITH (OID=336, DESCRIPTION="btree(internal)");
-- #define BTMARKPOS_OID 336

 CREATE FUNCTION btrestrpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'btrestrpos' WITH (OID=337, DESCRIPTION="btree(internal)");
-- #define BTRESTRPOS_OID 337

 CREATE FUNCTION btbuild(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'btbuild' WITH (OID=338, DESCRIPTION="btree(internal)");
-- #define BTBUILD_OID 338

 CREATE FUNCTION btbulkdelete(internal, internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'btbulkdelete' WITH (OID=332, DESCRIPTION="btree(internal)");
-- #define BTBULKDELETE_OID 332

 CREATE FUNCTION btvacuumcleanup(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'btvacuumcleanup' WITH (OID=972, DESCRIPTION="btree(internal)");
-- #define BTVACUUMCLEANUP_OID 972

 CREATE FUNCTION btcostestimate(internal, internal, internal, internal, internal, internal, internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'btcostestimate' WITH (OID=1268, DESCRIPTION="btree(internal)");
-- #define BTCOSTESTIMATE_OID 1268

 CREATE FUNCTION btoptions(_text, bool) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'btoptions' WITH (OID=2785, DESCRIPTION="btree(internal)");
-- #define BTOPTIONS_OID 2785

 CREATE FUNCTION poly_same(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_same' WITH (OID=339, DESCRIPTION="same as?");

 CREATE FUNCTION poly_contain(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_contain' WITH (OID=340, DESCRIPTION="contains?");

 CREATE FUNCTION poly_left(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_left' WITH (OID=341, DESCRIPTION="is left of");

 CREATE FUNCTION poly_overleft(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overleft' WITH (OID=342, DESCRIPTION="overlaps or is left of");

 CREATE FUNCTION poly_overright(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overright' WITH (OID=343, DESCRIPTION="overlaps or is right of");

 CREATE FUNCTION poly_right(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_right' WITH (OID=344, DESCRIPTION="is right of");

 CREATE FUNCTION poly_contained(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_contained' WITH (OID=345, DESCRIPTION="is contained by?");

 CREATE FUNCTION poly_overlap(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overlap' WITH (OID=346, DESCRIPTION="overlaps");

 CREATE FUNCTION poly_in(cstring) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'poly_in' WITH (OID=347, DESCRIPTION="I/O");

 CREATE FUNCTION poly_out(polygon) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'poly_out' WITH (OID=348, DESCRIPTION="I/O");

 CREATE FUNCTION btint2cmp(int2, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint2cmp' WITH (OID=350, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btint4cmp(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint4cmp' WITH (OID=351, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btint8cmp(int8, int8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint8cmp' WITH (OID=842, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btfloat4cmp(float4, float4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btfloat4cmp' WITH (OID=354, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btfloat8cmp(float8, float8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btfloat8cmp' WITH (OID=355, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btoidcmp(oid, oid) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btoidcmp' WITH (OID=356, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btoidvectorcmp(oidvector, oidvector) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btoidvectorcmp' WITH (OID=404, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btabstimecmp(abstime, abstime) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btabstimecmp' WITH (OID=357, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btcharcmp("char", "char") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btcharcmp' WITH (OID=358, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btnamecmp(name, name) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btnamecmp' WITH (OID=359, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION bttextcmp(text, text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttextcmp' WITH (OID=360, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION cash_cmp(money, money) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'cash_cmp' WITH (OID=377, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btreltimecmp(reltime, reltime) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btreltimecmp' WITH (OID=380, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION bttintervalcmp(tinterval, tinterval) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttintervalcmp' WITH (OID=381, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btarraycmp(anyarray, anyarray) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btarraycmp' WITH (OID=382, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION btgpxlogloccmp(gpxlogloc, gpxlogloc) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btgpxlogloccmp' WITH (OID=2905, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION lseg_distance(lseg, lseg) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lseg_distance' WITH (OID=361, DESCRIPTION="distance between");

 CREATE FUNCTION lseg_interpt(lseg, lseg) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lseg_interpt' WITH (OID=362, DESCRIPTION="intersection point");

 CREATE FUNCTION dist_ps(point, lseg) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_ps' WITH (OID=363, DESCRIPTION="distance between");

 CREATE FUNCTION dist_pb(point, box) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_pb' WITH (OID=364, DESCRIPTION="distance between point and box");

 CREATE FUNCTION dist_sb(lseg, box) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_sb' WITH (OID=365, DESCRIPTION="distance between segment and box");

 CREATE FUNCTION close_ps(point, lseg) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'close_ps' WITH (OID=366, DESCRIPTION="closest point on line segment");

 CREATE FUNCTION close_pb(point, box) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'close_pb' WITH (OID=367, DESCRIPTION="closest point on box");

 CREATE FUNCTION close_sb(lseg, box) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'close_sb' WITH (OID=368, DESCRIPTION="closest point to line segment on box");

 CREATE FUNCTION on_ps(point, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'on_ps' WITH (OID=369, DESCRIPTION="point contained in segment?");

 CREATE FUNCTION path_distance(path, path) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'path_distance' WITH (OID=370, DESCRIPTION="distance between paths");

 CREATE FUNCTION dist_ppath(point, path) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_ppath' WITH (OID=371, DESCRIPTION="distance between point and path");

 CREATE FUNCTION on_sb(lseg, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'on_sb' WITH (OID=372, DESCRIPTION="lseg contained in box?");

 CREATE FUNCTION inter_sb(lseg, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'inter_sb' WITH (OID=373, DESCRIPTION="intersect?");

-- OIDS 400 - 499

 CREATE FUNCTION text(bpchar) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'rtrim1' WITH (OID=401, DESCRIPTION="convert char(n) to text");

 CREATE FUNCTION text(name) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'name_text' WITH (OID=406, DESCRIPTION="convert name to text");

 CREATE FUNCTION name(text) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'text_name' WITH (OID=407, DESCRIPTION="convert text to name");

 CREATE FUNCTION bpchar(name) RETURNS bpchar LANGUAGE internal IMMUTABLE STRICT AS 'name_bpchar' WITH (OID=408, DESCRIPTION="convert name to char(n)");

 CREATE FUNCTION name(bpchar) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'bpchar_name' WITH (OID=409, DESCRIPTION="convert char(n) to name");

 CREATE FUNCTION hashgettuple(internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'hashgettuple' WITH (OID=440, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashgetmulti(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'hashgetmulti' WITH (OID=637, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashinsert(internal, internal, internal, internal, internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'hashinsert' WITH (OID=441, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashbeginscan(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'hashbeginscan' WITH (OID=443, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashrescan(internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'hashrescan' WITH (OID=444, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashendscan(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'hashendscan' WITH (OID=445, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashmarkpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'hashmarkpos' WITH (OID=446, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashrestrpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'hashrestrpos' WITH (OID=447, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashbuild(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'hashbuild' WITH (OID=448, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashbulkdelete(internal, internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'hashbulkdelete' WITH (OID=442, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashvacuumcleanup(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'hashvacuumcleanup' WITH (OID=425, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashcostestimate(internal, internal, internal, internal, internal, internal, internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'hashcostestimate' WITH (OID=438, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashoptions(_text, bool) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'hashoptions' WITH (OID=2786, DESCRIPTION="hash(internal)");

 CREATE FUNCTION hashint2(int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashint2' WITH (OID=449, DESCRIPTION="hash");

 CREATE FUNCTION hashint4(int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashint4' WITH (OID=450, DESCRIPTION="hash");

 CREATE FUNCTION hashint8(int8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashint8' WITH (OID=949, DESCRIPTION="hash");

 CREATE FUNCTION hashfloat4(float4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashfloat4' WITH (OID=451, DESCRIPTION="hash");

 CREATE FUNCTION hashfloat8(float8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashfloat8' WITH (OID=452, DESCRIPTION="hash");

 CREATE FUNCTION hashoid(oid) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashoid' WITH (OID=453, DESCRIPTION="hash");

 CREATE FUNCTION hashchar("char") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashchar' WITH (OID=454, DESCRIPTION="hash");

 CREATE FUNCTION hashname(name) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashname' WITH (OID=455, DESCRIPTION="hash");

 CREATE FUNCTION hashtext(text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashtext' WITH (OID=400, DESCRIPTION="hash");

 CREATE FUNCTION hashvarlena(internal) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashvarlena' WITH (OID=456, DESCRIPTION="hash any varlena type");

 CREATE FUNCTION hashoidvector(oidvector) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashoidvector' WITH (OID=457, DESCRIPTION="hash");

 CREATE FUNCTION hash_aclitem(aclitem) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hash_aclitem' WITH (OID=329, DESCRIPTION="hash");

 CREATE FUNCTION hashint2vector(int2vector) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashint2vector' WITH (OID=398, DESCRIPTION="hash");

 CREATE FUNCTION hashmacaddr(macaddr) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashmacaddr' WITH (OID=399, DESCRIPTION="hash");

 CREATE FUNCTION hashinet(inet) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashinet' WITH (OID=422, DESCRIPTION="hash");

 CREATE FUNCTION hash_numeric("numeric") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hash_numeric' WITH (OID=6432, DESCRIPTION="hash");

 CREATE FUNCTION text_larger(text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'text_larger' WITH (OID=458, DESCRIPTION="larger of two");

 CREATE FUNCTION text_smaller(text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'text_smaller' WITH (OID=459, DESCRIPTION="smaller of two");

 CREATE FUNCTION int8in(cstring) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8in' WITH (OID=460, DESCRIPTION="I/O");

 CREATE FUNCTION int8out(int8) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'int8out' WITH (OID=461, DESCRIPTION="I/O");

 CREATE FUNCTION int8um(int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8um' WITH (OID=462, DESCRIPTION="negate");

 CREATE FUNCTION int8pl(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8pl' WITH (OID=463, DESCRIPTION="add");

 CREATE FUNCTION int8mi(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8mi' WITH (OID=464, DESCRIPTION="subtract");

 CREATE FUNCTION int8mul(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8mul' WITH (OID=465, DESCRIPTION="multiply");

 CREATE FUNCTION int8div(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8div' WITH (OID=466, DESCRIPTION="divide");

 CREATE FUNCTION int8eq(int8, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int8eq' WITH (OID=467, DESCRIPTION="equal");

 CREATE FUNCTION int8ne(int8, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int8ne' WITH (OID=468, DESCRIPTION="not equal");

 CREATE FUNCTION int8lt(int8, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int8lt' WITH (OID=469, DESCRIPTION="less-than");

 CREATE FUNCTION int8gt(int8, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int8gt' WITH (OID=470, DESCRIPTION="greater-than");

 CREATE FUNCTION int8le(int8, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int8le' WITH (OID=471, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int8ge(int8, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int8ge' WITH (OID=472, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int84eq(int8, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int84eq' WITH (OID=474, DESCRIPTION="equal");

 CREATE FUNCTION int84ne(int8, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int84ne' WITH (OID=475, DESCRIPTION="not equal");

 CREATE FUNCTION int84lt(int8, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int84lt' WITH (OID=476, DESCRIPTION="less-than");

 CREATE FUNCTION int84gt(int8, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int84gt' WITH (OID=477, DESCRIPTION="greater-than");

 CREATE FUNCTION int84le(int8, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int84le' WITH (OID=478, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int84ge(int8, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int84ge' WITH (OID=479, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int4(int8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int84' WITH (OID=480, DESCRIPTION="convert int8 to int4");

 CREATE FUNCTION int8(int4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int48' WITH (OID=481, DESCRIPTION="convert int4 to int8");

 CREATE FUNCTION float8(int8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'i8tod' WITH (OID=482, DESCRIPTION="convert int8 to float8");

 CREATE FUNCTION int8(float8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'dtoi8' WITH (OID=483, DESCRIPTION="convert float8 to int8");

-- OIDS 500 - 599

-- OIDS 600 - 699

 CREATE FUNCTION float4(int8) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'i8tof' WITH (OID=652, DESCRIPTION="convert int8 to float4");

 CREATE FUNCTION int8(float4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'ftoi8' WITH (OID=653, DESCRIPTION="convert float4 to int8");

 CREATE FUNCTION int2(int8) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int82' WITH (OID=714, DESCRIPTION="convert int8 to int2");

 CREATE FUNCTION int8(int2) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int28' WITH (OID=754, DESCRIPTION="convert int2 to int8");

 CREATE FUNCTION int4notin(int4, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'int4notin' WITH (OID=1285, DESCRIPTION="not in");

 CREATE FUNCTION oidnotin(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'oidnotin' WITH (OID=1286, DESCRIPTION="not in");

 CREATE FUNCTION namelt(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namelt' WITH (OID=655, DESCRIPTION="less-than");

 CREATE FUNCTION namele(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namele' WITH (OID=656, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION namegt(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namegt' WITH (OID=657, DESCRIPTION="greater-than");

 CREATE FUNCTION namege(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namege' WITH (OID=658, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION namene(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namene' WITH (OID=659, DESCRIPTION="not equal");

 CREATE FUNCTION bpchar(bpchar, int4, bool) RETURNS bpchar LANGUAGE internal IMMUTABLE STRICT AS 'bpchar' WITH (OID=668, DESCRIPTION="adjust char() to typmod length");

 CREATE FUNCTION "varchar"("varchar", int4, bool) RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'varchar' WITH (OID=669, DESCRIPTION="adjust varchar() to typmod length");

 CREATE FUNCTION mktinterval(abstime, abstime) RETURNS tinterval LANGUAGE internal IMMUTABLE STRICT AS 'mktinterval' WITH (OID=676, DESCRIPTION="convert to tinterval");

 CREATE FUNCTION oidvectorne(oidvector, oidvector) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorne' WITH (OID=619, DESCRIPTION="not equal");

 CREATE FUNCTION oidvectorlt(oidvector, oidvector) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorlt' WITH (OID=677, DESCRIPTION="less-than");

 CREATE FUNCTION oidvectorle(oidvector, oidvector) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorle' WITH (OID=678, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION oidvectoreq(oidvector, oidvector) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectoreq' WITH (OID=679, DESCRIPTION="equal");

 CREATE FUNCTION oidvectorge(oidvector, oidvector) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorge' WITH (OID=680, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION oidvectorgt(oidvector, oidvector) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorgt' WITH (OID=681, DESCRIPTION="greater-than");

-- OIDS 700 - 799

 CREATE FUNCTION getpgusername() RETURNS name LANGUAGE internal STABLE STRICT AS 'current_user' WITH (OID=710, DESCRIPTION="deprecated -- use current_user");

 CREATE FUNCTION oidlt(oid, oid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidlt' WITH (OID=716, DESCRIPTION="less-than");

 CREATE FUNCTION oidle(oid, oid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidle' WITH (OID=717, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION octet_length(bytea) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'byteaoctetlen' WITH (OID=720, DESCRIPTION="octet length");

 CREATE FUNCTION get_byte(bytea, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'byteaGetByte' WITH (OID=721, DESCRIPTION="get byte");

 CREATE FUNCTION set_byte(bytea, int4, int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'byteaSetByte' WITH (OID=722, DESCRIPTION="set byte");

 CREATE FUNCTION get_bit(bytea, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'byteaGetBit' WITH (OID=723, DESCRIPTION="get bit");

 CREATE FUNCTION set_bit(bytea, int4, int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'byteaSetBit' WITH (OID=724, DESCRIPTION="set bit");

 CREATE FUNCTION dist_pl(point, line) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_pl' WITH (OID=725, DESCRIPTION="distance between point and line");

 CREATE FUNCTION dist_lb(line, box) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_lb' WITH (OID=726, DESCRIPTION="distance between line and box");

 CREATE FUNCTION dist_sl(lseg, line) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_sl' WITH (OID=727, DESCRIPTION="distance between lseg and line");

 CREATE FUNCTION dist_cpoly(circle, polygon) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_cpoly' WITH (OID=728, DESCRIPTION="distance between");

 CREATE FUNCTION poly_distance(polygon, polygon) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'poly_distance' WITH (OID=729, DESCRIPTION="distance between");

 CREATE FUNCTION text_lt(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_lt' WITH (OID=740, DESCRIPTION="less-than");

 CREATE FUNCTION text_le(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_le' WITH (OID=741, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION text_gt(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_gt' WITH (OID=742, DESCRIPTION="greater-than");

 CREATE FUNCTION text_ge(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_ge' WITH (OID=743, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION "current_user"() RETURNS name LANGUAGE internal STABLE STRICT AS 'current_user' WITH (OID=745, DESCRIPTION="current user name");

 CREATE FUNCTION "session_user"() RETURNS name LANGUAGE internal STABLE STRICT AS 'session_user' WITH (OID=746, DESCRIPTION="session user name");

 CREATE FUNCTION array_eq(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'array_eq' WITH (OID=744, DESCRIPTION="array equal");

 CREATE FUNCTION array_ne(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'array_ne' WITH (OID=390, DESCRIPTION="array not equal");

 CREATE FUNCTION array_lt(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'array_lt' WITH (OID=391, DESCRIPTION="array less than");

 CREATE FUNCTION array_gt(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'array_gt' WITH (OID=392, DESCRIPTION="array greater than");

 CREATE FUNCTION array_le(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'array_le' WITH (OID=393, DESCRIPTION="array less than or equal");

 CREATE FUNCTION array_ge(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'array_ge' WITH (OID=396, DESCRIPTION="array greater than or equal");

 CREATE FUNCTION array_dims(anyarray) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'array_dims' WITH (OID=747, DESCRIPTION="array dimensions");

 CREATE FUNCTION array_in(cstring, oid, int4) RETURNS anyarray LANGUAGE internal STABLE STRICT AS 'array_in' WITH (OID=750, DESCRIPTION="I/O");

-- #define ARRAY_OUT_OID 751
 CREATE FUNCTION array_out(anyarray) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'array_out' WITH (OID=751, DESCRIPTION="I/O");

 CREATE FUNCTION array_lower(anyarray, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'array_lower' WITH (OID=2091, DESCRIPTION="array lower dimension");

 CREATE FUNCTION array_upper(anyarray, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'array_upper' WITH (OID=2092, DESCRIPTION="array upper dimension");

 CREATE FUNCTION array_append(anyarray, anyelement) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'array_push' WITH (OID=378, DESCRIPTION="append element onto end of array");

 CREATE FUNCTION array_prepend(anyelement, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'array_push' WITH (OID=379, DESCRIPTION="prepend element onto front of array");

 CREATE FUNCTION array_cat(anyarray, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'array_cat' WITH (OID=383, DESCRIPTION="concatenate two arrays");

 CREATE FUNCTION array_coerce(anyarray) RETURNS anyarray LANGUAGE internal STABLE STRICT AS 'array_type_coerce' WITH (OID=384, DESCRIPTION="coerce array to another array type");

 CREATE FUNCTION string_to_array(text, text) RETURNS _text LANGUAGE internal IMMUTABLE STRICT AS 'text_to_array' WITH (OID=394, DESCRIPTION="split delimited text into text[]");

 CREATE FUNCTION array_to_string(anyarray, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'array_to_text' WITH (OID=395, DESCRIPTION="concatenate array elements, using delimiter, into text");

 CREATE FUNCTION array_larger(anyarray, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'array_larger' WITH (OID=515, DESCRIPTION="larger of two");

 CREATE FUNCTION array_smaller(anyarray, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'array_smaller' WITH (OID=516, DESCRIPTION="smaller of two");

-- MPP -- array_add -- special for prospective customer 
 CREATE FUNCTION array_add(_int4, _int4) RETURNS _int4 LANGUAGE internal IMMUTABLE STRICT AS 'array_int4_add' WITH (OID=6012, DESCRIPTION="itemwise add two integer arrays");

 CREATE FUNCTION array_agg_transfn(internal, anyelement) RETURNS internal LANGUAGE internal IMMUTABLE AS 'array_agg_transfn' WITH (OID=2908, DESCRIPTION="array_agg transition function");

 CREATE FUNCTION array_agg_finalfn(internal) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'array_agg_finalfn' WITH (OID=2909, DESCRIPTION="array_agg final function");

 CREATE FUNCTION array_agg(anyelement) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2910, DESCRIPTION="concatenate aggregate input into an array", proisagg="t");

 CREATE FUNCTION string_agg_transfn(internal, text) RETURNS internal LANGUAGE internal IMMUTABLE AS 'string_agg_transfn' WITH (OID=3534, DESCRIPTION="string_agg(text) transition function");

 CREATE FUNCTION string_agg_delim_transfn(internal, text, text) RETURNS internal LANGUAGE internal IMMUTABLE AS 'string_agg_delim_transfn' WITH (OID=3535, DESCRIPTION="string_agg(text, text) transition function");

 CREATE FUNCTION string_agg_finalfn(internal) RETURNS text LANGUAGE internal IMMUTABLE AS 'string_agg_finalfn' WITH (OID=3536, DESCRIPTION="string_agg final function");

 CREATE FUNCTION string_agg(text) RETURNS text LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3537, DESCRIPTION="concatenate aggregate input into an string", proisagg="t");

 CREATE FUNCTION string_agg(text, text) RETURNS text LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3538, DESCRIPTION="concatenate aggregate input into an string with delimiter", proisagg="t");

 CREATE FUNCTION smgrin(cstring) RETURNS smgr LANGUAGE internal STABLE STRICT AS 'smgrin' WITH (OID=760, DESCRIPTION="I/O");

 CREATE FUNCTION smgrout(smgr) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'smgrout' WITH (OID=761, DESCRIPTION="I/O");

 CREATE FUNCTION smgreq(smgr, smgr) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'smgreq' WITH (OID=762, DESCRIPTION="storage manager");

 CREATE FUNCTION smgrne(smgr, smgr) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'smgrne' WITH (OID=763, DESCRIPTION="storage manager");

 CREATE FUNCTION lo_import(text) RETURNS oid LANGUAGE internal VOLATILE STRICT AS 'lo_import' WITH (OID=764, DESCRIPTION="large object import");

 CREATE FUNCTION lo_export(oid, text) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'lo_export' WITH (OID=765, DESCRIPTION="large object export");

 CREATE FUNCTION int4inc(int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4inc' WITH (OID=766, DESCRIPTION="increment");

 CREATE FUNCTION int4larger(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4larger' WITH (OID=768, DESCRIPTION="larger of two");

 CREATE FUNCTION int4smaller(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4smaller' WITH (OID=769, DESCRIPTION="smaller of two");

 CREATE FUNCTION int2larger(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2larger' WITH (OID=770, DESCRIPTION="larger of two");

 CREATE FUNCTION int2smaller(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2smaller' WITH (OID=771, DESCRIPTION="smaller of two");

 CREATE FUNCTION gistgettuple(internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'gistgettuple' WITH (OID=774, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistgetmulti(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'gistgetmulti' WITH (OID=638, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistinsert(internal, internal, internal, internal, internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'gistinsert' WITH (OID=775, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistbeginscan(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'gistbeginscan' WITH (OID=777, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistrescan(internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'gistrescan' WITH (OID=778, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistendscan(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'gistendscan' WITH (OID=779, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistmarkpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'gistmarkpos' WITH (OID=780, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistrestrpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'gistrestrpos' WITH (OID=781, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistbuild(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'gistbuild' WITH (OID=782, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistbulkdelete(internal, internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'gistbulkdelete' WITH (OID=776, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistvacuumcleanup(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'gistvacuumcleanup' WITH (OID=2561, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistcostestimate(internal, internal, internal, internal, internal, internal, internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'gistcostestimate' WITH (OID=772, DESCRIPTION="gist(internal)");

 CREATE FUNCTION gistoptions(_text, bool) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'gistoptions' WITH (OID=2787, DESCRIPTION="gist(internal)");

 CREATE FUNCTION tintervaleq(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervaleq' WITH (OID=784, DESCRIPTION="equal");

 CREATE FUNCTION tintervalne(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalne' WITH (OID=785, DESCRIPTION="not equal");

 CREATE FUNCTION tintervallt(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervallt' WITH (OID=786, DESCRIPTION="less-than");

 CREATE FUNCTION tintervalgt(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalgt' WITH (OID=787, DESCRIPTION="greater-than");

 CREATE FUNCTION tintervalle(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalle' WITH (OID=788, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION tintervalge(tinterval, tinterval) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tintervalge' WITH (OID=789, DESCRIPTION="greater-than-or-equal");

-- OIDS 800 - 899

 CREATE FUNCTION oid(text) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'text_oid' WITH (OID=817, DESCRIPTION="convert text to oid");

 CREATE FUNCTION int2(text) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'text_int2' WITH (OID=818, DESCRIPTION="convert text to int2");

 CREATE FUNCTION int4(text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'text_int4' WITH (OID=819, DESCRIPTION="convert text to int4");

 CREATE FUNCTION float8(text) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'text_float8' WITH (OID=838, DESCRIPTION="convert text to float8");

 CREATE FUNCTION float4(text) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'text_float4' WITH (OID=839, DESCRIPTION="convert text to float4");

 CREATE FUNCTION text(float8) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'float8_text' WITH (OID=840, DESCRIPTION="convert float8 to text");

 CREATE FUNCTION text(float4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'float4_text' WITH (OID=841, DESCRIPTION="convert float4 to text");

 CREATE FUNCTION cash_mul_flt4(money, float4) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_mul_flt4' WITH (OID=846, DESCRIPTION="multiply");

 CREATE FUNCTION cash_div_flt4(money, float4) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_div_flt4' WITH (OID=847, DESCRIPTION="divide");

 CREATE FUNCTION flt4_mul_cash(float4, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'flt4_mul_cash' WITH (OID=848, DESCRIPTION="multiply");

 CREATE FUNCTION "position"(text, text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'textpos' WITH (OID=849, DESCRIPTION="return position of substring");

 CREATE FUNCTION textlike(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textlike' WITH (OID=850, DESCRIPTION="matches LIKE expression");

 CREATE FUNCTION textnlike(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textnlike' WITH (OID=851, DESCRIPTION="does not match LIKE expression");

 CREATE FUNCTION int48eq(int4, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int48eq' WITH (OID=852, DESCRIPTION="equal");

 CREATE FUNCTION int48ne(int4, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int48ne' WITH (OID=853, DESCRIPTION="not equal");

 CREATE FUNCTION int48lt(int4, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int48lt' WITH (OID=854, DESCRIPTION="less-than");

 CREATE FUNCTION int48gt(int4, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int48gt' WITH (OID=855, DESCRIPTION="greater-than");

 CREATE FUNCTION int48le(int4, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int48le' WITH (OID=856, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int48ge(int4, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int48ge' WITH (OID=857, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION namelike(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namelike' WITH (OID=858, DESCRIPTION="matches LIKE expression");

 CREATE FUNCTION namenlike(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namenlike' WITH (OID=859, DESCRIPTION="does not match LIKE expression");

 CREATE FUNCTION bpchar("char") RETURNS bpchar LANGUAGE internal IMMUTABLE STRICT AS 'char_bpchar' WITH (OID=860, DESCRIPTION="convert char to char()");

 CREATE FUNCTION current_database() RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'current_database' WITH (OID=861, DESCRIPTION="returns the current database");

 CREATE FUNCTION current_query() RETURNS text LANGUAGE internal VOLATILE AS 'current_query' WITH (OID=820, DESCRIPTION="returns the currently executing query");

 CREATE FUNCTION int4_mul_cash(int4, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'int4_mul_cash' WITH (OID=862, DESCRIPTION="multiply");

 CREATE FUNCTION int2_mul_cash(int2, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'int2_mul_cash' WITH (OID=863, DESCRIPTION="multiply");

 CREATE FUNCTION cash_mul_int4(money, int4) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_mul_int4' WITH (OID=864, DESCRIPTION="multiply");

 CREATE FUNCTION cash_div_int4(money, int4) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_div_int4' WITH (OID=865, DESCRIPTION="divide");

 CREATE FUNCTION cash_mul_int2(money, int2) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_mul_int2' WITH (OID=866, DESCRIPTION="multiply");

 CREATE FUNCTION cash_div_int2(money, int2) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_div_int2' WITH (OID=867, DESCRIPTION="divide");

 CREATE FUNCTION cash_in(cstring) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_in' WITH (OID=886, DESCRIPTION="I/O");

 CREATE FUNCTION cash_out(money) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'cash_out' WITH (OID=887, DESCRIPTION="I/O");

 CREATE FUNCTION cash_eq(money, money) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_eq' WITH (OID=888, DESCRIPTION="equal");

 CREATE FUNCTION cash_ne(money, money) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_ne' WITH (OID=889, DESCRIPTION="not equal");

 CREATE FUNCTION cash_lt(money, money) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_lt' WITH (OID=890, DESCRIPTION="less-than");

 CREATE FUNCTION cash_le(money, money) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_le' WITH (OID=891, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION cash_gt(money, money) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_gt' WITH (OID=892, DESCRIPTION="greater-than");

 CREATE FUNCTION cash_ge(money, money) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'cash_ge' WITH (OID=893, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION cash_pl(money, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_pl' WITH (OID=894, DESCRIPTION="add");

 CREATE FUNCTION cash_mi(money, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_mi' WITH (OID=895, DESCRIPTION="subtract");

 CREATE FUNCTION cash_mul_flt8(money, float8) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_mul_flt8' WITH (OID=896, DESCRIPTION="multiply");

 CREATE FUNCTION cash_div_flt8(money, float8) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_div_flt8' WITH (OID=897, DESCRIPTION="divide");

 CREATE FUNCTION cashlarger(money, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cashlarger' WITH (OID=898, DESCRIPTION="larger of two");

 CREATE FUNCTION cashsmaller(money, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cashsmaller' WITH (OID=899, DESCRIPTION="smaller of two");

 CREATE FUNCTION flt8_mul_cash(float8, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'flt8_mul_cash' WITH (OID=919, DESCRIPTION="multiply");

 CREATE FUNCTION cash_words(money) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'cash_words' WITH (OID=935, DESCRIPTION="output amount as words");

-- OIDS 900 - 999

 CREATE FUNCTION mod(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2mod' WITH (OID=940, DESCRIPTION="modulus");

 CREATE FUNCTION mod(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4mod' WITH (OID=941, DESCRIPTION="modulus");

 CREATE FUNCTION mod(int2, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int24mod' WITH (OID=942, DESCRIPTION="modulus");

 CREATE FUNCTION mod(int4, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int42mod' WITH (OID=943, DESCRIPTION="modulus");

 CREATE FUNCTION int8mod(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8mod' WITH (OID=945, DESCRIPTION="modulus");

 CREATE FUNCTION mod(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8mod' WITH (OID=947, DESCRIPTION="modulus");

 CREATE FUNCTION "char"(text) RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'text_char' WITH (OID=944, DESCRIPTION="convert text to char");

 CREATE FUNCTION text("char") RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'char_text' WITH (OID=946, DESCRIPTION="convert char to text");

 CREATE FUNCTION istrue(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'istrue' WITH (OID=950, DESCRIPTION="bool is true (not false or unknown)");

 CREATE FUNCTION isfalse(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'isfalse' WITH (OID=951, DESCRIPTION="bool is false (not true or unknown)");

 CREATE FUNCTION lo_open(oid, int4) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'lo_open' WITH (OID=952, DESCRIPTION="large object open");

 CREATE FUNCTION lo_close(int4) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'lo_close' WITH (OID=953, DESCRIPTION="large object close");

 CREATE FUNCTION loread(int4, int4) RETURNS bytea LANGUAGE internal VOLATILE STRICT AS 'loread' WITH (OID=954, DESCRIPTION="large object read");

 CREATE FUNCTION lowrite(int4, bytea) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'lowrite' WITH (OID=955, DESCRIPTION="large object write");

 CREATE FUNCTION lo_lseek(int4, int4, int4) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'lo_lseek' WITH (OID=956, DESCRIPTION="large object seek");

 CREATE FUNCTION lo_creat(int4) RETURNS oid LANGUAGE internal VOLATILE STRICT AS 'lo_creat' WITH (OID=957, DESCRIPTION="large object create");

 CREATE FUNCTION lo_create(oid) RETURNS oid LANGUAGE internal VOLATILE STRICT AS 'lo_create' WITH (OID=715, DESCRIPTION="large object create");

 CREATE FUNCTION lo_tell(int4) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'lo_tell' WITH (OID=958, DESCRIPTION="large object position");

 CREATE FUNCTION lo_truncate(int4, int4) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'lo_truncate' WITH (OID=828, DESCRIPTION="truncate large object");

 CREATE FUNCTION on_pl(point, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'on_pl' WITH (OID=959, DESCRIPTION="point on line?");

 CREATE FUNCTION on_sl(lseg, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'on_sl' WITH (OID=960, DESCRIPTION="lseg on line?");

 CREATE FUNCTION close_pl(point, line) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'close_pl' WITH (OID=961, DESCRIPTION="closest point on line");

 CREATE FUNCTION close_sl(lseg, line) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'close_sl' WITH (OID=962, DESCRIPTION="closest point to line segment on line");

 CREATE FUNCTION close_lb(line, box) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'close_lb' WITH (OID=963, DESCRIPTION="closest point to line on box");

 CREATE FUNCTION lo_unlink(oid) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'lo_unlink' WITH (OID=964, DESCRIPTION="large object unlink(delete)");

 CREATE FUNCTION path_inter(path, path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'path_inter' WITH (OID=973, DESCRIPTION="intersect?");

 CREATE FUNCTION area(box) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'box_area' WITH (OID=975, DESCRIPTION="box area");

 CREATE FUNCTION width(box) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'box_width' WITH (OID=976, DESCRIPTION="box width");

 CREATE FUNCTION height(box) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'box_height' WITH (OID=977, DESCRIPTION="box height");

 CREATE FUNCTION box_distance(box, box) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'box_distance' WITH (OID=978, DESCRIPTION="distance between boxes");

 CREATE FUNCTION area(path) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'path_area' WITH (OID=979, DESCRIPTION="area of a closed path");

 CREATE FUNCTION box_intersect(box, box) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'box_intersect' WITH (OID=980, DESCRIPTION="box intersection (another box)");

 CREATE FUNCTION diagonal(box) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'box_diagonal' WITH (OID=981, DESCRIPTION="box diagonal");

 CREATE FUNCTION path_n_lt(path, path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_lt' WITH (OID=982, DESCRIPTION="less-than");

 CREATE FUNCTION path_n_gt(path, path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_gt' WITH (OID=983, DESCRIPTION="greater-than");

 CREATE FUNCTION path_n_eq(path, path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_eq' WITH (OID=984, DESCRIPTION="equal");

 CREATE FUNCTION path_n_le(path, path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_le' WITH (OID=985, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION path_n_ge(path, path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'path_n_ge' WITH (OID=986, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION path_length(path) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'path_length' WITH (OID=987, DESCRIPTION="sum of path segment lengths");

 CREATE FUNCTION point_ne(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_ne' WITH (OID=988, DESCRIPTION="not equal");

 CREATE FUNCTION point_vert(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_vert' WITH (OID=989, DESCRIPTION="vertically aligned?");

 CREATE FUNCTION point_horiz(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_horiz' WITH (OID=990, DESCRIPTION="horizontally aligned?");

 CREATE FUNCTION point_distance(point, point) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'point_distance' WITH (OID=991, DESCRIPTION="distance between");

 CREATE FUNCTION slope(point, point) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'point_slope' WITH (OID=992, DESCRIPTION="slope between points");

 CREATE FUNCTION lseg(point, point) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lseg_construct' WITH (OID=993, DESCRIPTION="convert points to line segment");

 CREATE FUNCTION lseg_intersect(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_intersect' WITH (OID=994, DESCRIPTION="intersect?");

 CREATE FUNCTION lseg_parallel(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_parallel' WITH (OID=995, DESCRIPTION="parallel?");

 CREATE FUNCTION lseg_perp(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_perp' WITH (OID=996, DESCRIPTION="perpendicular?");

 CREATE FUNCTION lseg_vertical(lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_vertical' WITH (OID=997, DESCRIPTION="vertical?");

 CREATE FUNCTION lseg_horizontal(lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_horizontal' WITH (OID=998, DESCRIPTION="horizontal?");

 CREATE FUNCTION lseg_eq(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_eq' WITH (OID=999, DESCRIPTION="equal");

 CREATE FUNCTION date(text) RETURNS date LANGUAGE internal STABLE STRICT AS 'text_date' WITH (OID=748, DESCRIPTION="convert text to date");

 CREATE FUNCTION text(date) RETURNS text LANGUAGE internal STABLE STRICT AS 'date_text' WITH (OID=749, DESCRIPTION="convert date to text");

 CREATE FUNCTION "time"(text) RETURNS "time" LANGUAGE internal STABLE STRICT AS 'text_time' WITH (OID=837, DESCRIPTION="convert text to time");

 CREATE FUNCTION text("time") RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'time_text' WITH (OID=948, DESCRIPTION="convert time to text");

 CREATE FUNCTION timetz(text) RETURNS timetz LANGUAGE internal STABLE STRICT AS 'text_timetz' WITH (OID=938, DESCRIPTION="convert text to timetz");

 CREATE FUNCTION text(timetz) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'timetz_text' WITH (OID=939, DESCRIPTION="convert timetz to text");

-- OIDS 1000 - 1099

 CREATE FUNCTION timezone("interval", timestamptz) RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_izone' WITH (OID=1026, DESCRIPTION="adjust timestamp to new time zone");

 CREATE FUNCTION nullvalue("any") RETURNS bool LANGUAGE internal IMMUTABLE AS 'nullvalue' WITH (OID=1029, DESCRIPTION="(internal)");

 CREATE FUNCTION nonnullvalue("any") RETURNS bool LANGUAGE internal IMMUTABLE AS 'nonnullvalue' WITH (OID=1030, DESCRIPTION="(internal)");

 CREATE FUNCTION aclitemin(cstring) RETURNS aclitem LANGUAGE internal STABLE STRICT AS 'aclitemin' WITH (OID=1031, DESCRIPTION="I/O");

 CREATE FUNCTION aclitemout(aclitem) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'aclitemout' WITH (OID=1032, DESCRIPTION="I/O");

 CREATE FUNCTION aclinsert(_aclitem, aclitem) RETURNS _aclitem LANGUAGE internal IMMUTABLE STRICT AS 'aclinsert' WITH (OID=1035, DESCRIPTION="add/update ACL item");

 CREATE FUNCTION aclremove(_aclitem, aclitem) RETURNS _aclitem LANGUAGE internal IMMUTABLE STRICT AS 'aclremove' WITH (OID=1036, DESCRIPTION="remove ACL item");

 CREATE FUNCTION aclcontains(_aclitem, aclitem) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'aclcontains' WITH (OID=1037, DESCRIPTION="ACL contains item?");

 CREATE FUNCTION aclitemeq(aclitem, aclitem) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'aclitem_eq' WITH (OID=1062, DESCRIPTION="equality operator for ACL items");

 CREATE FUNCTION makeaclitem(oid, oid, text, bool) RETURNS aclitem LANGUAGE internal IMMUTABLE STRICT AS 'makeaclitem' WITH (OID=1365, DESCRIPTION="make ACL item");

 CREATE FUNCTION bpcharin(cstring, oid, int4) RETURNS bpchar LANGUAGE internal IMMUTABLE STRICT AS 'bpcharin' WITH (OID=1044, DESCRIPTION="I/O");

 CREATE FUNCTION bpcharout(bpchar) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'bpcharout' WITH (OID=1045, DESCRIPTION="I/O");

 CREATE FUNCTION varcharin(cstring, oid, int4) RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'varcharin' WITH (OID=1046, DESCRIPTION="I/O");

 CREATE FUNCTION varcharout("varchar") RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'varcharout' WITH (OID=1047, DESCRIPTION="I/O");

 CREATE FUNCTION bpchareq(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bpchareq' WITH (OID=1048, DESCRIPTION="equal");

 CREATE FUNCTION bpcharlt(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bpcharlt' WITH (OID=1049, DESCRIPTION="less-than");

 CREATE FUNCTION bpcharle(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bpcharle' WITH (OID=1050, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION bpchargt(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bpchargt' WITH (OID=1051, DESCRIPTION="greater-than");

 CREATE FUNCTION bpcharge(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bpcharge' WITH (OID=1052, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION bpcharne(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bpcharne' WITH (OID=1053, DESCRIPTION="not equal");

 CREATE FUNCTION bpchar_larger(bpchar, bpchar) RETURNS bpchar LANGUAGE internal IMMUTABLE STRICT AS 'bpchar_larger' WITH (OID=1063, DESCRIPTION="larger of two");

 CREATE FUNCTION bpchar_smaller(bpchar, bpchar) RETURNS bpchar LANGUAGE internal IMMUTABLE STRICT AS 'bpchar_smaller' WITH (OID=1064, DESCRIPTION="smaller of two");

 CREATE FUNCTION bpcharcmp(bpchar, bpchar) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bpcharcmp' WITH (OID=1078, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION hashbpchar(bpchar) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'hashbpchar' WITH (OID=1080, DESCRIPTION="hash");

 CREATE FUNCTION format_type(oid, int4) RETURNS text LANGUAGE internal STABLE AS 'format_type' WITH (OID=1081, DESCRIPTION="format a type oid and atttypmod to canonical SQL");

 CREATE FUNCTION date_in(cstring) RETURNS date LANGUAGE internal STABLE STRICT AS 'date_in' WITH (OID=1084, DESCRIPTION="I/O");

 CREATE FUNCTION date_out(date) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'date_out' WITH (OID=1085, DESCRIPTION="I/O");

 CREATE FUNCTION date_eq(date, date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_eq' WITH (OID=1086, DESCRIPTION="equal");

 CREATE FUNCTION date_lt(date, date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_lt' WITH (OID=1087, DESCRIPTION="less-than");

 CREATE FUNCTION date_le(date, date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_le' WITH (OID=1088, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION date_gt(date, date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_gt' WITH (OID=1089, DESCRIPTION="greater-than");

 CREATE FUNCTION date_ge(date, date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_ge' WITH (OID=1090, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION date_ne(date, date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_ne' WITH (OID=1091, DESCRIPTION="not equal");

 CREATE FUNCTION date_cmp(date, date) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'date_cmp' WITH (OID=1092, DESCRIPTION="less-equal-greater");

-- OIDS 1100 - 1199

 CREATE FUNCTION time_lt("time", "time") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'time_lt' WITH (OID=1102, DESCRIPTION="less-than");

 CREATE FUNCTION time_le("time", "time") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'time_le' WITH (OID=1103, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION time_gt("time", "time") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'time_gt' WITH (OID=1104, DESCRIPTION="greater-than");

 CREATE FUNCTION time_ge("time", "time") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'time_ge' WITH (OID=1105, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION time_ne("time", "time") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'time_ne' WITH (OID=1106, DESCRIPTION="not equal");

 CREATE FUNCTION time_cmp("time", "time") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'time_cmp' WITH (OID=1107, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION date_larger(date, date) RETURNS date LANGUAGE internal IMMUTABLE STRICT AS 'date_larger' WITH (OID=1138, DESCRIPTION="larger of two");

 CREATE FUNCTION date_smaller(date, date) RETURNS date LANGUAGE internal IMMUTABLE STRICT AS 'date_smaller' WITH (OID=1139, DESCRIPTION="smaller of two");

 CREATE FUNCTION date_mi(date, date) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'date_mi' WITH (OID=1140, DESCRIPTION="subtract");

 CREATE FUNCTION date_pli(date, int4) RETURNS date LANGUAGE internal IMMUTABLE STRICT AS 'date_pli' WITH (OID=1141, DESCRIPTION="add");

 CREATE FUNCTION date_mii(date, int4) RETURNS date LANGUAGE internal IMMUTABLE STRICT AS 'date_mii' WITH (OID=1142, DESCRIPTION="subtract");

 CREATE FUNCTION time_in(cstring, oid, int4) RETURNS "time" LANGUAGE internal STABLE STRICT AS 'time_in' WITH (OID=1143, DESCRIPTION="I/O");

 CREATE FUNCTION time_out("time") RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'time_out' WITH (OID=1144, DESCRIPTION="I/O");

 CREATE FUNCTION time_eq("time", "time") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'time_eq' WITH (OID=1145, DESCRIPTION="equal");

 CREATE FUNCTION circle_add_pt(circle, point) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'circle_add_pt' WITH (OID=1146, DESCRIPTION="add");

 CREATE FUNCTION circle_sub_pt(circle, point) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'circle_sub_pt' WITH (OID=1147, DESCRIPTION="subtract");

 CREATE FUNCTION circle_mul_pt(circle, point) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'circle_mul_pt' WITH (OID=1148, DESCRIPTION="multiply");

 CREATE FUNCTION circle_div_pt(circle, point) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'circle_div_pt' WITH (OID=1149, DESCRIPTION="divide");

 CREATE FUNCTION timestamptz_in(cstring, oid, int4) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'timestamptz_in' WITH (OID=1150, DESCRIPTION="I/O");

 CREATE FUNCTION timestamptz_out(timestamptz) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'timestamptz_out' WITH (OID=1151, DESCRIPTION="I/O");

 CREATE FUNCTION timestamptz_eq(timestamptz, timestamptz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_eq' WITH (OID=1152, DESCRIPTION="equal");

 CREATE FUNCTION timestamptz_ne(timestamptz, timestamptz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ne' WITH (OID=1153, DESCRIPTION="not equal");

 CREATE FUNCTION timestamptz_lt(timestamptz, timestamptz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_lt' WITH (OID=1154, DESCRIPTION="less-than");

 CREATE FUNCTION timestamptz_le(timestamptz, timestamptz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_le' WITH (OID=1155, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION timestamptz_ge(timestamptz, timestamptz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ge' WITH (OID=1156, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION timestamptz_gt(timestamptz, timestamptz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_gt' WITH (OID=1157, DESCRIPTION="greater-than");

 CREATE FUNCTION to_timestamp(float8) RETURNS pg_catalog.timestamptz LANGUAGE sql IMMUTABLE STRICT AS $$select ('epoch'::timestamptz + $1 * '1 second'::interval)$$  WITH (OID=1158, DESCRIPTION="convert UNIX epoch to timestamptz");

 CREATE FUNCTION timezone(text, timestamptz) RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_zone' WITH (OID=1159, DESCRIPTION="adjust timestamp to new time zone");

 CREATE FUNCTION interval_in(cstring, oid, int4) RETURNS "interval" LANGUAGE internal STABLE STRICT AS 'interval_in' WITH (OID=1160, DESCRIPTION="I/O");

 CREATE FUNCTION interval_out("interval") RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'interval_out' WITH (OID=1161, DESCRIPTION="I/O");

 CREATE FUNCTION interval_eq("interval", "interval") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_eq' WITH (OID=1162, DESCRIPTION="equal");

 CREATE FUNCTION interval_ne("interval", "interval") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_ne' WITH (OID=1163, DESCRIPTION="not equal");

 CREATE FUNCTION interval_lt("interval", "interval") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_lt' WITH (OID=1164, DESCRIPTION="less-than");

 CREATE FUNCTION interval_le("interval", "interval") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_le' WITH (OID=1165, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION interval_ge("interval", "interval") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_ge' WITH (OID=1166, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION interval_gt("interval", "interval") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_gt' WITH (OID=1167, DESCRIPTION="greater-than");

 CREATE FUNCTION interval_um("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_um' WITH (OID=1168, DESCRIPTION="subtract");

 CREATE FUNCTION interval_pl("interval", "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_pl' WITH (OID=1169, DESCRIPTION="add");

 CREATE FUNCTION interval_mi("interval", "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_mi' WITH (OID=1170, DESCRIPTION="subtract");

 CREATE FUNCTION date_part(text, timestamptz) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'timestamptz_part' WITH (OID=1171, DESCRIPTION="extract field from timestamp with time zone");

 CREATE FUNCTION date_part(text, "interval") RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'interval_part' WITH (OID=1172, DESCRIPTION="extract field from interval");

 CREATE FUNCTION timestamptz(abstime) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'abstime_timestamptz' WITH (OID=1173, DESCRIPTION="convert abstime to timestamp with time zone");

 CREATE FUNCTION timestamptz(date) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'date_timestamptz' WITH (OID=1174, DESCRIPTION="convert date to timestamp with time zone");

 CREATE FUNCTION justify_interval("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_justify_interval' WITH (OID=2711, DESCRIPTION="promote groups of 24 hours to numbers of days and promote groups of 30 days to numbers of months");

 CREATE FUNCTION justify_hours("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_justify_hours' WITH (OID=1175, DESCRIPTION="promote groups of 24 hours to numbers of days");

 CREATE FUNCTION justify_days("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_justify_days' WITH (OID=1295, DESCRIPTION="promote groups of 30 days to numbers of months");

 CREATE FUNCTION timestamptz(date, "time") RETURNS pg_catalog.timestamptz LANGUAGE sql STABLE STRICT AS $$select cast(($1 + $2) as timestamp with time zone)$$  WITH (OID=1176, DESCRIPTION="convert date and time to timestamp with time zone");

 CREATE FUNCTION "interval"(reltime) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'reltime_interval' WITH (OID=1177, DESCRIPTION="convert reltime to interval");

 CREATE FUNCTION date(timestamptz) RETURNS date LANGUAGE internal STABLE STRICT AS 'timestamptz_date' WITH (OID=1178, DESCRIPTION="convert timestamp with time zone to date");

 CREATE FUNCTION date(abstime) RETURNS date LANGUAGE internal STABLE STRICT AS 'abstime_date' WITH (OID=1179, DESCRIPTION="convert abstime to date");

 CREATE FUNCTION abstime(timestamptz) RETURNS abstime LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_abstime' WITH (OID=1180, DESCRIPTION="convert timestamp with time zone to abstime");

 CREATE FUNCTION age(xid) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'xid_age' WITH (OID=1181, DESCRIPTION="age of a transaction ID, in transactions before current transaction");

 CREATE FUNCTION timestamptz_mi(timestamptz, timestamptz) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_mi' WITH (OID=1188, DESCRIPTION="subtract");

 CREATE FUNCTION timestamptz_pl_interval(timestamptz, "interval") RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'timestamptz_pl_interval' WITH (OID=1189, DESCRIPTION="plus");

 CREATE FUNCTION timestamptz_mi_interval(timestamptz, "interval") RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'timestamptz_mi_interval' WITH (OID=1190, DESCRIPTION="minus");

 CREATE FUNCTION timestamptz(text) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'text_timestamptz' WITH (OID=1191, DESCRIPTION="convert text to timestamp with time zone");

 CREATE FUNCTION text(timestamptz) RETURNS text LANGUAGE internal STABLE STRICT AS 'timestamptz_text' WITH (OID=1192, DESCRIPTION="convert timestamp with time zone to text");

 CREATE FUNCTION text("interval") RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'interval_text' WITH (OID=1193, DESCRIPTION="convert interval to text");

 CREATE FUNCTION reltime("interval") RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'interval_reltime' WITH (OID=1194, DESCRIPTION="convert interval to reltime");

 CREATE FUNCTION timestamptz_smaller(timestamptz, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_smaller' WITH (OID=1195, DESCRIPTION="smaller of two");

 CREATE FUNCTION timestamptz_larger(timestamptz, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_larger' WITH (OID=1196, DESCRIPTION="larger of two");

 CREATE FUNCTION interval_smaller("interval", "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_smaller' WITH (OID=1197, DESCRIPTION="smaller of two");

 CREATE FUNCTION interval_larger("interval", "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_larger' WITH (OID=1198, DESCRIPTION="larger of two");

 CREATE FUNCTION age(timestamptz, timestamptz) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_age' WITH (OID=1199, DESCRIPTION="date difference preserving months and years");

-- OIDS 1200 - 1299

 CREATE FUNCTION "interval"("interval", int4) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_scale' WITH (OID=1200, DESCRIPTION="adjust interval precision");

 CREATE FUNCTION obj_description(oid, name) RETURNS pg_catalog.text LANGUAGE sql STABLE STRICT AS $$select description from pg_catalog.pg_description where objoid = $1 and classoid = (select oid from pg_catalog.pg_class where relname = $2 and relnamespace = PGNSP) and objsubid = 0$$  WITH (OID=1215, DESCRIPTION="get description for object id and catalog name");

 CREATE FUNCTION col_description(oid, int4) RETURNS pg_catalog.text LANGUAGE sql STABLE STRICT AS $$select description from pg_catalog.pg_description where objoid = $1 and classoid = 'pg_catalog.pg_class'::regclass and objsubid = $2$$  WITH (OID=1216, DESCRIPTION="get description for table column");

 CREATE FUNCTION shobj_description(oid, name) RETURNS pg_catalog.text LANGUAGE sql STABLE STRICT AS $$select description from pg_catalog.pg_shdescription where objoid = $1 and classoid = (select oid from pg_catalog.pg_class where relname = $2 and relnamespace = PGNSP)$$  WITH (OID=1993, DESCRIPTION="get description for object id and shared catalog name");

 CREATE FUNCTION date_trunc(text, timestamptz) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'timestamptz_trunc' WITH (OID=1217, DESCRIPTION="truncate timestamp with time zone to specified units");

 CREATE FUNCTION date_trunc(text, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_trunc' WITH (OID=1218, DESCRIPTION="truncate interval to specified units");

 CREATE FUNCTION int8inc(int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8inc' WITH (OID=1219, DESCRIPTION="increment");

 CREATE FUNCTION int8dec(int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8dec' WITH (OID=2857);

 CREATE FUNCTION int8inc_any(int8, "any") RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8inc_any' WITH (OID=2804, DESCRIPTION="increment, ignores second argument");

 CREATE FUNCTION int8abs(int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8abs' WITH (OID=1230, DESCRIPTION="absolute value");

 CREATE FUNCTION int8larger(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8larger' WITH (OID=1236, DESCRIPTION="larger of two");

 CREATE FUNCTION int8smaller(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8smaller' WITH (OID=1237, DESCRIPTION="smaller of two");

 CREATE FUNCTION texticregexeq(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texticregexeq' WITH (OID=1238, DESCRIPTION="matches regex., case-insensitive");

 CREATE FUNCTION texticregexne(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texticregexne' WITH (OID=1239, DESCRIPTION="does not match regex., case-insensitive");

 CREATE FUNCTION nameicregexeq(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'nameicregexeq' WITH (OID=1240, DESCRIPTION="matches regex., case-insensitive");

 CREATE FUNCTION nameicregexne(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'nameicregexne' WITH (OID=1241, DESCRIPTION="does not match regex., case-insensitive");

 CREATE FUNCTION int4abs(int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4abs' WITH (OID=1251, DESCRIPTION="absolute value");

 CREATE FUNCTION int2abs(int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2abs' WITH (OID=1253, DESCRIPTION="absolute value");

 CREATE FUNCTION "interval"(text) RETURNS "interval" LANGUAGE internal STABLE STRICT AS 'text_interval' WITH (OID=1263, DESCRIPTION="convert text to interval");

 CREATE FUNCTION "overlaps"(timetz, timetz, timetz, timetz) RETURNS bool LANGUAGE internal IMMUTABLE AS 'overlaps_timetz' WITH (OID=1271, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION datetime_pl(date, "time") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'datetime_timestamp' WITH (OID=1272, DESCRIPTION="convert date and time to timestamp");

 CREATE FUNCTION date_part(text, timetz) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'timetz_part' WITH (OID=1273, DESCRIPTION="extract field from time with time zone");

 CREATE FUNCTION int84pl(int8, int4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int84pl' WITH (OID=1274, DESCRIPTION="add");

 CREATE FUNCTION int84mi(int8, int4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int84mi' WITH (OID=1275, DESCRIPTION="subtract");

 CREATE FUNCTION int84mul(int8, int4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int84mul' WITH (OID=1276, DESCRIPTION="multiply");

 CREATE FUNCTION int84div(int8, int4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int84div' WITH (OID=1277, DESCRIPTION="divide");

 CREATE FUNCTION int48pl(int4, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int48pl' WITH (OID=1278, DESCRIPTION="add");

 CREATE FUNCTION int48mi(int4, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int48mi' WITH (OID=1279, DESCRIPTION="subtract");

 CREATE FUNCTION int48mul(int4, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int48mul' WITH (OID=1280, DESCRIPTION="multiply");

 CREATE FUNCTION int48div(int4, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int48div' WITH (OID=1281, DESCRIPTION="divide");

 CREATE FUNCTION oid(int8) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'i8tooid' WITH (OID=1287, DESCRIPTION="convert int8 to oid");

 CREATE FUNCTION int8(oid) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'oidtoi8' WITH (OID=1288, DESCRIPTION="convert oid to int8");

 CREATE FUNCTION text(int8) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'int8_text' WITH (OID=1289, DESCRIPTION="convert int8 to text");

 CREATE FUNCTION int8(text) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'text_int8' WITH (OID=1290, DESCRIPTION="convert text to int8");

 CREATE FUNCTION array_length_coerce(anyarray, int4, bool) RETURNS anyarray LANGUAGE internal STABLE STRICT AS 'array_length_coerce' WITH (OID=1291, DESCRIPTION="adjust any array to new element typmod");

 CREATE FUNCTION tideq(tid, tid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tideq' WITH (OID=1292, DESCRIPTION="equal");

 CREATE FUNCTION currtid(oid, tid) RETURNS tid LANGUAGE internal VOLATILE STRICT AS 'currtid_byreloid' WITH (OID=1293, DESCRIPTION="latest tid of a tuple");

 CREATE FUNCTION currtid2(text, tid) RETURNS tid LANGUAGE internal VOLATILE STRICT AS 'currtid_byrelname' WITH (OID=1294, DESCRIPTION="latest tid of a tuple");

 CREATE FUNCTION tidne(tid, tid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tidne' WITH (OID=1265, DESCRIPTION="not equal");

 CREATE FUNCTION tidgt(tid, tid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tidgt' WITH (OID=2790, DESCRIPTION="greater-than");

 CREATE FUNCTION tidlt(tid, tid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tidlt' WITH (OID=2791, DESCRIPTION="less-than");

 CREATE FUNCTION tidge(tid, tid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tidge' WITH (OID=2792, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION tidle(tid, tid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'tidle' WITH (OID=2793, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION bttidcmp(tid, tid) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttidcmp' WITH (OID=2794, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION tidlarger(tid, tid) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'tidlarger' WITH (OID=2795, DESCRIPTION="larger of two");

 CREATE FUNCTION tidsmaller(tid, tid) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'tidsmaller' WITH (OID=2796, DESCRIPTION="smaller of two");

 CREATE FUNCTION timedate_pl("time", date) RETURNS pg_catalog."timestamp" LANGUAGE sql IMMUTABLE STRICT AS $$select ($2 + $1)$$  WITH (OID=1296, DESCRIPTION="convert time and date to timestamp");

 CREATE FUNCTION datetimetz_pl(date, timetz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'datetimetz_timestamptz' WITH (OID=1297, DESCRIPTION="convert date and time with time zone to timestamp with time zone");

 CREATE FUNCTION timetzdate_pl(timetz, date) RETURNS pg_catalog.timestamptz LANGUAGE sql IMMUTABLE STRICT AS $$select ($2 + $1)$$  WITH (OID=1298, DESCRIPTION="convert time with time zone and date to timestamp with time zone");

 CREATE FUNCTION now() RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'now' WITH (OID=1299, DESCRIPTION="current transaction time");

 CREATE FUNCTION transaction_timestamp() RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'now' WITH (OID=2647, DESCRIPTION="current transaction time");

 CREATE FUNCTION statement_timestamp() RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'statement_timestamp' WITH (OID=2648, DESCRIPTION="current statement time");

 CREATE FUNCTION clock_timestamp() RETURNS timestamptz LANGUAGE internal VOLATILE STRICT AS 'clock_timestamp' WITH (OID=2649, DESCRIPTION="current clock time");

-- OIDS 1300 - 1399

 CREATE FUNCTION positionsel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'positionsel' WITH (OID=1300, DESCRIPTION="restriction selectivity for position-comparison operators");

 CREATE FUNCTION positionjoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'positionjoinsel' WITH (OID=1301, DESCRIPTION="join selectivity for position-comparison operators");

 CREATE FUNCTION contsel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'contsel' WITH (OID=1302, DESCRIPTION="restriction selectivity for containment comparison operators");

 CREATE FUNCTION contjoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'contjoinsel' WITH (OID=1303, DESCRIPTION="join selectivity for containment comparison operators");

 CREATE FUNCTION "overlaps"(timestamptz, timestamptz, timestamptz, timestamptz) RETURNS bool LANGUAGE internal IMMUTABLE AS 'overlaps_timestamp' WITH (OID=1304, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"(timestamptz, "interval", timestamptz, "interval") RETURNS pg_catalog.bool LANGUAGE sql STABLE AS $$select ($1, ($1 + $2)) overlaps ($3, ($3 + $4))$$  WITH (OID=1305, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"(timestamptz, timestamptz, timestamptz, "interval") RETURNS pg_catalog.bool LANGUAGE sql STABLE AS $$select ($1, $2) overlaps ($3, ($3 + $4))$$  WITH (OID=1306, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"(timestamptz, "interval", timestamptz, timestamptz) RETURNS pg_catalog.bool LANGUAGE sql STABLE AS $$select ($1, ($1 + $2)) overlaps ($3, $4)$$  WITH (OID=1307, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"("time", "time", "time", "time") RETURNS bool LANGUAGE internal IMMUTABLE AS 'overlaps_time' WITH (OID=1308, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"("time", "interval", "time", "interval") RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE AS $$select ($1, ($1 + $2)) overlaps ($3, ($3 + $4))$$  WITH (OID=1309, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"("time", "time", "time", "interval") RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE AS $$select ($1, $2) overlaps ($3, ($3 + $4))$$  WITH (OID=1310, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"("time", "interval", "time", "time") RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE AS $$select ($1, ($1 + $2)) overlaps ($3, $4)$$  WITH (OID=1311, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION timestamp_in(cstring, oid, int4) RETURNS "timestamp" LANGUAGE internal STABLE STRICT AS 'timestamp_in' WITH (OID=1312, DESCRIPTION="I/O");

 CREATE FUNCTION timestamp_out("timestamp") RETURNS cstring LANGUAGE internal STABLE STRICT AS 'timestamp_out' WITH (OID=1313, DESCRIPTION="I/O");

 CREATE FUNCTION timestamptz_cmp(timestamptz, timestamptz) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_cmp' WITH (OID=1314, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION interval_cmp("interval", "interval") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'interval_cmp' WITH (OID=1315, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION "time"("timestamp") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_time' WITH (OID=1316, DESCRIPTION="convert timestamp to time");

 CREATE FUNCTION length(text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'textlen' WITH (OID=1317, DESCRIPTION="length");

 CREATE FUNCTION length(bpchar) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bpcharlen' WITH (OID=1318, DESCRIPTION="character length");

 CREATE FUNCTION xideqint4(xid, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'xideq' WITH (OID=1319, DESCRIPTION="equal");

 CREATE FUNCTION interval_div("interval", float8) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_div' WITH (OID=1326, DESCRIPTION="divide");

 CREATE FUNCTION interval_interval_div("interval", "interval") RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'interval_interval_div' WITH (OID=2918, DESCRIPTION="divide");

 CREATE FUNCTION interval_interval_mod("interval", "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_interval_mod' WITH (OID=2919, DESCRIPTION="modulus");

 CREATE FUNCTION dlog10(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dlog10' WITH (OID=1339, DESCRIPTION="base 10 logarithm");

 CREATE FUNCTION "log"(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dlog10' WITH (OID=1340, DESCRIPTION="base 10 logarithm");

 CREATE FUNCTION ln(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dlog1' WITH (OID=1341, DESCRIPTION="natural logarithm");

 CREATE FUNCTION round(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dround' WITH (OID=1342, DESCRIPTION="round to nearest integer");

 CREATE FUNCTION trunc(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dtrunc' WITH (OID=1343, DESCRIPTION="truncate to integer");

 CREATE FUNCTION sqrt(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dsqrt' WITH (OID=1344, DESCRIPTION="square root");

 CREATE FUNCTION cbrt(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dcbrt' WITH (OID=1345, DESCRIPTION="cube root");

 CREATE FUNCTION pow(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dpow' WITH (OID=1346, DESCRIPTION="exponentiation");

 CREATE FUNCTION power(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dpow' WITH (OID=1368, DESCRIPTION="exponentiation");

 CREATE FUNCTION exp(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dexp' WITH (OID=1347, DESCRIPTION="exponential");

-- This form of obj_description is now deprecated, since it will fail if
-- OIDs are not unique across system catalogs.	Use the other forms instead.

 CREATE FUNCTION obj_description(oid) RETURNS pg_catalog.text LANGUAGE sql STABLE STRICT AS $$select description from pg_catalog.pg_description where objoid = $1 and objsubid = 0$$  WITH (OID=1348, DESCRIPTION="get description for object id (deprecated)");

 CREATE FUNCTION oidvectortypes(oidvector) RETURNS text LANGUAGE internal STABLE STRICT AS 'oidvectortypes' WITH (OID=1349, DESCRIPTION="print type names of oidvector field");

 CREATE FUNCTION timetz_in(cstring, oid, int4) RETURNS timetz LANGUAGE internal STABLE STRICT AS 'timetz_in' WITH (OID=1350, DESCRIPTION="I/O");

 CREATE FUNCTION timetz_out(timetz) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'timetz_out' WITH (OID=1351, DESCRIPTION="I/O");

 CREATE FUNCTION timetz_eq(timetz, timetz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_eq' WITH (OID=1352, DESCRIPTION="equal");

 CREATE FUNCTION timetz_ne(timetz, timetz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_ne' WITH (OID=1353, DESCRIPTION="not equal");

 CREATE FUNCTION timetz_lt(timetz, timetz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_lt' WITH (OID=1354, DESCRIPTION="less-than");

 CREATE FUNCTION timetz_le(timetz, timetz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_le' WITH (OID=1355, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION timetz_ge(timetz, timetz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_ge' WITH (OID=1356, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION timetz_gt(timetz, timetz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timetz_gt' WITH (OID=1357, DESCRIPTION="greater-than");

 CREATE FUNCTION timetz_cmp(timetz, timetz) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'timetz_cmp' WITH (OID=1358, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION timestamptz(date, timetz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'datetimetz_timestamptz' WITH (OID=1359, DESCRIPTION="convert date and time with time zone to timestamp with time zone");

 CREATE FUNCTION "time"(abstime) RETURNS pg_catalog."time" LANGUAGE sql STABLE STRICT AS $$select cast(cast($1 as timestamp without time zone) as time)$$  WITH (OID=1364, DESCRIPTION="convert abstime to time");

 CREATE FUNCTION character_length(bpchar) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bpcharlen' WITH (OID=1367, DESCRIPTION="character length");

 CREATE FUNCTION character_length(text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'textlen' WITH (OID=1369, DESCRIPTION="character length");

 CREATE FUNCTION "interval"("time") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'time_interval' WITH (OID=1370, DESCRIPTION="convert time to interval");

 CREATE FUNCTION char_length(bpchar) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bpcharlen' WITH (OID=1372, DESCRIPTION="character length");

 CREATE FUNCTION array_type_length_coerce(anyarray, int4, bool) RETURNS anyarray LANGUAGE internal STABLE STRICT AS 'array_type_length_coerce' WITH (OID=1373, DESCRIPTION="coerce array to another type and adjust element typmod");

 CREATE FUNCTION octet_length(text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'textoctetlen' WITH (OID=1374, DESCRIPTION="octet length");

 CREATE FUNCTION octet_length(bpchar) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bpcharoctetlen' WITH (OID=1375, DESCRIPTION="octet length");

 CREATE FUNCTION time_larger("time", "time") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'time_larger' WITH (OID=1377, DESCRIPTION="larger of two");

 CREATE FUNCTION time_smaller("time", "time") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'time_smaller' WITH (OID=1378, DESCRIPTION="smaller of two");

 CREATE FUNCTION timetz_larger(timetz, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_larger' WITH (OID=1379, DESCRIPTION="larger of two");

 CREATE FUNCTION timetz_smaller(timetz, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_smaller' WITH (OID=1380, DESCRIPTION="smaller of two");

 CREATE FUNCTION char_length(text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'textlen' WITH (OID=1381, DESCRIPTION="character length");

 CREATE FUNCTION date_part(text, abstime) RETURNS pg_catalog.float8 LANGUAGE sql STABLE STRICT  AS $$select pg_catalog.date_part($1, cast($2 as timestamp with time zone))$$  WITH (OID=1382, DESCRIPTION="extract field from abstime");

 CREATE FUNCTION date_part(text, reltime) RETURNS pg_catalog.float8 LANGUAGE sql STABLE STRICT AS $$select pg_catalog.date_part($1, cast($2 as pg_catalog.interval))$$  WITH (OID=1383, DESCRIPTION="extract field from reltime");

 CREATE FUNCTION date_part(text, date) RETURNS pg_catalog.float8 LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.date_part($1, cast($2 as timestamp without time zone))$$  WITH (OID=1384, DESCRIPTION="extract field from date");

 CREATE FUNCTION date_part(text, "time") RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'time_part' WITH (OID=1385, DESCRIPTION="extract field from time");

 CREATE FUNCTION age(timestamptz) RETURNS pg_catalog."interval" LANGUAGE sql STABLE STRICT AS $$select pg_catalog.age(cast(current_date as timestamp with time zone), $1)$$  WITH (OID=1386, DESCRIPTION="date difference from today preserving months and years");

 CREATE FUNCTION timetz(timestamptz) RETURNS timetz LANGUAGE internal STABLE STRICT AS 'timestamptz_timetz' WITH (OID=1388, DESCRIPTION="convert timestamptz to timetz");

 CREATE FUNCTION isfinite(timestamptz) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_finite' WITH (OID=1389, DESCRIPTION="finite timestamp?");

 CREATE FUNCTION isfinite("interval") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'interval_finite' WITH (OID=1390, DESCRIPTION="finite interval?");

 CREATE FUNCTION factorial(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_fac' WITH (OID=1376, DESCRIPTION="factorial");

 CREATE FUNCTION abs(float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4abs' WITH (OID=1394, DESCRIPTION="absolute value");

 CREATE FUNCTION abs(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8abs' WITH (OID=1395, DESCRIPTION="absolute value");

 CREATE FUNCTION abs(int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8abs' WITH (OID=1396, DESCRIPTION="absolute value");

 CREATE FUNCTION abs(int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4abs' WITH (OID=1397, DESCRIPTION="absolute value");

 CREATE FUNCTION abs(int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2abs' WITH (OID=1398, DESCRIPTION="absolute value");

-- OIDS 1400 - 1499

 CREATE FUNCTION name("varchar") RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'text_name' WITH (OID=1400, DESCRIPTION="convert varchar to name");

 CREATE FUNCTION "varchar"(name) RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'name_text' WITH (OID=1401, DESCRIPTION="convert name to varchar");

 CREATE FUNCTION "current_schema"() RETURNS name LANGUAGE internal STABLE STRICT AS 'current_schema' WITH (OID=1402, DESCRIPTION="current schema name");

 CREATE FUNCTION current_schemas(bool) RETURNS _name LANGUAGE internal STABLE STRICT AS 'current_schemas' WITH (OID=1403, DESCRIPTION="current schema search list");

 CREATE FUNCTION "overlay"(text, text, int4, int4) RETURNS pg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.substring($1, 1, ($3 - 1)) || $2 || pg_catalog.substring($1, ($3 + $4))$$  WITH (OID=1404, DESCRIPTION="substitute portion of string");

 CREATE FUNCTION "overlay"(text, text, int4) RETURNS pg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.substring($1, 1, ($3 - 1)) || $2 || pg_catalog.substring($1, ($3 + pg_catalog.char_length($2)))$$  WITH (OID=1405, DESCRIPTION="substitute portion of string");

 CREATE FUNCTION isvertical(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_vert' WITH (OID=1406, DESCRIPTION="vertically aligned?");

 CREATE FUNCTION ishorizontal(point, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'point_horiz' WITH (OID=1407, DESCRIPTION="horizontally aligned?");

 CREATE FUNCTION isparallel(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_parallel' WITH (OID=1408, DESCRIPTION="parallel?");

 CREATE FUNCTION isperp(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_perp' WITH (OID=1409, DESCRIPTION="perpendicular?");

 CREATE FUNCTION isvertical(lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_vertical' WITH (OID=1410, DESCRIPTION="vertical?");

 CREATE FUNCTION ishorizontal(lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_horizontal' WITH (OID=1411, DESCRIPTION="horizontal?");

 CREATE FUNCTION isparallel(line, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_parallel' WITH (OID=1412, DESCRIPTION="parallel?");

 CREATE FUNCTION isperp(line, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_perp' WITH (OID=1413, DESCRIPTION="perpendicular?");

 CREATE FUNCTION isvertical(line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_vertical' WITH (OID=1414, DESCRIPTION="vertical?");

 CREATE FUNCTION ishorizontal(line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_horizontal' WITH (OID=1415, DESCRIPTION="horizontal?");

 CREATE FUNCTION point(circle) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'circle_center' WITH (OID=1416, DESCRIPTION="center of");

 CREATE FUNCTION isnottrue(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'isnottrue' WITH (OID=1417, DESCRIPTION="bool is not true (ie, false or unknown)");

 CREATE FUNCTION isnotfalse(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'isnotfalse' WITH (OID=1418, DESCRIPTION="bool is not false (ie, true or unknown)");

 CREATE FUNCTION "time"("interval") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'interval_time' WITH (OID=1419, DESCRIPTION="convert interval to time");

 CREATE FUNCTION box(point, point) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'points_box' WITH (OID=1421, DESCRIPTION="convert points to box");

 CREATE FUNCTION box_add(box, point) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'box_add' WITH (OID=1422, DESCRIPTION="add point to box (translate)");

 CREATE FUNCTION box_sub(box, point) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'box_sub' WITH (OID=1423, DESCRIPTION="subtract point from box (translate)");

 CREATE FUNCTION box_mul(box, point) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'box_mul' WITH (OID=1424, DESCRIPTION="multiply box by point (scale)");

 CREATE FUNCTION box_div(box, point) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'box_div' WITH (OID=1425, DESCRIPTION="divide box by point (scale)");

 CREATE FUNCTION path_contain_pt(path, point) RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.on_ppath($2, $1)$$  WITH (OID=1426, DESCRIPTION="path contains point?");

 CREATE FUNCTION poly_contain_pt(polygon, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_contain_pt' WITH (OID=1428, DESCRIPTION="polygon contains point?");

 CREATE FUNCTION pt_contained_poly(point, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'pt_contained_poly' WITH (OID=1429, DESCRIPTION="point contained in polygon?");

 CREATE FUNCTION isclosed(path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'path_isclosed' WITH (OID=1430, DESCRIPTION="path closed?");

 CREATE FUNCTION isopen(path) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'path_isopen' WITH (OID=1431, DESCRIPTION="path open?");

 CREATE FUNCTION path_npoints(path) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'path_npoints' WITH (OID=1432, DESCRIPTION="number of points in path");

-- pclose and popen might better be named close and open, but that crashes initdb.
-- - thomas 97/04/20


 CREATE FUNCTION pclose(path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_close' WITH (OID=1433, DESCRIPTION="close path");

 CREATE FUNCTION popen(path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_open' WITH (OID=1434, DESCRIPTION="open path");

 CREATE FUNCTION path_add(path, path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_add' WITH (OID=1435, DESCRIPTION="concatenate open paths");

 CREATE FUNCTION path_add_pt(path, point) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_add_pt' WITH (OID=1436, DESCRIPTION="add (translate path)");

 CREATE FUNCTION path_sub_pt(path, point) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_sub_pt' WITH (OID=1437, DESCRIPTION="subtract (translate path)");

 CREATE FUNCTION path_mul_pt(path, point) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_mul_pt' WITH (OID=1438, DESCRIPTION="multiply (rotate/scale path)");

 CREATE FUNCTION path_div_pt(path, point) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_div_pt' WITH (OID=1439, DESCRIPTION="divide (rotate/scale path)");

 CREATE FUNCTION point(float8, float8) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'construct_point' WITH (OID=1440, DESCRIPTION="convert x, y to point");

 CREATE FUNCTION point_add(point, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'point_add' WITH (OID=1441, DESCRIPTION="add points (translate)");

 CREATE FUNCTION point_sub(point, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'point_sub' WITH (OID=1442, DESCRIPTION="subtract points (translate)");

 CREATE FUNCTION point_mul(point, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'point_mul' WITH (OID=1443, DESCRIPTION="multiply points (scale/rotate)");

 CREATE FUNCTION point_div(point, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'point_div' WITH (OID=1444, DESCRIPTION="divide points (scale/rotate)");

 CREATE FUNCTION poly_npoints(polygon) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'poly_npoints' WITH (OID=1445, DESCRIPTION="number of points in polygon");

 CREATE FUNCTION box(polygon) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'poly_box' WITH (OID=1446, DESCRIPTION="convert polygon to bounding box");

 CREATE FUNCTION path(polygon) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'poly_path' WITH (OID=1447, DESCRIPTION="convert polygon to path");

 CREATE FUNCTION polygon(box) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'box_poly' WITH (OID=1448, DESCRIPTION="convert box to polygon");

 CREATE FUNCTION polygon(path) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'path_poly' WITH (OID=1449, DESCRIPTION="convert path to polygon");

 CREATE FUNCTION circle_in(cstring) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'circle_in' WITH (OID=1450, DESCRIPTION="I/O");

 CREATE FUNCTION circle_out(circle) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'circle_out' WITH (OID=1451, DESCRIPTION="I/O");

 CREATE FUNCTION circle_same(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_same' WITH (OID=1452, DESCRIPTION="same as?");

 CREATE FUNCTION circle_contain(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_contain' WITH (OID=1453, DESCRIPTION="contains?");

 CREATE FUNCTION circle_left(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_left' WITH (OID=1454, DESCRIPTION="is left of");

 CREATE FUNCTION circle_overleft(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overleft' WITH (OID=1455, DESCRIPTION="overlaps or is left of");

 CREATE FUNCTION circle_overright(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overright' WITH (OID=1456, DESCRIPTION="overlaps or is right of");

 CREATE FUNCTION circle_right(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_right' WITH (OID=1457, DESCRIPTION="is right of");

 CREATE FUNCTION circle_contained(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_contained' WITH (OID=1458, DESCRIPTION="is contained by?");

 CREATE FUNCTION circle_overlap(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overlap' WITH (OID=1459, DESCRIPTION="overlaps");

 CREATE FUNCTION circle_below(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_below' WITH (OID=1460, DESCRIPTION="is below");

 CREATE FUNCTION circle_above(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_above' WITH (OID=1461, DESCRIPTION="is above");

 CREATE FUNCTION circle_eq(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_eq' WITH (OID=1462, DESCRIPTION="equal by area");

 CREATE FUNCTION circle_ne(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_ne' WITH (OID=1463, DESCRIPTION="not equal by area");

 CREATE FUNCTION circle_lt(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_lt' WITH (OID=1464, DESCRIPTION="less-than by area");

 CREATE FUNCTION circle_gt(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_gt' WITH (OID=1465, DESCRIPTION="greater-than by area");

 CREATE FUNCTION circle_le(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_le' WITH (OID=1466, DESCRIPTION="less-than-or-equal by area");

 CREATE FUNCTION circle_ge(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_ge' WITH (OID=1467, DESCRIPTION="greater-than-or-equal by area");

 CREATE FUNCTION area(circle) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'circle_area' WITH (OID=1468, DESCRIPTION="area of circle");

 CREATE FUNCTION diameter(circle) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'circle_diameter' WITH (OID=1469, DESCRIPTION="diameter of circle");

 CREATE FUNCTION radius(circle) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'circle_radius' WITH (OID=1470, DESCRIPTION="radius of circle");

 CREATE FUNCTION circle_distance(circle, circle) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'circle_distance' WITH (OID=1471, DESCRIPTION="distance between");

 CREATE FUNCTION circle_center(circle) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'circle_center' WITH (OID=1472, DESCRIPTION="center of");

 CREATE FUNCTION circle(point, float8) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'cr_circle' WITH (OID=1473, DESCRIPTION="convert point and radius to circle");

 CREATE FUNCTION circle(polygon) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'poly_circle' WITH (OID=1474, DESCRIPTION="convert polygon to circle");

 CREATE FUNCTION polygon(int4, circle) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'circle_poly' WITH (OID=1475, DESCRIPTION="convert vertex count and circle to polygon");

 CREATE FUNCTION dist_pc(point, circle) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dist_pc' WITH (OID=1476, DESCRIPTION="distance between point and circle");

 CREATE FUNCTION circle_contain_pt(circle, point) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_contain_pt' WITH (OID=1477, DESCRIPTION="circle contains point?");

 CREATE FUNCTION pt_contained_circle(point, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'pt_contained_circle' WITH (OID=1478, DESCRIPTION="point contained in circle?");

 CREATE FUNCTION circle(box) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'box_circle' WITH (OID=1479, DESCRIPTION="convert box to circle");

 CREATE FUNCTION box(circle) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'circle_box' WITH (OID=1480, DESCRIPTION="convert circle to box");

 CREATE FUNCTION tinterval(abstime, abstime) RETURNS tinterval LANGUAGE internal IMMUTABLE STRICT AS 'mktinterval' WITH (OID=1481, DESCRIPTION="convert to tinterval");

 CREATE FUNCTION lseg_ne(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_ne' WITH (OID=1482, DESCRIPTION="not equal");

 CREATE FUNCTION lseg_lt(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_lt' WITH (OID=1483, DESCRIPTION="less-than by length");

 CREATE FUNCTION lseg_le(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_le' WITH (OID=1484, DESCRIPTION="less-than-or-equal by length");

 CREATE FUNCTION lseg_gt(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_gt' WITH (OID=1485, DESCRIPTION="greater-than by length");

 CREATE FUNCTION lseg_ge(lseg, lseg) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lseg_ge' WITH (OID=1486, DESCRIPTION="greater-than-or-equal by length");

 CREATE FUNCTION lseg_length(lseg) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lseg_length' WITH (OID=1487, DESCRIPTION="distance between endpoints");

 CREATE FUNCTION close_ls(line, lseg) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'close_ls' WITH (OID=1488, DESCRIPTION="closest point to line on line segment");

 CREATE FUNCTION close_lseg(lseg, lseg) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'close_lseg' WITH (OID=1489, DESCRIPTION="closest point to line segment on line segment");

 CREATE FUNCTION line_in(cstring) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'line_in' WITH (OID=1490, DESCRIPTION="I/O");

 CREATE FUNCTION line_out(line) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'line_out' WITH (OID=1491, DESCRIPTION="I/O");

 CREATE FUNCTION line_eq(line, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_eq' WITH (OID=1492, DESCRIPTION="lines equal?");

 CREATE FUNCTION line(point, point) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'line_construct_pp' WITH (OID=1493, DESCRIPTION="line from points");

 CREATE FUNCTION line_interpt(line, line) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'line_interpt' WITH (OID=1494, DESCRIPTION="intersection point");

 CREATE FUNCTION line_intersect(line, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_intersect' WITH (OID=1495, DESCRIPTION="intersect?");

 CREATE FUNCTION line_parallel(line, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_parallel' WITH (OID=1496, DESCRIPTION="parallel?");

 CREATE FUNCTION line_perp(line, line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_perp' WITH (OID=1497, DESCRIPTION="perpendicular?");

 CREATE FUNCTION line_vertical(line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_vertical' WITH (OID=1498, DESCRIPTION="vertical?");

 CREATE FUNCTION line_horizontal(line) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'line_horizontal' WITH (OID=1499, DESCRIPTION="horizontal?");

-- OIDS 1500 - 1599

 CREATE FUNCTION length(lseg) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lseg_length' WITH (OID=1530, DESCRIPTION="distance between endpoints");

 CREATE FUNCTION length(path) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'path_length' WITH (OID=1531, DESCRIPTION="sum of path segments");

 CREATE FUNCTION point(lseg) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lseg_center' WITH (OID=1532, DESCRIPTION="center of");

 CREATE FUNCTION point(path) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'path_center' WITH (OID=1533, DESCRIPTION="center of");

 CREATE FUNCTION point(box) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'box_center' WITH (OID=1534, DESCRIPTION="center of");

 CREATE FUNCTION point(polygon) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'poly_center' WITH (OID=1540, DESCRIPTION="center of");

 CREATE FUNCTION lseg(box) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'box_diagonal' WITH (OID=1541, DESCRIPTION="diagonal of");

 CREATE FUNCTION center(box) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'box_center' WITH (OID=1542, DESCRIPTION="center of");

 CREATE FUNCTION center(circle) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'circle_center' WITH (OID=1543, DESCRIPTION="center of");

 CREATE FUNCTION polygon(circle) RETURNS pg_catalog.polygon LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.polygon(12, $1)$$  WITH (OID=1544, DESCRIPTION="convert circle to 12-vertex polygon");

 CREATE FUNCTION npoints(path) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'path_npoints' WITH (OID=1545, DESCRIPTION="number of points in path");

 CREATE FUNCTION npoints(polygon) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'poly_npoints' WITH (OID=1556, DESCRIPTION="number of points in polygon");

 CREATE FUNCTION bit_in(cstring, oid, int4) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bit_in' WITH (OID=1564, DESCRIPTION="I/O");

 CREATE FUNCTION bit_out("bit") RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'bit_out' WITH (OID=1565, DESCRIPTION="I/O");

 CREATE FUNCTION "like"(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textlike' WITH (OID=1569, DESCRIPTION="matches LIKE expression");

 CREATE FUNCTION notlike(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textnlike' WITH (OID=1570, DESCRIPTION="does not match LIKE expression");

 CREATE FUNCTION "like"(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namelike' WITH (OID=1571, DESCRIPTION="matches LIKE expression");

 CREATE FUNCTION notlike(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'namenlike' WITH (OID=1572, DESCRIPTION="does not match LIKE expression");

-- SEQUENCE functions
 CREATE FUNCTION nextval(regclass) RETURNS int8 LANGUAGE internal VOLATILE STRICT AS 'nextval_oid' WITH (OID=1574, DESCRIPTION="sequence next value");
-- #define NEXTVAL_FUNC_OID 1574

 CREATE FUNCTION currval(regclass) RETURNS int8 LANGUAGE internal VOLATILE STRICT AS 'currval_oid' WITH (OID=1575, DESCRIPTION="sequence current value");
-- #define CURRVAL_FUNC_OID 1575

 CREATE FUNCTION setval(regclass, int8) RETURNS int8 LANGUAGE internal VOLATILE STRICT AS 'setval_oid' WITH (OID=1576, DESCRIPTION="set sequence value");
-- #define SETVAL_FUNC_OID 1576

 CREATE FUNCTION setval(regclass, int8, bool) RETURNS int8 LANGUAGE internal VOLATILE STRICT AS 'setval3_oid' WITH (OID=1765, DESCRIPTION="set sequence value and iscalled status");

 CREATE FUNCTION varbit_in(cstring, oid, int4) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'varbit_in' WITH (OID=1579, DESCRIPTION="I/O");

 CREATE FUNCTION varbit_out(varbit) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'varbit_out' WITH (OID=1580, DESCRIPTION="I/O");

 CREATE FUNCTION biteq("bit", "bit") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'biteq' WITH (OID=1581, DESCRIPTION="equal");

 CREATE FUNCTION bitne("bit", "bit") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitne' WITH (OID=1582, DESCRIPTION="not equal");

 CREATE FUNCTION bitge("bit", "bit") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitge' WITH (OID=1592, DESCRIPTION="greater than or equal");

 CREATE FUNCTION bitgt("bit", "bit") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitgt' WITH (OID=1593, DESCRIPTION="greater than");

 CREATE FUNCTION bitle("bit", "bit") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitle' WITH (OID=1594, DESCRIPTION="less than or equal");

 CREATE FUNCTION bitlt("bit", "bit") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitlt' WITH (OID=1595, DESCRIPTION="less than");

 CREATE FUNCTION bitcmp("bit", "bit") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bitcmp' WITH (OID=1596, DESCRIPTION="compare");

 CREATE FUNCTION random() RETURNS float8 LANGUAGE internal VOLATILE STRICT AS 'drandom' WITH (OID=1598, DESCRIPTION="random value");

 CREATE FUNCTION setseed(float8) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'setseed' WITH (OID=1599, DESCRIPTION="set random seed");

-- OIDS 1600 - 1699

 CREATE FUNCTION asin(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dasin' WITH (OID=1600, DESCRIPTION="arcsine");

 CREATE FUNCTION acos(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dacos' WITH (OID=1601, DESCRIPTION="arccosine");

 CREATE FUNCTION atan(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'datan' WITH (OID=1602, DESCRIPTION="arctangent");

 CREATE FUNCTION atan2(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'datan2' WITH (OID=1603, DESCRIPTION="arctangent, two arguments");

 CREATE FUNCTION sin(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dsin' WITH (OID=1604, DESCRIPTION="sine");

 CREATE FUNCTION cos(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dcos' WITH (OID=1605, DESCRIPTION="cosine");

 CREATE FUNCTION tan(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dtan' WITH (OID=1606, DESCRIPTION="tangent");

 CREATE FUNCTION cot(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dcot' WITH (OID=1607, DESCRIPTION="cotangent");

 CREATE FUNCTION degrees(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'degrees' WITH (OID=1608, DESCRIPTION="radians to degrees");

 CREATE FUNCTION radians(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'radians' WITH (OID=1609, DESCRIPTION="degrees to radians");

 CREATE FUNCTION pi() RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'dpi' WITH (OID=1610, DESCRIPTION="PI");

 CREATE FUNCTION interval_mul("interval", float8) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_mul' WITH (OID=1618, DESCRIPTION="multiply interval");

 CREATE FUNCTION ascii(text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'ascii' WITH (OID=1620, DESCRIPTION="convert first char to int4");

 CREATE FUNCTION chr(int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'chr' WITH (OID=1621, DESCRIPTION="convert int4 to char");

 CREATE FUNCTION repeat(text, int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'repeat' WITH (OID=1622, DESCRIPTION="replicate string int4 times");

 CREATE FUNCTION similar_escape(text, text) RETURNS text LANGUAGE internal IMMUTABLE AS 'similar_escape' WITH (OID=1623, DESCRIPTION="convert SQL99 regexp pattern to POSIX style");

 CREATE FUNCTION mul_d_interval(float8, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'mul_d_interval' WITH (OID=1624);

 CREATE FUNCTION bpcharlike(bpchar, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textlike' WITH (OID=1631, DESCRIPTION="matches LIKE expression");

 CREATE FUNCTION bpcharnlike(bpchar, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textnlike' WITH (OID=1632, DESCRIPTION="does not match LIKE expression");

 CREATE FUNCTION texticlike(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texticlike' WITH (OID=1633, DESCRIPTION="matches LIKE expression, case-insensitive");

 CREATE FUNCTION texticnlike(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texticnlike' WITH (OID=1634, DESCRIPTION="does not match LIKE expression, case-insensitive");

 CREATE FUNCTION nameiclike(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'nameiclike' WITH (OID=1635, DESCRIPTION="matches LIKE expression, case-insensitive");

 CREATE FUNCTION nameicnlike(name, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'nameicnlike' WITH (OID=1636, DESCRIPTION="does not match LIKE expression, case-insensitive");

 CREATE FUNCTION like_escape(text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'like_escape' WITH (OID=1637, DESCRIPTION="convert LIKE pattern to use backslash escapes");

 CREATE FUNCTION bpcharicregexeq(bpchar, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texticregexeq' WITH (OID=1656, DESCRIPTION="matches regex., case-insensitive");

 CREATE FUNCTION bpcharicregexne(bpchar, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texticregexne' WITH (OID=1657, DESCRIPTION="does not match regex., case-insensitive");

 CREATE FUNCTION bpcharregexeq(bpchar, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textregexeq' WITH (OID=1658, DESCRIPTION="matches regex., case-sensitive");

 CREATE FUNCTION bpcharregexne(bpchar, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textregexne' WITH (OID=1659, DESCRIPTION="does not match regex., case-sensitive");

 CREATE FUNCTION bpchariclike(bpchar, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texticlike' WITH (OID=1660, DESCRIPTION="matches LIKE expression, case-insensitive");

 CREATE FUNCTION bpcharicnlike(bpchar, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texticnlike' WITH (OID=1661, DESCRIPTION="does not match LIKE expression, case-insensitive");

 CREATE FUNCTION flatfile_update_trigger() RETURNS trigger LANGUAGE internal VOLATILE STRICT AS 'flatfile_update_trigger' WITH (OID=1689, DESCRIPTION="update flat-file copy of a shared catalog");

-- Oracle Compatibility Related Functions - By Edmund Mergl <E.Mergl@bawue.de> 
 CREATE FUNCTION strpos(text, text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'textpos' WITH (OID=868, DESCRIPTION="find position of substring");

 CREATE FUNCTION lower(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'lower' WITH (OID=870, DESCRIPTION="lowercase");

 CREATE FUNCTION upper(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'upper' WITH (OID=871, DESCRIPTION="uppercase");

 CREATE FUNCTION initcap(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'initcap' WITH (OID=872, DESCRIPTION="capitalize each word");

 CREATE FUNCTION lpad(text, int4, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'lpad' WITH (OID=873, DESCRIPTION="left-pad string to length");

 CREATE FUNCTION rpad(text, int4, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'rpad' WITH (OID=874, DESCRIPTION="right-pad string to length");

 CREATE FUNCTION ltrim(text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'ltrim' WITH (OID=875, DESCRIPTION="trim selected characters from left end of string");

 CREATE FUNCTION rtrim(text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'rtrim' WITH (OID=876, DESCRIPTION="trim selected characters from right end of string");

 CREATE FUNCTION substr(text, int4, int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'text_substr' WITH (OID=877, DESCRIPTION="return portion of string");

 CREATE FUNCTION translate(text, text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'translate' WITH (OID=878, DESCRIPTION="map a set of character appearing in string");

 CREATE FUNCTION lpad(text, int4) RETURNS pg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.lpad($1, $2, ' ')$$  WITH (OID=879, DESCRIPTION="left-pad string to length");

 CREATE FUNCTION rpad(text, int4) RETURNS pg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.rpad($1, $2, ' ')$$  WITH (OID=880, DESCRIPTION="right-pad string to length");

 CREATE FUNCTION ltrim(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'ltrim1' WITH (OID=881, DESCRIPTION="trim spaces from left end of string");

 CREATE FUNCTION rtrim(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'rtrim1' WITH (OID=882, DESCRIPTION="trim spaces from right end of string");

 CREATE FUNCTION substr(text, int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'text_substr_no_len' WITH (OID=883, DESCRIPTION="return portion of string");

 CREATE FUNCTION btrim(text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'btrim' WITH (OID=884, DESCRIPTION="trim selected characters from both ends of string");

 CREATE FUNCTION btrim(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'btrim1' WITH (OID=885, DESCRIPTION="trim spaces from both ends of string");

 CREATE FUNCTION "substring"(text, int4, int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'text_substr' WITH (OID=936, DESCRIPTION="return portion of string");

 CREATE FUNCTION "substring"(text, int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'text_substr_no_len' WITH (OID=937, DESCRIPTION="return portion of string");

 CREATE FUNCTION replace(text, text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'replace_text' WITH (OID=2087, DESCRIPTION="replace all occurrences of old_substr with new_substr in string");

 CREATE FUNCTION regexp_replace(text, text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'textregexreplace_noopt' WITH (OID=2284, DESCRIPTION="replace text using regexp");

 CREATE FUNCTION regexp_replace(text, text, text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'textregexreplace' WITH (OID=2285, DESCRIPTION="replace text using regexp");

 CREATE FUNCTION regexp_matches(text, text) RETURNS SETOF _text LANGUAGE internal IMMUTABLE STRICT AS 'regexp_matches_no_flags' WITH (OID=5018, DESCRIPTION="return all match groups for regexp");

 CREATE FUNCTION regexp_matches(text, text, text) RETURNS SETOF _text LANGUAGE internal IMMUTABLE STRICT AS 'regexp_matches' WITH (OID=5019, DESCRIPTION="return all match groups for regexp");

 CREATE FUNCTION regexp_split_to_table(text, text) RETURNS SETOF text LANGUAGE internal IMMUTABLE STRICT AS 'regexp_split_to_table_no_flags' WITH (OID=5020, DESCRIPTION="split string by pattern");

 CREATE FUNCTION regexp_split_to_table(text, text, text) RETURNS SETOF text LANGUAGE internal IMMUTABLE STRICT AS 'regexp_split_to_table' WITH (OID=5021, DESCRIPTION="split string by pattern");

 CREATE FUNCTION regexp_split_to_array(text, text) RETURNS _text LANGUAGE internal IMMUTABLE STRICT AS 'regexp_split_to_array_no_flags' WITH (OID=5022, DESCRIPTION="split string by pattern");

 CREATE FUNCTION regexp_split_to_array(text, text, text) RETURNS _text LANGUAGE internal IMMUTABLE STRICT AS 'regexp_split_to_array' WITH (OID=5023, DESCRIPTION="split string by pattern");

 CREATE FUNCTION split_part(text, text, int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'split_text' WITH (OID=2088, DESCRIPTION="split string by field_sep and return field_num");

 CREATE FUNCTION to_hex(int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'to_hex32' WITH (OID=2089, DESCRIPTION="convert int4 number to hex");

 CREATE FUNCTION to_hex(int8) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'to_hex64' WITH (OID=2090, DESCRIPTION="convert int8 number to hex");

--  for character set encoding support 

-- return database encoding name 
 CREATE FUNCTION getdatabaseencoding() RETURNS name LANGUAGE internal STABLE STRICT AS 'getdatabaseencoding' WITH (OID=1039, DESCRIPTION="encoding name of current database");

-- return client encoding name i.e. session encoding 
 CREATE FUNCTION pg_client_encoding() RETURNS name LANGUAGE internal STABLE STRICT AS 'pg_client_encoding' WITH (OID=810, DESCRIPTION="encoding name of current database");

 CREATE FUNCTION "convert"(text, name) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_convert' WITH (OID=1717, DESCRIPTION="convert string with specified destination encoding name");

 CREATE FUNCTION "convert"(text, name, name) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_convert2' WITH (OID=1813, DESCRIPTION="convert string with specified encoding names");

 CREATE FUNCTION convert_using(text, text) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_convert_using' WITH (OID=1619, DESCRIPTION="convert string with specified conversion name");

 CREATE FUNCTION pg_char_to_encoding(name) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'PG_char_to_encoding' WITH (OID=1264, DESCRIPTION="convert encoding name to encoding id");

 CREATE FUNCTION pg_encoding_to_char(int4) RETURNS name LANGUAGE internal STABLE STRICT AS 'PG_encoding_to_char' WITH (OID=1597, DESCRIPTION="convert encoding id to encoding name");

 CREATE FUNCTION oidgt(oid, oid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidgt' WITH (OID=1638, DESCRIPTION="greater-than");

 CREATE FUNCTION oidge(oid, oid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'oidge' WITH (OID=1639, DESCRIPTION="greater-than-or-equal");

-- System-view support functions 
 CREATE FUNCTION pg_get_ruledef(oid) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_ruledef' WITH (OID=1573, DESCRIPTION="source text of a rule");

 CREATE FUNCTION pg_get_viewdef(text) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_viewdef_name' WITH (OID=1640, DESCRIPTION="select statement of a view");

 CREATE FUNCTION pg_get_viewdef(oid) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_viewdef' WITH (OID=1641, DESCRIPTION="select statement of a view");

 CREATE FUNCTION pg_get_userbyid(oid) RETURNS name LANGUAGE internal STABLE STRICT AS 'pg_get_userbyid' WITH (OID=1642, DESCRIPTION="role name by OID (with fallback)");

 CREATE FUNCTION pg_get_indexdef(oid) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_indexdef' WITH (OID=1643, DESCRIPTION="index description");

 CREATE FUNCTION pg_get_triggerdef(oid) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_triggerdef' WITH (OID=1662, DESCRIPTION="trigger description");

 CREATE FUNCTION pg_get_constraintdef(oid) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_constraintdef' WITH (OID=1387, DESCRIPTION="constraint description");

 CREATE FUNCTION pg_get_expr(text, oid) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_expr' WITH (OID=1716, DESCRIPTION="deparse an encoded expression");

 CREATE FUNCTION pg_get_serial_sequence(text, text) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_serial_sequence' WITH (OID=1665, DESCRIPTION="name of sequence for a serial column");

 CREATE FUNCTION pg_get_partition_def(oid) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_partition_def' WITH (OID=5024);

 CREATE FUNCTION pg_get_partition_def(oid, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_partition_def_ext' WITH (OID=5025, DESCRIPTION="partition configuration for a given relation");

 CREATE FUNCTION pg_get_partition_def(oid, bool, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_partition_def_ext2' WITH (OID=5034, DESCRIPTION="partition configuration for a given relation");

 CREATE FUNCTION pg_get_partition_rule_def(oid) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_partition_rule_def' WITH (OID=5027);

 CREATE FUNCTION pg_get_partition_rule_def(oid, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_partition_rule_def_ext' WITH (OID=5028, DESCRIPTION="partition configuration for a given rule");

 CREATE FUNCTION pg_get_partition_template_def(oid, bool, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_partition_template_def' WITH (OID=5037, DESCRIPTION="ALTER statement to recreate subpartition templates for a give relation");

 CREATE FUNCTION pg_get_keywords(OUT word text, OUT catcode "char", OUT catdesc text) RETURNS SETOF pg_catalog.record LANGUAGE internal STABLE STRICT AS 'pg_get_keywords' WITH (OID=821, DESCRIPTION="list of SQL keywords");

 CREATE FUNCTION pg_typeof("any") RETURNS regtype LANGUAGE internal STABLE AS 'pg_typeof' WITH (OID=822, DESCRIPTION="returns the type of the argument");

-- Generic referential integrity constraint triggers 
 CREATE FUNCTION "RI_FKey_check_ins"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_check_ins' WITH (OID=1644, DESCRIPTION="referential integrity FOREIGN KEY ... REFERENCES");

 CREATE FUNCTION "RI_FKey_check_upd"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_check_upd' WITH (OID=1645, DESCRIPTION="referential integrity FOREIGN KEY ... REFERENCES");

 CREATE FUNCTION "RI_FKey_cascade_del"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_cascade_del' WITH (OID=1646, DESCRIPTION="referential integrity ON DELETE CASCADE");

 CREATE FUNCTION "RI_FKey_cascade_upd"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_cascade_upd' WITH (OID=1647, DESCRIPTION="referential integrity ON UPDATE CASCADE");

 CREATE FUNCTION "RI_FKey_restrict_del"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_restrict_del' WITH (OID=1648, DESCRIPTION="referential integrity ON DELETE RESTRICT");

 CREATE FUNCTION "RI_FKey_restrict_upd"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_restrict_upd' WITH (OID=1649, DESCRIPTION="referential integrity ON UPDATE RESTRICT");

 CREATE FUNCTION "RI_FKey_setnull_del"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_setnull_del' WITH (OID=1650, DESCRIPTION="referential integrity ON DELETE SET NULL");

 CREATE FUNCTION "RI_FKey_setnull_upd"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_setnull_upd' WITH (OID=1651, DESCRIPTION="referential integrity ON UPDATE SET NULL");

 CREATE FUNCTION "RI_FKey_setdefault_del"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_setdefault_del' WITH (OID=1652, DESCRIPTION="referential integrity ON DELETE SET DEFAULT");

 CREATE FUNCTION "RI_FKey_setdefault_upd"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_setdefault_upd' WITH (OID=1653, DESCRIPTION="referential integrity ON UPDATE SET DEFAULT");

 CREATE FUNCTION "RI_FKey_noaction_del"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_noaction_del' WITH (OID=1654, DESCRIPTION="referential integrity ON DELETE NO ACTION");

 CREATE FUNCTION "RI_FKey_noaction_upd"() RETURNS trigger LANGUAGE internal VOLATILE STRICT MODIFIES SQL DATA AS 'RI_FKey_noaction_upd' WITH (OID=1655, DESCRIPTION="referential integrity ON UPDATE NO ACTION");

 CREATE FUNCTION varbiteq(varbit, varbit) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'biteq' WITH (OID=1666, DESCRIPTION="equal");

 CREATE FUNCTION varbitne(varbit, varbit) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitne' WITH (OID=1667, DESCRIPTION="not equal");

 CREATE FUNCTION varbitge(varbit, varbit) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitge' WITH (OID=1668, DESCRIPTION="greater than or equal");

 CREATE FUNCTION varbitgt(varbit, varbit) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitgt' WITH (OID=1669, DESCRIPTION="greater than");

 CREATE FUNCTION varbitle(varbit, varbit) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitle' WITH (OID=1670, DESCRIPTION="less than or equal");

 CREATE FUNCTION varbitlt(varbit, varbit) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bitlt' WITH (OID=1671, DESCRIPTION="less than");

 CREATE FUNCTION varbitcmp(varbit, varbit) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bitcmp' WITH (OID=1672, DESCRIPTION="compare");

 CREATE FUNCTION bitand("bit", "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitand' WITH (OID=1673, DESCRIPTION="bitwise and");

 CREATE FUNCTION bitor("bit", "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitor' WITH (OID=1674, DESCRIPTION="bitwise or");

 CREATE FUNCTION bitxor("bit", "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitxor' WITH (OID=1675, DESCRIPTION="bitwise exclusive or");

 CREATE FUNCTION bitnot("bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitnot' WITH (OID=1676, DESCRIPTION="bitwise not");

 CREATE FUNCTION bitshiftleft("bit", int4) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitshiftleft' WITH (OID=1677, DESCRIPTION="bitwise left shift");

 CREATE FUNCTION bitshiftright("bit", int4) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitshiftright' WITH (OID=1678, DESCRIPTION="bitwise right shift");

 CREATE FUNCTION bitcat("bit", "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitcat' WITH (OID=1679, DESCRIPTION="bitwise concatenation");

 CREATE FUNCTION "substring"("bit", int4, int4) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitsubstr' WITH (OID=1680, DESCRIPTION="return portion of bitstring");

 CREATE FUNCTION length("bit") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bitlength' WITH (OID=1681, DESCRIPTION="bitstring length");

 CREATE FUNCTION octet_length("bit") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bitoctetlength' WITH (OID=1682, DESCRIPTION="octet length");

 CREATE FUNCTION "bit"(int4, int4) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitfromint4' WITH (OID=1683, DESCRIPTION="int4 to bitstring");

 CREATE FUNCTION int4("bit") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bittoint4' WITH (OID=1684, DESCRIPTION="bitstring to int4");

 CREATE FUNCTION "bit"("bit", int4, bool) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bit' WITH (OID=1685, DESCRIPTION="adjust bit() to typmod length");

 CREATE FUNCTION varbit(varbit, int4, bool) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'varbit' WITH (OID=1687, DESCRIPTION="adjust varbit() to typmod length");

 CREATE FUNCTION "position"("bit", "bit") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bitposition' WITH (OID=1698, DESCRIPTION="return position of sub-bitstring");

 CREATE FUNCTION "substring"("bit", int4) RETURNS pg_catalog."bit" LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.substring($1, $2, -1)$$  WITH (OID=1699, DESCRIPTION="return portion of bitstring");

-- for mac type support 
 CREATE FUNCTION macaddr_in(cstring) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_in' WITH (OID=436, DESCRIPTION="I/O");

 CREATE FUNCTION macaddr_out(macaddr) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_out' WITH (OID=437, DESCRIPTION="I/O");

 CREATE FUNCTION text(macaddr) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_text' WITH (OID=752, DESCRIPTION="MAC address to text");

 CREATE FUNCTION trunc(macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_trunc' WITH (OID=753, DESCRIPTION="MAC manufacturer fields");

 CREATE FUNCTION macaddr(text) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'text_macaddr' WITH (OID=767, DESCRIPTION="text to MAC address");

 CREATE FUNCTION macaddr_eq(macaddr, macaddr) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_eq' WITH (OID=830, DESCRIPTION="equal");

 CREATE FUNCTION macaddr_lt(macaddr, macaddr) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_lt' WITH (OID=831, DESCRIPTION="less-than");

 CREATE FUNCTION macaddr_le(macaddr, macaddr) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_le' WITH (OID=832, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION macaddr_gt(macaddr, macaddr) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_gt' WITH (OID=833, DESCRIPTION="greater-than");

 CREATE FUNCTION macaddr_ge(macaddr, macaddr) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_ge' WITH (OID=834, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION macaddr_ne(macaddr, macaddr) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_ne' WITH (OID=835, DESCRIPTION="not equal");

 CREATE FUNCTION macaddr_cmp(macaddr, macaddr) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_cmp' WITH (OID=836, DESCRIPTION="less-equal-greater");

-- for inet type support 
 CREATE FUNCTION inet_in(cstring) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'inet_in' WITH (OID=910, DESCRIPTION="I/O");

 CREATE FUNCTION inet_out(inet) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'inet_out' WITH (OID=911, DESCRIPTION="I/O");

-- for cidr type support 
 CREATE FUNCTION cidr_in(cstring) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'cidr_in' WITH (OID=1267, DESCRIPTION="I/O");

 CREATE FUNCTION cidr_out(cidr) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'cidr_out' WITH (OID=1427, DESCRIPTION="I/O");


-- these are used for both inet and cidr 
 CREATE FUNCTION network_eq(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_eq' WITH (OID=920, DESCRIPTION="equal");

 CREATE FUNCTION network_lt(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_lt' WITH (OID=921, DESCRIPTION="less-than");

 CREATE FUNCTION network_le(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_le' WITH (OID=922, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION network_gt(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_gt' WITH (OID=923, DESCRIPTION="greater-than");

 CREATE FUNCTION network_ge(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_ge' WITH (OID=924, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION network_ne(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_ne' WITH (OID=925, DESCRIPTION="not equal");

 CREATE FUNCTION network_cmp(inet, inet) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'network_cmp' WITH (OID=926, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION network_sub(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_sub' WITH (OID=927, DESCRIPTION="is-subnet");

 CREATE FUNCTION network_subeq(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_subeq' WITH (OID=928, DESCRIPTION="is-subnet-or-equal");

 CREATE FUNCTION network_sup(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_sup' WITH (OID=929, DESCRIPTION="is-supernet");

 CREATE FUNCTION network_supeq(inet, inet) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'network_supeq' WITH (OID=930, DESCRIPTION="is-supernet-or-equal");

-- inet/cidr functions 
 CREATE FUNCTION abbrev(inet) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'inet_abbrev' WITH (OID=598, DESCRIPTION="abbreviated display of inet value");

 CREATE FUNCTION abbrev(cidr) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'cidr_abbrev' WITH (OID=599, DESCRIPTION="abbreviated display of cidr value");

 CREATE FUNCTION set_masklen(inet, int4) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'inet_set_masklen' WITH (OID=605, DESCRIPTION="change netmask of inet");

 CREATE FUNCTION set_masklen(cidr, int4) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'cidr_set_masklen' WITH (OID=635, DESCRIPTION="change netmask of cidr");

 CREATE FUNCTION family(inet) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'network_family' WITH (OID=711, DESCRIPTION="address family (4 for IPv4, 6 for IPv6)");

 CREATE FUNCTION network(inet) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'network_network' WITH (OID=683, DESCRIPTION="network part of address");

 CREATE FUNCTION netmask(inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'network_netmask' WITH (OID=696, DESCRIPTION="netmask of address");

 CREATE FUNCTION masklen(inet) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'network_masklen' WITH (OID=697, DESCRIPTION="netmask length");

 CREATE FUNCTION broadcast(inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'network_broadcast' WITH (OID=698, DESCRIPTION="broadcast address of network");

 CREATE FUNCTION "host"(inet) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'network_host' WITH (OID=699, DESCRIPTION="show address octets only");

 CREATE FUNCTION text(inet) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'network_show' WITH (OID=730, DESCRIPTION="show all parts of inet/cidr value");

 CREATE FUNCTION hostmask(inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'network_hostmask' WITH (OID=1362, DESCRIPTION="hostmask of address");

 CREATE FUNCTION inet(text) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'text_inet' WITH (OID=1713, DESCRIPTION="text to inet");

 CREATE FUNCTION cidr(text) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'text_cidr' WITH (OID=1714, DESCRIPTION="text to cidr");

 CREATE FUNCTION cidr(inet) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'inet_to_cidr' WITH (OID=1715, DESCRIPTION="coerce inet to cidr");

 CREATE FUNCTION inet_client_addr() RETURNS inet LANGUAGE internal STABLE AS 'inet_client_addr' WITH (OID=2196, DESCRIPTION="inet address of the client");

 CREATE FUNCTION inet_client_port() RETURNS int4 LANGUAGE internal STABLE AS 'inet_client_port' WITH (OID=2197, DESCRIPTION="client's port number for this connection");

 CREATE FUNCTION inet_server_addr() RETURNS inet LANGUAGE internal STABLE AS 'inet_server_addr' WITH (OID=2198, DESCRIPTION="inet address of the server");

 CREATE FUNCTION inet_server_port() RETURNS int4 LANGUAGE internal STABLE AS 'inet_server_port' WITH (OID=2199, DESCRIPTION="server's port number for this connection");

 CREATE FUNCTION inetnot(inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'inetnot' WITH (OID=2627, DESCRIPTION="bitwise not");

 CREATE FUNCTION inetand(inet, inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'inetand' WITH (OID=2628, DESCRIPTION="bitwise and");

 CREATE FUNCTION inetor(inet, inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'inetor' WITH (OID=2629, DESCRIPTION="bitwise or");

 CREATE FUNCTION inetpl(inet, int8) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'inetpl' WITH (OID=2630, DESCRIPTION="add integer to inet value");

 CREATE FUNCTION int8pl_inet(int8, inet) RETURNS pg_catalog.inet LANGUAGE sql IMMUTABLE STRICT AS $$select $2 + $1$$  WITH (OID=2631, DESCRIPTION="add integer to inet value");

 CREATE FUNCTION inetmi_int8(inet, int8) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'inetmi_int8' WITH (OID=2632, DESCRIPTION="subtract integer from inet value");

 CREATE FUNCTION inetmi(inet, inet) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'inetmi' WITH (OID=2633, DESCRIPTION="subtract inet values");

 CREATE FUNCTION "numeric"(text) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'text_numeric' WITH (OID=1686, DESCRIPTION="(internal)");

 CREATE FUNCTION text("numeric") RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'numeric_text' WITH (OID=1688, DESCRIPTION="(internal)");

 CREATE FUNCTION time_mi_time("time", "time") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'time_mi_time' WITH (OID=1690, DESCRIPTION="minus");

 CREATE FUNCTION boolle(bool, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'boolle' WITH (OID=1691, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION boolge(bool, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'boolge' WITH (OID=1692, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION btboolcmp(bool, bool) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btboolcmp' WITH (OID=1693, DESCRIPTION="btree less-equal-greater");

 CREATE FUNCTION timetz_hash(timetz) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'timetz_hash' WITH (OID=1696, DESCRIPTION="hash");

 CREATE FUNCTION interval_hash("interval") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'interval_hash' WITH (OID=1697, DESCRIPTION="hash");

-- OID's 1700 - 1799 NUMERIC data type 
 CREATE FUNCTION numeric_in(cstring, oid, int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_in' WITH (OID=1701, DESCRIPTION="I/O");

 CREATE FUNCTION numeric_out("numeric") RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'numeric_out' WITH (OID=1702, DESCRIPTION="I/O");

 CREATE FUNCTION "numeric"("numeric", int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric' WITH (OID=1703, DESCRIPTION="adjust numeric to typmod precision/scale");

 CREATE FUNCTION numeric_abs("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_abs' WITH (OID=1704, DESCRIPTION="absolute value");

 CREATE FUNCTION abs("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_abs' WITH (OID=1705, DESCRIPTION="absolute value");

 CREATE FUNCTION sign("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_sign' WITH (OID=1706, DESCRIPTION="sign of value");

 CREATE FUNCTION round("numeric", int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_round' WITH (OID=1707, DESCRIPTION="value rounded to 'scale'");

 CREATE FUNCTION round("numeric") RETURNS pg_catalog."numeric" LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.round($1,0)$$  WITH (OID=1708, DESCRIPTION="value rounded to 'scale' of zero");

 CREATE FUNCTION trunc("numeric", int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_trunc' WITH (OID=1709, DESCRIPTION="value truncated to 'scale'");

 CREATE FUNCTION trunc("numeric") RETURNS pg_catalog."numeric" LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.trunc($1,0)$$  WITH (OID=1710, DESCRIPTION="value truncated to 'scale' of zero");

 CREATE FUNCTION ceil("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_ceil' WITH (OID=1711, DESCRIPTION="smallest integer >= value");

 CREATE FUNCTION ceiling("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_ceil' WITH (OID=2167, DESCRIPTION="smallest integer >= value");

 CREATE FUNCTION floor("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_floor' WITH (OID=1712, DESCRIPTION="largest integer <= value");

 CREATE FUNCTION numeric_eq("numeric", "numeric") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_eq' WITH (OID=1718, DESCRIPTION="equal");

 CREATE FUNCTION numeric_ne("numeric", "numeric") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_ne' WITH (OID=1719, DESCRIPTION="not equal");

 CREATE FUNCTION numeric_gt("numeric", "numeric") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_gt' WITH (OID=1720, DESCRIPTION="greater-than");

 CREATE FUNCTION numeric_ge("numeric", "numeric") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_ge' WITH (OID=1721, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION numeric_lt("numeric", "numeric") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_lt' WITH (OID=1722, DESCRIPTION="less-than");

 CREATE FUNCTION numeric_le("numeric", "numeric") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'numeric_le' WITH (OID=1723, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION numeric_add("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_add' WITH (OID=1724, DESCRIPTION="add");

 CREATE FUNCTION numeric_sub("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_sub' WITH (OID=1725, DESCRIPTION="subtract");

 CREATE FUNCTION numeric_mul("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_mul' WITH (OID=1726, DESCRIPTION="multiply");

 CREATE FUNCTION numeric_div("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_div' WITH (OID=1727, DESCRIPTION="divide");

 CREATE FUNCTION mod("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_mod' WITH (OID=1728, DESCRIPTION="modulus");

 CREATE FUNCTION numeric_mod("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_mod' WITH (OID=1729, DESCRIPTION="modulus");

 CREATE FUNCTION sqrt("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_sqrt' WITH (OID=1730, DESCRIPTION="square root");

 CREATE FUNCTION numeric_sqrt("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_sqrt' WITH (OID=1731, DESCRIPTION="square root");

 CREATE FUNCTION exp("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_exp' WITH (OID=1732, DESCRIPTION="e raised to the power of n");

 CREATE FUNCTION numeric_exp("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_exp' WITH (OID=1733, DESCRIPTION="e raised to the power of n");

 CREATE FUNCTION ln("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_ln' WITH (OID=1734, DESCRIPTION="natural logarithm of n");

 CREATE FUNCTION numeric_ln("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_ln' WITH (OID=1735, DESCRIPTION="natural logarithm of n");

 CREATE FUNCTION "log"("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_log' WITH (OID=1736, DESCRIPTION="logarithm base m of n");

 CREATE FUNCTION numeric_log("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_log' WITH (OID=1737, DESCRIPTION="logarithm base m of n");

 CREATE FUNCTION pow("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_power' WITH (OID=1738, DESCRIPTION="m raised to the power of n");

 CREATE FUNCTION power("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_power' WITH (OID=2169, DESCRIPTION="m raised to the power of n");

 CREATE FUNCTION numeric_power("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_power' WITH (OID=1739, DESCRIPTION="m raised to the power of n");

 CREATE FUNCTION "numeric"(int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'int4_numeric' WITH (OID=1740, DESCRIPTION="(internal)");

 CREATE FUNCTION "log"("numeric") RETURNS pg_catalog."numeric" LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.log(10, $1)$$  WITH (OID=1741, DESCRIPTION="logarithm base 10 of n");

 CREATE FUNCTION "numeric"(float4) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'float4_numeric' WITH (OID=1742, DESCRIPTION="(internal)");

 CREATE FUNCTION "numeric"(float8) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'float8_numeric' WITH (OID=1743, DESCRIPTION="(internal)");

 CREATE FUNCTION int4("numeric") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_int4' WITH (OID=1744, DESCRIPTION="(internal)");

 CREATE FUNCTION float4("numeric") RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_float4' WITH (OID=1745, DESCRIPTION="(internal)");

 CREATE FUNCTION float8("numeric") RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_float8' WITH (OID=1746, DESCRIPTION="(internal)");

 CREATE FUNCTION width_bucket("numeric", "numeric", "numeric", int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'width_bucket_numeric' WITH (OID=2170, DESCRIPTION="bucket number of operand in equidepth histogram");

 CREATE FUNCTION time_pl_interval("time", "interval") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'time_pl_interval' WITH (OID=1747, DESCRIPTION="plus");

 CREATE FUNCTION time_mi_interval("time", "interval") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'time_mi_interval' WITH (OID=1748, DESCRIPTION="minus");

 CREATE FUNCTION timetz_pl_interval(timetz, "interval") RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_pl_interval' WITH (OID=1749, DESCRIPTION="plus");

 CREATE FUNCTION timetz_mi_interval(timetz, "interval") RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_mi_interval' WITH (OID=1750, DESCRIPTION="minus");

 CREATE FUNCTION numeric_inc("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_inc' WITH (OID=1764, DESCRIPTION="increment by one");

 CREATE FUNCTION numeric_dec("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_dec' WITH (OID=1004, DESCRIPTION="increment by one");

 CREATE FUNCTION numeric_smaller("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_smaller' WITH (OID=1766, DESCRIPTION="smaller of two numbers");

 CREATE FUNCTION numeric_larger("numeric", "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_larger' WITH (OID=1767, DESCRIPTION="larger of two numbers");

 CREATE FUNCTION numeric_cmp("numeric", "numeric") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_cmp' WITH (OID=1769, DESCRIPTION="compare two numbers");

 CREATE FUNCTION numeric_uminus("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_uminus' WITH (OID=1771, DESCRIPTION="negate");

 CREATE FUNCTION int8("numeric") RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_int8' WITH (OID=1779, DESCRIPTION="(internal)");

 CREATE FUNCTION "numeric"(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'int8_numeric' WITH (OID=1781, DESCRIPTION="(internal)");

 CREATE FUNCTION "numeric"(int2) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'int2_numeric' WITH (OID=1782, DESCRIPTION="(internal)");

 CREATE FUNCTION int2("numeric") RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'numeric_int2' WITH (OID=1783, DESCRIPTION="(internal)");

-- formatting
 CREATE FUNCTION to_char(timestamptz, text) RETURNS text LANGUAGE internal STABLE STRICT AS 'timestamptz_to_char' WITH (OID=1770, DESCRIPTION="format timestamp with time zone to text");

 CREATE FUNCTION to_char("numeric", text) RETURNS text LANGUAGE internal STABLE STRICT AS 'numeric_to_char' WITH (OID=1772, DESCRIPTION="format numeric to text");

 CREATE FUNCTION to_char(int4, text) RETURNS text LANGUAGE internal STABLE STRICT AS 'int4_to_char' WITH (OID=1773, DESCRIPTION="format int4 to text");

 CREATE FUNCTION to_char(int8, text) RETURNS text LANGUAGE internal STABLE STRICT AS 'int8_to_char' WITH (OID=1774, DESCRIPTION="format int8 to text");

 CREATE FUNCTION to_char(float4, text) RETURNS text LANGUAGE internal STABLE STRICT AS 'float4_to_char' WITH (OID=1775, DESCRIPTION="format float4 to text");

 CREATE FUNCTION to_char(float8, text) RETURNS text LANGUAGE internal STABLE STRICT AS 'float8_to_char' WITH (OID=1776, DESCRIPTION="format float8 to text");

 CREATE FUNCTION to_number(text, text) RETURNS "numeric" LANGUAGE internal STABLE STRICT AS 'numeric_to_number' WITH (OID=1777, DESCRIPTION="convert text to numeric");

 CREATE FUNCTION to_timestamp(text, text) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'to_timestamp' WITH (OID=1778, DESCRIPTION="convert text to timestamp with time zone");

 CREATE FUNCTION to_date(text, text) RETURNS date LANGUAGE internal STABLE STRICT AS 'to_date' WITH (OID=1780, DESCRIPTION="convert text to date");

 CREATE FUNCTION to_char("interval", text) RETURNS text LANGUAGE internal STABLE STRICT AS 'interval_to_char' WITH (OID=1768, DESCRIPTION="format interval to text");

 CREATE FUNCTION quote_ident(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'quote_ident' WITH (OID=1282, DESCRIPTION="quote an identifier for usage in a querystring");

 CREATE FUNCTION quote_literal(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'quote_literal' WITH (OID=1283, DESCRIPTION="quote a literal for usage in a querystring");

 CREATE FUNCTION oidin(cstring) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'oidin' WITH (OID=1798, DESCRIPTION="I/O");

 CREATE FUNCTION oidout(oid) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'oidout' WITH (OID=1799, DESCRIPTION="I/O");

 CREATE FUNCTION bit_length(bytea) RETURNS pg_catalog.int4 LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.octet_length($1) * 8$$  WITH (OID=1810, DESCRIPTION="length in bits");

 CREATE FUNCTION bit_length(text) RETURNS pg_catalog.int4 LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.octet_length($1) * 8$$  WITH (OID=1811, DESCRIPTION="length in bits");

 CREATE FUNCTION bit_length("bit") RETURNS pg_catalog.int4 LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.length($1)$$  WITH (OID=1812, DESCRIPTION="length in bits");

-- Selectivity estimators for LIKE and related operators 
 CREATE FUNCTION iclikesel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'iclikesel' WITH (OID=1814, DESCRIPTION="restriction selectivity of ILIKE");

 CREATE FUNCTION icnlikesel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'icnlikesel' WITH (OID=1815, DESCRIPTION="restriction selectivity of NOT ILIKE");

 CREATE FUNCTION iclikejoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'iclikejoinsel' WITH (OID=1816, DESCRIPTION="join selectivity of ILIKE");

 CREATE FUNCTION icnlikejoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'icnlikejoinsel' WITH (OID=1817, DESCRIPTION="join selectivity of NOT ILIKE");

 CREATE FUNCTION regexeqsel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'regexeqsel' WITH (OID=1818, DESCRIPTION="restriction selectivity of regex match");

 CREATE FUNCTION likesel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'likesel' WITH (OID=1819, DESCRIPTION="restriction selectivity of LIKE");

 CREATE FUNCTION icregexeqsel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'icregexeqsel' WITH (OID=1820, DESCRIPTION="restriction selectivity of case-insensitive regex match");

 CREATE FUNCTION regexnesel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'regexnesel' WITH (OID=1821, DESCRIPTION="restriction selectivity of regex non-match");

 CREATE FUNCTION nlikesel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'nlikesel' WITH (OID=1822, DESCRIPTION="restriction selectivity of NOT LIKE");

 CREATE FUNCTION icregexnesel(internal, oid, internal, int4) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'icregexnesel' WITH (OID=1823, DESCRIPTION="restriction selectivity of case-insensitive regex non-match");

 CREATE FUNCTION regexeqjoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'regexeqjoinsel' WITH (OID=1824, DESCRIPTION="join selectivity of regex match");

 CREATE FUNCTION likejoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'likejoinsel' WITH (OID=1825, DESCRIPTION="join selectivity of LIKE");

 CREATE FUNCTION icregexeqjoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'icregexeqjoinsel' WITH (OID=1826, DESCRIPTION="join selectivity of case-insensitive regex match");

 CREATE FUNCTION regexnejoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'regexnejoinsel' WITH (OID=1827, DESCRIPTION="join selectivity of regex non-match");

 CREATE FUNCTION nlikejoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'nlikejoinsel' WITH (OID=1828, DESCRIPTION="join selectivity of NOT LIKE");

 CREATE FUNCTION icregexnejoinsel(internal, oid, internal, int2) RETURNS float8 LANGUAGE internal STABLE STRICT AS 'icregexnejoinsel' WITH (OID=1829, DESCRIPTION="join selectivity of case-insensitive regex non-match");

-- Sequences and time series
 CREATE FUNCTION interval_bound(numeric, numeric) RETURNS numeric LANGUAGE internal IMMUTABLE STRICT AS 'numeric_interval_bound' WITH (OID=2991, DESCRIPTION="boundary of the interval containing the given value");

 CREATE FUNCTION interval_bound(numeric, numeric, int4) RETURNS numeric LANGUAGE internal IMMUTABLE AS 'numeric_interval_bound_shift' WITH (OID=2992, DESCRIPTION="boundary of the interval containing the given value");

 CREATE FUNCTION interval_bound(numeric, numeric, int4, numeric) RETURNS numeric LANGUAGE internal IMMUTABLE AS 'numeric_interval_bound_shift_rbound' WITH (OID=2993, DESCRIPTION="boundary of the interval containing the given value");

 CREATE FUNCTION interval_bound(timestamp, "interval") RETURNS timestamp LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_interval_bound' WITH (OID=2994, DESCRIPTION="boundary of the interval containing the given value");

 CREATE FUNCTION interval_bound(timestamp, "interval", int4) RETURNS timestamp LANGUAGE internal IMMUTABLE AS 'timestamp_interval_bound_shift' WITH (OID=2995, DESCRIPTION="boundary of the interval containing the given value");

 CREATE FUNCTION interval_bound(timestamp, "interval", int4, timestamp) RETURNS timestamp LANGUAGE internal IMMUTABLE AS 'timestamp_interval_bound_shift_reg' WITH (OID=2996, DESCRIPTION="boundary of the interval containing the given value");

 CREATE FUNCTION interval_bound(timestamptz, "interval") RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_interval_bound' WITH (OID=2997, DESCRIPTION="boundary of the interval containing the given value");

 CREATE FUNCTION interval_bound(timestamptz, "interval", int4) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'timestamptz_interval_bound_shift' WITH (OID=2998, DESCRIPTION="boundary of the interval containing the given value");

 CREATE FUNCTION interval_bound(timestamptz, "interval", int4, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'timestamptz_interval_bound_shift_reg' WITH (OID=2999, DESCRIPTION="boundary of the interval containing the given value");


-- Aggregate-related functions 
 CREATE FUNCTION float8_avg(bytea) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_avg' WITH (OID=1830, DESCRIPTION="AVG aggregate final function");

 CREATE FUNCTION float8_var_pop(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_var_pop' WITH (OID=2512, DESCRIPTION="VAR_POP aggregate final function");

 CREATE FUNCTION float8_var_samp(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_var_samp' WITH (OID=1831, DESCRIPTION="VAR_SAMP aggregate final function");

 CREATE FUNCTION float8_stddev_pop(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_stddev_pop' WITH (OID=2513, DESCRIPTION="STDDEV_POP aggregate final function");

 CREATE FUNCTION float8_stddev_samp(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_stddev_samp' WITH (OID=1832, DESCRIPTION="STDDEV_SAMP aggregate final function");

 CREATE FUNCTION numeric_accum(_numeric, "numeric") RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'numeric_accum' WITH (OID=1833, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION numeric_avg_accum(bytea, "numeric") RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'numeric_avg_accum' WITH (OID=3102, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION numeric_decum(_numeric, "numeric") RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'numeric_decum' WITH (OID=7309, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION numeric_avg_decum(bytea, "numeric") RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'numeric_avg_decum' WITH (OID=3103, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION int2_accum(_numeric, int2) RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'int2_accum' WITH (OID=1834, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION int4_accum(_numeric, int4) RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'int4_accum' WITH (OID=1835, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION int8_accum(_numeric, int8) RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'int8_accum' WITH (OID=1836, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION int2_decum(_numeric, int2) RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'int2_decum' WITH (OID=7306, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION int4_decum(_numeric, int4) RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'int4_decum' WITH (OID=7307, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION int8_decum(_numeric, int8) RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'int8_decum' WITH (OID=7308, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION numeric_avg(bytea) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_avg' WITH (OID=1837, DESCRIPTION="AVG aggregate final function");

 CREATE FUNCTION numeric_var_pop(_numeric) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_var_pop' WITH (OID=2514, DESCRIPTION="VAR_POP aggregate final function");

 CREATE FUNCTION numeric_var_samp(_numeric) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_var_samp' WITH (OID=1838, DESCRIPTION="VAR_SAMP aggregate final function");

 CREATE FUNCTION numeric_stddev_pop(_numeric) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_stddev_pop' WITH (OID=2596, DESCRIPTION="STDDEV_POP aggregate final function");

 CREATE FUNCTION numeric_stddev_samp(_numeric) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_stddev_samp' WITH (OID=1839, DESCRIPTION="STDDEV_SAMP aggregate final function");

 CREATE FUNCTION int2_sum(int8, int2) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'int2_sum' WITH (OID=1840, DESCRIPTION="SUM(int2) transition function");

 CREATE FUNCTION int4_sum(int8, int4) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'int4_sum' WITH (OID=1841, DESCRIPTION="SUM(int4) transition function");

 CREATE FUNCTION int8_sum("numeric", int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'int8_sum' WITH (OID=1842, DESCRIPTION="SUM(int8) transition function");

 CREATE FUNCTION int2_invsum(int8, int2) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'int2_invsum' WITH (OID=7008, DESCRIPTION="SUM(int2) inverse transition function");

 CREATE FUNCTION int4_invsum(int8, int4) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'int4_invsum' WITH (OID=7009, DESCRIPTION="SUM(int4) inverse transition function");

 CREATE FUNCTION int8_invsum("numeric", int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'int8_invsum' WITH (OID=7010, DESCRIPTION="SUM(int8) inverse transition function");

 CREATE FUNCTION interval_accum(_interval, "interval") RETURNS _interval LANGUAGE internal IMMUTABLE STRICT AS 'interval_accum' WITH (OID=1843, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION interval_decum(_interval, "interval") RETURNS _interval LANGUAGE internal IMMUTABLE STRICT AS 'interval_decum' WITH (OID=6038, DESCRIPTION="aggregate inverse transition function");

 CREATE FUNCTION interval_avg(_interval) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_avg' WITH (OID=1844, DESCRIPTION="AVG aggregate final function");

 CREATE FUNCTION int2_avg_accum(bytea, int2) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int2_avg_accum' WITH (OID=1962, DESCRIPTION="AVG(int2) transition function");

 CREATE FUNCTION int4_avg_accum(bytea, int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int4_avg_accum' WITH (OID=1963, DESCRIPTION="AVG(int4) transition function");

 CREATE FUNCTION int8_avg_accum(bytea, int8) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int8_avg_accum' WITH (OID=3100, DESCRIPTION="AVG(int8) transition function");

 CREATE FUNCTION int2_avg_decum(bytea, int2) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int2_avg_decum' WITH (OID=6019, DESCRIPTION="AVG(int2) transition function");

 CREATE FUNCTION int4_avg_decum(bytea, int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int4_avg_decum' WITH (OID=6020, DESCRIPTION="AVG(int4) transition function");

 CREATE FUNCTION int8_avg_decum(bytea, int8) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int8_avg_decum' WITH (OID=3101, DESCRIPTION="AVG(int8) transition function");

 CREATE FUNCTION int8_avg(bytea) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'int8_avg' WITH (OID=1964, DESCRIPTION="AVG(int) aggregate final function");

 CREATE FUNCTION int8inc_float8_float8(int8, float8, float8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8inc_float8_float8' WITH (OID=2805, DESCRIPTION="REGR_COUNT(double, double) transition function");

 CREATE FUNCTION float8_regr_accum(_float8, float8, float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_accum' WITH (OID=2806, DESCRIPTION="REGR_...(double, double) transition function");

 CREATE FUNCTION float8_regr_sxx(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_sxx' WITH (OID=2807, DESCRIPTION="REGR_SXX(double, double) aggregate final function");

 CREATE FUNCTION float8_regr_syy(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_syy' WITH (OID=2808, DESCRIPTION="REGR_SYY(double, double) aggregate final function");

 CREATE FUNCTION float8_regr_sxy(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_sxy' WITH (OID=2809, DESCRIPTION="REGR_SXY(double, double) aggregate final function");

 CREATE FUNCTION float8_regr_avgx(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_avgx' WITH (OID=2810, DESCRIPTION="REGR_AVGX(double, double) aggregate final function");

 CREATE FUNCTION float8_regr_avgy(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_avgy' WITH (OID=2811, DESCRIPTION="REGR_AVGY(double, double) aggregate final function");

 CREATE FUNCTION float8_regr_r2(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_r2' WITH (OID=2812, DESCRIPTION="REGR_R2(double, double) aggregate final function");

 CREATE FUNCTION float8_regr_slope(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_slope' WITH (OID=2813, DESCRIPTION="REGR_SLOPE(double, double) aggregate final function");

 CREATE FUNCTION float8_regr_intercept(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_intercept' WITH (OID=2814, DESCRIPTION="REGR_INTERCEPT(double, double) aggregate final function");

 CREATE FUNCTION float8_covar_pop(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_covar_pop' WITH (OID=2815, DESCRIPTION="COVAR_POP(double, double) aggregate final function");

 CREATE FUNCTION float8_covar_samp(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_covar_samp' WITH (OID=2816, DESCRIPTION="COVAR_SAMP(double, double) aggregate final function");

 CREATE FUNCTION float8_corr(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_corr' WITH (OID=2817, DESCRIPTION="CORR(double, double) aggregate final function");

 CREATE FUNCTION pg_partition_oid_transfn(internal, oid, record) RETURNS internal LANGUAGE internal IMMUTABLE AS 'pg_partition_oid_transfn' WITH (OID=2911, DESCRIPTION="pg_partition_oid transition function");

 CREATE FUNCTION pg_partition_oid_finalfn(internal) RETURNS _oid LANGUAGE internal IMMUTABLE AS 'pg_partition_oid_finalfn' WITH (OID=2912, DESCRIPTION="pg_partition_oid final function");

 CREATE FUNCTION pg_partition_oid(oid, record) RETURNS _oid LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2913, proisagg="t");

-- To ASCII conversion 
 CREATE FUNCTION to_ascii(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'to_ascii_default' WITH (OID=1845, DESCRIPTION="encode text from DB encoding to ASCII text");

 CREATE FUNCTION to_ascii(text, int4) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'to_ascii_enc' WITH (OID=1846, DESCRIPTION="encode text from encoding to ASCII text");

 CREATE FUNCTION to_ascii(text, name) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'to_ascii_encname' WITH (OID=1847, DESCRIPTION="encode text from encoding to ASCII text");

 CREATE FUNCTION interval_pl_time("interval", "time") RETURNS pg_catalog."time" LANGUAGE sql IMMUTABLE STRICT AS $$select $2 + $1$$  WITH (OID=1848, DESCRIPTION="plus");

 CREATE FUNCTION int28eq(int2, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int28eq' WITH (OID=1850, DESCRIPTION="equal");

 CREATE FUNCTION int28ne(int2, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int28ne' WITH (OID=1851, DESCRIPTION="not equal");

 CREATE FUNCTION int28lt(int2, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int28lt' WITH (OID=1852, DESCRIPTION="less-than");

 CREATE FUNCTION int28gt(int2, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int28gt' WITH (OID=1853, DESCRIPTION="greater-than");

 CREATE FUNCTION int28le(int2, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int28le' WITH (OID=1854, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int28ge(int2, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int28ge' WITH (OID=1855, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int82eq(int8, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int82eq' WITH (OID=1856, DESCRIPTION="equal");

 CREATE FUNCTION int82ne(int8, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int82ne' WITH (OID=1857, DESCRIPTION="not equal");

 CREATE FUNCTION int82lt(int8, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int82lt' WITH (OID=1858, DESCRIPTION="less-than");

 CREATE FUNCTION int82gt(int8, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int82gt' WITH (OID=1859, DESCRIPTION="greater-than");

 CREATE FUNCTION int82le(int8, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int82le' WITH (OID=1860, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION int82ge(int8, int2) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int82ge' WITH (OID=1861, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION int2and(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2and' WITH (OID=1892, DESCRIPTION="bitwise and");

 CREATE FUNCTION int2or(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2or' WITH (OID=1893, DESCRIPTION="bitwise or");

 CREATE FUNCTION int2xor(int2, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2xor' WITH (OID=1894, DESCRIPTION="bitwise xor");

 CREATE FUNCTION int2not(int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2not' WITH (OID=1895, DESCRIPTION="bitwise not");

 CREATE FUNCTION int2shl(int2, int4) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2shl' WITH (OID=1896, DESCRIPTION="bitwise shift left");

 CREATE FUNCTION int2shr(int2, int4) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2shr' WITH (OID=1897, DESCRIPTION="bitwise shift right");

 CREATE FUNCTION int4and(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4and' WITH (OID=1898, DESCRIPTION="bitwise and");

 CREATE FUNCTION int4or(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4or' WITH (OID=1899, DESCRIPTION="bitwise or");

 CREATE FUNCTION int4xor(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4xor' WITH (OID=1900, DESCRIPTION="bitwise xor");

 CREATE FUNCTION int4not(int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4not' WITH (OID=1901, DESCRIPTION="bitwise not");

 CREATE FUNCTION int4shl(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4shl' WITH (OID=1902, DESCRIPTION="bitwise shift left");

 CREATE FUNCTION int4shr(int4, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4shr' WITH (OID=1903, DESCRIPTION="bitwise shift right");

 CREATE FUNCTION int8and(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8and' WITH (OID=1904, DESCRIPTION="bitwise and");

 CREATE FUNCTION int8or(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8or' WITH (OID=1905, DESCRIPTION="bitwise or");

 CREATE FUNCTION int8xor(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8xor' WITH (OID=1906, DESCRIPTION="bitwise xor");

 CREATE FUNCTION int8not(int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8not' WITH (OID=1907, DESCRIPTION="bitwise not");

 CREATE FUNCTION int8shl(int8, int4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8shl' WITH (OID=1908, DESCRIPTION="bitwise shift left");

 CREATE FUNCTION int8shr(int8, int4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8shr' WITH (OID=1909, DESCRIPTION="bitwise shift right");

 CREATE FUNCTION int8up(int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8up' WITH (OID=1910, DESCRIPTION="unary plus");

 CREATE FUNCTION int2up(int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2up' WITH (OID=1911, DESCRIPTION="unary plus");

 CREATE FUNCTION int4up(int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4up' WITH (OID=1912, DESCRIPTION="unary plus");

 CREATE FUNCTION float4up(float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4up' WITH (OID=1913, DESCRIPTION="unary plus");

 CREATE FUNCTION float8up(float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8up' WITH (OID=1914, DESCRIPTION="unary plus");

 CREATE FUNCTION numeric_uplus("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_uplus' WITH (OID=1915, DESCRIPTION="unary plus");

 CREATE FUNCTION has_table_privilege(name, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_table_privilege_name_name' WITH (OID=1922, DESCRIPTION="user privilege on relation by username, rel name");

 CREATE FUNCTION has_table_privilege(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_table_privilege_name_id' WITH (OID=1923, DESCRIPTION="user privilege on relation by username, rel oid");

 CREATE FUNCTION has_table_privilege(oid, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_table_privilege_id_name' WITH (OID=1924, DESCRIPTION="user privilege on relation by user oid, rel name");

 CREATE FUNCTION has_table_privilege(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_table_privilege_id_id' WITH (OID=1925, DESCRIPTION="user privilege on relation by user oid, rel oid");

 CREATE FUNCTION has_table_privilege(text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_table_privilege_name' WITH (OID=1926, DESCRIPTION="current user privilege on relation by rel name");

 CREATE FUNCTION has_table_privilege(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_table_privilege_id' WITH (OID=1927, DESCRIPTION="current user privilege on relation by rel oid");

 CREATE FUNCTION pg_stat_get_numscans(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_numscans' WITH (OID=1928, DESCRIPTION="Statistics: Number of scans done for table/index");

 CREATE FUNCTION pg_stat_get_tuples_returned(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_tuples_returned' WITH (OID=1929, DESCRIPTION="Statistics: Number of tuples read by seqscan");

 CREATE FUNCTION pg_stat_get_tuples_fetched(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_tuples_fetched' WITH (OID=1930, DESCRIPTION="Statistics: Number of tuples fetched by idxscan");

 CREATE FUNCTION pg_stat_get_tuples_inserted(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_tuples_inserted' WITH (OID=1931, DESCRIPTION="Statistics: Number of tuples inserted");

 CREATE FUNCTION pg_stat_get_tuples_updated(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_tuples_updated' WITH (OID=1932, DESCRIPTION="Statistics: Number of tuples updated");

 CREATE FUNCTION pg_stat_get_tuples_deleted(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_tuples_deleted' WITH (OID=1933, DESCRIPTION="Statistics: Number of tuples deleted");

 CREATE FUNCTION pg_stat_get_blocks_fetched(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_blocks_fetched' WITH (OID=1934, DESCRIPTION="Statistics: Number of blocks fetched");

 CREATE FUNCTION pg_stat_get_blocks_hit(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_blocks_hit' WITH (OID=1935, DESCRIPTION="Statistics: Number of blocks found in cache");

 CREATE FUNCTION pg_stat_get_last_vacuum_time(oid) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'pg_stat_get_last_vacuum_time' WITH (OID=2781, DESCRIPTION="Statistics: Last manual vacuum time for a table");

 CREATE FUNCTION pg_stat_get_last_autovacuum_time(oid) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'pg_stat_get_last_autovacuum_time' WITH (OID=2782, DESCRIPTION="Statistics: Last auto vacuum time for a table");

 CREATE FUNCTION pg_stat_get_last_analyze_time(oid) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'pg_stat_get_last_analyze_time' WITH (OID=2783, DESCRIPTION="Statistics: Last manual analyze time for a table");

 CREATE FUNCTION pg_stat_get_last_autoanalyze_time(oid) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'pg_stat_get_last_autoanalyze_time' WITH (OID=2784, DESCRIPTION="Statistics: Last auto analyze time for a table");

 CREATE FUNCTION pg_stat_get_backend_idset() RETURNS SETOF int4 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_idset' WITH (OID=1936, DESCRIPTION="Statistics: Currently active backend IDs");

 CREATE FUNCTION pg_backend_pid() RETURNS int4 LANGUAGE internal STABLE STRICT AS 'pg_backend_pid' WITH (OID=2026, DESCRIPTION="Statistics: Current backend PID");

 CREATE FUNCTION pg_stat_reset() RETURNS bool LANGUAGE internal VOLATILE AS 'pg_stat_reset' WITH (OID=2274, DESCRIPTION="Statistics: Reset collected statistics");

 CREATE FUNCTION pg_stat_get_backend_pid(int4) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_pid' WITH (OID=1937, DESCRIPTION="Statistics: PID of backend");

 CREATE FUNCTION pg_stat_get_backend_dbid(int4) RETURNS oid LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_dbid' WITH (OID=1938, DESCRIPTION="Statistics: Database ID of backend");

 CREATE FUNCTION pg_stat_get_backend_userid(int4) RETURNS oid LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_userid' WITH (OID=1939, DESCRIPTION="Statistics: User ID of backend");

 CREATE FUNCTION pg_stat_get_backend_activity(int4) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_activity' WITH (OID=1940, DESCRIPTION="Statistics: Current query of backend");

 CREATE FUNCTION pg_stat_get_backend_waiting(int4) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_waiting' WITH (OID=2853, DESCRIPTION="Statistics: Is backend currently waiting for a lock");

 CREATE FUNCTION pg_stat_get_backend_activity_start(int4) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_activity_start' WITH (OID=2094, DESCRIPTION="Statistics: Start time for current query of backend");

 CREATE FUNCTION pg_stat_get_backend_start(int4) RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_start' WITH (OID=1391, DESCRIPTION="Statistics: Start time for current backend session");

 CREATE FUNCTION pg_stat_get_backend_client_addr(int4) RETURNS inet LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_client_addr' WITH (OID=1392, DESCRIPTION="Statistics: Address of client connected to backend");

 CREATE FUNCTION pg_stat_get_backend_client_port(int4) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_client_port' WITH (OID=1393, DESCRIPTION="Statistics: Port number of client connected to backend");

 CREATE FUNCTION pg_stat_get_backend_waiting_reason(int4) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_waiting_reason' WITH (OID=2972, DESCRIPTION="Statistics: Reason backend is waiting for");

 CREATE FUNCTION pg_stat_get_db_numbackends(oid) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_db_numbackends' WITH (OID=1941, DESCRIPTION="Statistics: Number of backends in database");

 CREATE FUNCTION pg_stat_get_db_xact_commit(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_db_xact_commit' WITH (OID=1942, DESCRIPTION="Statistics: Transactions committed");

 CREATE FUNCTION pg_stat_get_db_xact_rollback(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_db_xact_rollback' WITH (OID=1943, DESCRIPTION="Statistics: Transactions rolled back");

 CREATE FUNCTION pg_stat_get_db_blocks_fetched(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_db_blocks_fetched' WITH (OID=1944, DESCRIPTION="Statistics: Blocks fetched for database");

 CREATE FUNCTION pg_stat_get_db_blocks_hit(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_db_blocks_hit' WITH (OID=1945, DESCRIPTION="Statistics: Blocks found in cache for database");

 CREATE FUNCTION pg_stat_get_queue_num_exec(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_queue_num_exec' WITH (OID=6031, DESCRIPTION="Statistics: Number of queries that executed in queue");

 CREATE FUNCTION pg_stat_get_queue_num_wait(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_queue_num_wait' WITH (OID=6032, DESCRIPTION="Statistics: Number of queries that waited in queue");

 CREATE FUNCTION pg_stat_get_queue_elapsed_exec(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_queue_elapsed_exec' WITH (OID=6033, DESCRIPTION="Statistics:  Elapsed seconds for queries that executed in queue");

 CREATE FUNCTION pg_stat_get_queue_elapsed_wait(oid) RETURNS int8 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_queue_elapsed_wait' WITH (OID=6034, DESCRIPTION="Statistics:  Elapsed seconds for queries that waited in queue");

 CREATE FUNCTION pg_stat_get_backend_session_id(int4) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'pg_stat_get_backend_session_id' WITH (OID=6039, DESCRIPTION="Statistics: Greenplum session id of backend");

 CREATE FUNCTION pg_renice_session(int4, int4) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'pg_renice_session' WITH (OID=6042, DESCRIPTION="change priority of all the backends for a given session id");

 CREATE FUNCTION pg_stat_get_activity(IN pid int4, OUT datid oid, OUT procpid int4, OUT usesysid oid, OUT application_name text, OUT current_query text, OUT waiting bool, OUT xact_start timestamptz, OUT query_start timestamptz, OUT backend_start timestamptz, OUT client_addr inet, OUT client_port int4, OUT sess_id int4, OUT waiting_reason text) RETURNS SETOF pg_catalog.record LANGUAGE internal VOLATILE AS 'pg_stat_get_activity' WITH (OID=6071, DESCRIPTION="statistics: information about currently active backends");

 CREATE FUNCTION pg_stat_get_wal_senders(OUT pid int4, OUT state text, OUT sent_location text, OUT write_location text, OUT flush_location text, OUT replay_location text, OUT sync_priority int4, OUT sync_state text) RETURNS SETOF pg_catalog.record LANGUAGE internal STABLE AS 'pg_stat_get_wal_senders' WITH (OID=3099, DESCRIPTION="statistics: information about currently active replication");

 CREATE FUNCTION encode(bytea, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'binary_encode' WITH (OID=1946, DESCRIPTION="Convert bytea value into some ascii-only text string");

 CREATE FUNCTION "decode"(text, text) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'binary_decode' WITH (OID=1947, DESCRIPTION="Convert ascii-encoded text string into bytea value");

 CREATE FUNCTION byteaeq(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'byteaeq' WITH (OID=1948, DESCRIPTION="equal");

 CREATE FUNCTION bytealt(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bytealt' WITH (OID=1949, DESCRIPTION="less-than");

 CREATE FUNCTION byteale(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'byteale' WITH (OID=1950, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION byteagt(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'byteagt' WITH (OID=1951, DESCRIPTION="greater-than");

 CREATE FUNCTION byteage(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'byteage' WITH (OID=1952, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION byteane(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'byteane' WITH (OID=1953, DESCRIPTION="not equal");

 CREATE FUNCTION byteacmp(bytea, bytea) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'byteacmp' WITH (OID=1954, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION "timestamp"("timestamp", int4) RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_scale' WITH (OID=1961, DESCRIPTION="adjust timestamp precision");

 CREATE FUNCTION oidlarger(oid, oid) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'oidlarger' WITH (OID=1965, DESCRIPTION="larger of two");

 CREATE FUNCTION oidsmaller(oid, oid) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'oidsmaller' WITH (OID=1966, DESCRIPTION="smaller of two");

 CREATE FUNCTION timestamptz(timestamptz, int4) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_scale' WITH (OID=1967, DESCRIPTION="adjust timestamptz precision");

 CREATE FUNCTION "time"("time", int4) RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'time_scale' WITH (OID=1968, DESCRIPTION="adjust time precision");

 CREATE FUNCTION timetz(timetz, int4) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_scale' WITH (OID=1969, DESCRIPTION="adjust time with time zone precision");

 CREATE FUNCTION bytealike(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bytealike' WITH (OID=2005, DESCRIPTION="matches LIKE expression");

 CREATE FUNCTION byteanlike(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'byteanlike' WITH (OID=2006, DESCRIPTION="does not match LIKE expression");

 CREATE FUNCTION "like"(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'bytealike' WITH (OID=2007, DESCRIPTION="matches LIKE expression");

 CREATE FUNCTION notlike(bytea, bytea) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'byteanlike' WITH (OID=2008, DESCRIPTION="does not match LIKE expression");

 CREATE FUNCTION like_escape(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'like_escape_bytea' WITH (OID=2009, DESCRIPTION="convert LIKE pattern to use backslash escapes");

 CREATE FUNCTION length(bytea) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'byteaoctetlen' WITH (OID=2010, DESCRIPTION="octet length");

 CREATE FUNCTION byteacat(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'byteacat' WITH (OID=2011, DESCRIPTION="concatenate");

 CREATE FUNCTION "substring"(bytea, int4, int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'bytea_substr' WITH (OID=2012, DESCRIPTION="return portion of string");

 CREATE FUNCTION "substring"(bytea, int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'bytea_substr_no_len' WITH (OID=2013, DESCRIPTION="return portion of string");

 CREATE FUNCTION substr(bytea, int4, int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'bytea_substr' WITH (OID=2085, DESCRIPTION="return portion of string");

 CREATE FUNCTION substr(bytea, int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'bytea_substr_no_len' WITH (OID=2086, DESCRIPTION="return portion of string");

 CREATE FUNCTION "position"(bytea, bytea) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'byteapos' WITH (OID=2014, DESCRIPTION="return position of substring");

 CREATE FUNCTION btrim(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'byteatrim' WITH (OID=2015, DESCRIPTION="trim both ends of string");

 CREATE FUNCTION "time"(timestamptz) RETURNS "time" LANGUAGE internal STABLE STRICT AS 'timestamptz_time' WITH (OID=2019, DESCRIPTION="convert timestamptz to time");

 CREATE FUNCTION date_trunc(text, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_trunc' WITH (OID=2020, DESCRIPTION="truncate timestamp to specified units");

 CREATE FUNCTION date_part(text, "timestamp") RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_part' WITH (OID=2021, DESCRIPTION="extract field from timestamp");

 CREATE FUNCTION "timestamp"(text) RETURNS "timestamp" LANGUAGE internal STABLE STRICT AS 'text_timestamp' WITH (OID=2022, DESCRIPTION="convert text to timestamp");

 CREATE FUNCTION "timestamp"(abstime) RETURNS "timestamp" LANGUAGE internal STABLE STRICT AS 'abstime_timestamp' WITH (OID=2023, DESCRIPTION="convert abstime to timestamp");

 CREATE FUNCTION "timestamp"(date) RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'date_timestamp' WITH (OID=2024, DESCRIPTION="convert date to timestamp");

 CREATE FUNCTION "timestamp"(date, "time") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'datetime_timestamp' WITH (OID=2025, DESCRIPTION="convert date and time to timestamp");

 CREATE FUNCTION "timestamp"(timestamptz) RETURNS "timestamp" LANGUAGE internal STABLE STRICT AS 'timestamptz_timestamp' WITH (OID=2027, DESCRIPTION="convert timestamp with time zone to timestamp");

 CREATE FUNCTION timestamptz("timestamp") RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'timestamp_timestamptz' WITH (OID=2028, DESCRIPTION="convert timestamp to timestamp with time zone");

 CREATE FUNCTION date("timestamp") RETURNS date LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_date' WITH (OID=2029, DESCRIPTION="convert timestamp to date");

 CREATE FUNCTION abstime("timestamp") RETURNS abstime LANGUAGE internal STABLE STRICT AS 'timestamp_abstime' WITH (OID=2030, DESCRIPTION="convert timestamp to abstime");

 CREATE FUNCTION timestamp_mi("timestamp", "timestamp") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_mi' WITH (OID=2031, DESCRIPTION="subtract");

 CREATE FUNCTION timestamp_pl_interval("timestamp", "interval") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_pl_interval' WITH (OID=2032, DESCRIPTION="plus");

 CREATE FUNCTION timestamp_mi_interval("timestamp", "interval") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_mi_interval' WITH (OID=2033, DESCRIPTION="minus");

 CREATE FUNCTION text("timestamp") RETURNS text LANGUAGE internal STABLE STRICT AS 'timestamp_text' WITH (OID=2034, DESCRIPTION="convert timestamp to text");

 CREATE FUNCTION timestamp_smaller("timestamp", "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_smaller' WITH (OID=2035, DESCRIPTION="smaller of two");

 CREATE FUNCTION timestamp_larger("timestamp", "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_larger' WITH (OID=2036, DESCRIPTION="larger of two");

 CREATE FUNCTION timezone(text, timetz) RETURNS timetz LANGUAGE internal VOLATILE STRICT AS 'timetz_zone' WITH (OID=2037, DESCRIPTION="adjust time with time zone to new zone");

 CREATE FUNCTION timezone("interval", timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_izone' WITH (OID=2038, DESCRIPTION="adjust time with time zone to new zone");

 CREATE FUNCTION "overlaps"("timestamp", "timestamp", "timestamp", "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE AS 'overlaps_timestamp' WITH (OID=2041, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"("timestamp", "interval", "timestamp", "interval") RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE AS $$select ($1, ($1 + $2)) overlaps ($3, ($3 + $4))$$  WITH (OID=2042, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"("timestamp", "timestamp", "timestamp", "interval") RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE AS $$select ($1, $2) overlaps ($3, ($3 + $4))$$  WITH (OID=2043, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION "overlaps"("timestamp", "interval", "timestamp", "timestamp") RETURNS pg_catalog.bool LANGUAGE sql IMMUTABLE AS $$select ($1, ($1 + $2)) overlaps ($3, $4)$$  WITH (OID=2044, DESCRIPTION="SQL92 interval comparison");

 CREATE FUNCTION timestamp_cmp("timestamp", "timestamp") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_cmp' WITH (OID=2045, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION "time"(timetz) RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'timetz_time' WITH (OID=2046, DESCRIPTION="convert time with time zone to time");

 CREATE FUNCTION timetz("time") RETURNS timetz LANGUAGE internal STABLE STRICT AS 'time_timetz' WITH (OID=2047, DESCRIPTION="convert time to timetz");

 CREATE FUNCTION isfinite("timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_finite' WITH (OID=2048, DESCRIPTION="finite timestamp?");

 CREATE FUNCTION to_char("timestamp", text) RETURNS text LANGUAGE internal STABLE STRICT AS 'timestamp_to_char' WITH (OID=2049, DESCRIPTION="format timestamp to text");

 CREATE FUNCTION timestamp_eq("timestamp", "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_eq' WITH (OID=2052, DESCRIPTION="equal");

 CREATE FUNCTION timestamp_ne("timestamp", "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ne' WITH (OID=2053, DESCRIPTION="not equal");

 CREATE FUNCTION timestamp_lt("timestamp", "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_lt' WITH (OID=2054, DESCRIPTION="less-than");

 CREATE FUNCTION timestamp_le("timestamp", "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_le' WITH (OID=2055, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION timestamp_ge("timestamp", "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ge' WITH (OID=2056, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION timestamp_gt("timestamp", "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_gt' WITH (OID=2057, DESCRIPTION="greater-than");

 CREATE FUNCTION age("timestamp", "timestamp") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_age' WITH (OID=2058, DESCRIPTION="date difference preserving months and years");

 CREATE FUNCTION age("timestamp") RETURNS pg_catalog."interval" LANGUAGE sql STABLE STRICT AS $$select pg_catalog.age(cast(current_date as timestamp without time zone), $1)$$  WITH (OID=2059, DESCRIPTION="date difference from today preserving months and years");

 CREATE FUNCTION timezone(text, "timestamp") RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_zone' WITH (OID=2069, DESCRIPTION="adjust timestamp to new time zone");

 CREATE FUNCTION timezone("interval", "timestamp") RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_izone' WITH (OID=2070, DESCRIPTION="adjust timestamp to new time zone");

 CREATE FUNCTION date_pl_interval(date, "interval") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'date_pl_interval' WITH (OID=2071, DESCRIPTION="add");

 CREATE FUNCTION date_mi_interval(date, "interval") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'date_mi_interval' WITH (OID=2072, DESCRIPTION="subtract");

 CREATE FUNCTION "substring"(text, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'textregexsubstr' WITH (OID=2073, DESCRIPTION="extracts text matching regular expression");

 CREATE FUNCTION "substring"(text, text, text) RETURNS pg_catalog.text LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.substring($1, pg_catalog.similar_escape($2, $3))$$  WITH (OID=2074, DESCRIPTION="extracts text matching SQL99 regular expression");

 CREATE FUNCTION "bit"(int8, int4) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bitfromint8' WITH (OID=2075, DESCRIPTION="int8 to bitstring");

 CREATE FUNCTION int8("bit") RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'bittoint8' WITH (OID=2076, DESCRIPTION="bitstring to int8");

 CREATE FUNCTION current_setting(text) RETURNS text LANGUAGE internal STABLE STRICT AS 'show_config_by_name' WITH (OID=2077, DESCRIPTION="SHOW X as a function");

 CREATE FUNCTION set_config(text, text, bool) RETURNS text LANGUAGE internal VOLATILE AS 'set_config_by_name' WITH (OID=2078, DESCRIPTION="SET X as a function");

 CREATE FUNCTION pg_show_all_settings() RETURNS SETOF record LANGUAGE internal STABLE STRICT AS 'show_all_settings' WITH (OID=2084, DESCRIPTION="SHOW ALL as a function");

 CREATE FUNCTION pg_lock_status() RETURNS SETOF record LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_lock_status' WITH (OID=1371, DESCRIPTION="view system lock information");

 CREATE FUNCTION pg_prepared_xact() RETURNS SETOF record LANGUAGE internal VOLATILE STRICT AS 'pg_prepared_xact' WITH (OID=1065, DESCRIPTION="view two-phase transactions");

 CREATE FUNCTION pg_table_is_visible(oid) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_table_is_visible' WITH (OID=2079, DESCRIPTION="is table visible in search path?");

 CREATE FUNCTION pg_type_is_visible(oid) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_type_is_visible' WITH (OID=2080, DESCRIPTION="is type visible in search path?");

 CREATE FUNCTION pg_function_is_visible(oid) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_function_is_visible' WITH (OID=2081, DESCRIPTION="is function visible in search path?");

 CREATE FUNCTION pg_operator_is_visible(oid) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_operator_is_visible' WITH (OID=2082, DESCRIPTION="is operator visible in search path?");

 CREATE FUNCTION pg_opclass_is_visible(oid) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_opclass_is_visible' WITH (OID=2083, DESCRIPTION="is opclass visible in search path?");

 CREATE FUNCTION pg_conversion_is_visible(oid) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_conversion_is_visible' WITH (OID=2093, DESCRIPTION="is conversion visible in search path?");

 CREATE FUNCTION pg_my_temp_schema() RETURNS oid LANGUAGE internal STABLE STRICT AS 'pg_my_temp_schema' WITH (OID=2854, DESCRIPTION="get OID of current session's temp schema, if any");

 CREATE FUNCTION pg_is_other_temp_schema(oid) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_is_other_temp_schema' WITH (OID=2855, DESCRIPTION="is schema another session's temp schema?");

 CREATE FUNCTION pg_cancel_backend(int4) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_cancel_backend' WITH (OID=2171, DESCRIPTION="Cancel a server process' current query");

 CREATE FUNCTION pg_terminate_backend(int4) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_terminate_backend' WITH (OID=2878, DESCRIPTION="terminate a server process");

 CREATE FUNCTION pg_start_backup(text) RETURNS text LANGUAGE internal VOLATILE STRICT AS 'pg_start_backup' WITH (OID=2172, DESCRIPTION="Prepare for taking an online backup");

 CREATE FUNCTION pg_stop_backup() RETURNS text LANGUAGE internal VOLATILE STRICT AS 'pg_stop_backup' WITH (OID=2173, DESCRIPTION="Finish taking an online backup");

 CREATE FUNCTION pg_switch_xlog() RETURNS text LANGUAGE internal VOLATILE STRICT AS 'pg_switch_xlog' WITH (OID=2848, DESCRIPTION="Switch to new xlog file");

 CREATE FUNCTION pg_current_xlog_location() RETURNS text LANGUAGE internal VOLATILE STRICT AS 'pg_current_xlog_location' WITH (OID=2849, DESCRIPTION="current xlog write location");

 CREATE FUNCTION pg_current_xlog_insert_location() RETURNS text LANGUAGE internal VOLATILE STRICT AS 'pg_current_xlog_insert_location' WITH (OID=2852, DESCRIPTION="current xlog insert location");

 CREATE FUNCTION pg_xlogfile_name_offset(IN wal_location text, OUT file_name text, OUT file_offset int4) RETURNS pg_catalog.record LANGUAGE internal IMMUTABLE STRICT AS 'pg_xlogfile_name_offset' WITH (OID=2850, DESCRIPTION="xlog filename and byte offset, given an xlog location");

 CREATE FUNCTION pg_xlogfile_name(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'pg_xlogfile_name' WITH (OID=2851, DESCRIPTION="xlog filename, given an xlog location");

 CREATE FUNCTION pg_reload_conf() RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_reload_conf' WITH (OID=2621, DESCRIPTION="Reload configuration files");

 CREATE FUNCTION pg_rotate_logfile() RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_rotate_logfile' WITH (OID=2622, DESCRIPTION="Rotate log file");

 CREATE FUNCTION pg_stat_file(IN filename text, OUT size int8, OUT access timestamptz, OUT modification timestamptz, OUT change timestamptz, OUT creation timestamptz, OUT isdir bool) RETURNS pg_catalog.record LANGUAGE internal VOLATILE STRICT AS 'pg_stat_file' WITH (OID=2623, DESCRIPTION="Return file information");

 CREATE FUNCTION pg_read_file(text, int8, int8) RETURNS text LANGUAGE internal VOLATILE STRICT AS 'pg_read_file' WITH (OID=2624, DESCRIPTION="Read text from a file");

 CREATE FUNCTION pg_ls_dir(text) RETURNS SETOF text LANGUAGE internal VOLATILE STRICT AS 'pg_ls_dir' WITH (OID=2625, DESCRIPTION="List all files in a directory");

 CREATE FUNCTION pg_sleep(float8) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'pg_sleep' WITH (OID=2626, DESCRIPTION="Sleep for the specified time in seconds");

 CREATE FUNCTION pg_resqueue_status() RETURNS SETOF record LANGUAGE internal VOLATILE STRICT AS 'pg_resqueue_status' WITH (OID=6030, DESCRIPTION="Return resource queue information");

 CREATE FUNCTION pg_resqueue_status_kv() RETURNS SETOF record LANGUAGE internal VOLATILE STRICT AS 'pg_resqueue_status_kv' WITH (OID=6069, DESCRIPTION="Return resource queue information");

 CREATE FUNCTION pg_file_read(text, int8, int8) RETURNS text LANGUAGE internal VOLATILE STRICT AS 'pg_read_file' WITH (OID=6045, DESCRIPTION="Read text from a file");

 CREATE FUNCTION pg_logfile_rotate() RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_rotate_logfile' WITH (OID=6046, DESCRIPTION="Rotate log file");

 CREATE FUNCTION pg_file_write(text, text, bool) RETURNS int8 LANGUAGE internal VOLATILE STRICT AS 'pg_file_write' WITH (OID=6047, DESCRIPTION="Write text to a file");

 CREATE FUNCTION pg_file_rename(text, text, text) RETURNS bool LANGUAGE internal VOLATILE AS 'pg_file_rename' WITH (OID=6048, DESCRIPTION="Rename a file");

 CREATE FUNCTION pg_file_unlink(text) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_file_unlink' WITH (OID=6049, DESCRIPTION="Delete (unlink) a file");

 CREATE FUNCTION pg_logdir_ls() RETURNS SETOF record LANGUAGE internal VOLATILE STRICT AS 'pg_logdir_ls' WITH (OID=6050, DESCRIPTION="ls the log dir");

 CREATE FUNCTION pg_file_length(text) RETURNS int8 LANGUAGE internal VOLATILE STRICT AS 'pg_file_length' WITH (OID=6051, DESCRIPTION="Get the length of a file (via stat)");

 CREATE FUNCTION text(bool) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'booltext' WITH (OID=2971, DESCRIPTION="convert boolean to text");

-- Aggregates (moved here from pg_aggregate for 7.3) 

 CREATE FUNCTION avg(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2100, proisagg="t");

 CREATE FUNCTION avg(int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2101, proisagg="t");

 CREATE FUNCTION avg(int2) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2102, proisagg="t");

 CREATE FUNCTION avg("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2103, proisagg="t");

 CREATE FUNCTION avg(float4) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2104, proisagg="t");

 CREATE FUNCTION avg(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2105, proisagg="t");

 CREATE FUNCTION avg("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2106, proisagg="t");

-- #define SUM_OID_MIN 2107
 CREATE FUNCTION sum(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2107, proisagg="t");

 CREATE FUNCTION sum(int4) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2108, proisagg="t");

 CREATE FUNCTION sum(int2) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2109, proisagg="t");

 CREATE FUNCTION sum(float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2110, proisagg="t");

 CREATE FUNCTION sum(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2111, proisagg="t");

 CREATE FUNCTION sum(money) RETURNS money LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2112, proisagg="t");

 CREATE FUNCTION sum("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2113, proisagg="t");

 CREATE FUNCTION sum("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2114, proisagg="t");
-- #define SUM_OID_MAX 2114

 CREATE FUNCTION max(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2115, proisagg="t");

 CREATE FUNCTION max(int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2116, proisagg="t");

 CREATE FUNCTION max(int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2117, proisagg="t");

 CREATE FUNCTION max(oid) RETURNS oid LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2118, proisagg="t");

 CREATE FUNCTION max(float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2119, proisagg="t");

 CREATE FUNCTION max(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2120, proisagg="t");

 CREATE FUNCTION max(abstime) RETURNS abstime LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2121, proisagg="t");

 CREATE FUNCTION max(date) RETURNS date LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2122, proisagg="t");

 CREATE FUNCTION max("time") RETURNS "time" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2123, proisagg="t");

 CREATE FUNCTION max(timetz) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2124, proisagg="t");

 CREATE FUNCTION max(money) RETURNS money LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2125, proisagg="t");

 CREATE FUNCTION max("timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2126, proisagg="t");

 CREATE FUNCTION max(timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2127, proisagg="t");

 CREATE FUNCTION max("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2128, proisagg="t");

 CREATE FUNCTION max(text) RETURNS text LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2129, proisagg="t");

 CREATE FUNCTION max("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2130, proisagg="t");

 CREATE FUNCTION max(anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2050, proisagg="t");

 CREATE FUNCTION max(bpchar) RETURNS bpchar LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2244, proisagg="t");

 CREATE FUNCTION max(tid) RETURNS tid LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2797, proisagg="t");

 CREATE FUNCTION max(gpxlogloc) RETURNS gpxlogloc LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3332, proisagg="t");

 CREATE FUNCTION min(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2131, proisagg="t");

 CREATE FUNCTION min(int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2132, proisagg="t");

 CREATE FUNCTION min(int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2133, proisagg="t");

 CREATE FUNCTION min(oid) RETURNS oid LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2134, proisagg="t");

 CREATE FUNCTION min(float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2135, proisagg="t");

 CREATE FUNCTION min(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2136, proisagg="t");

 CREATE FUNCTION min(abstime) RETURNS abstime LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2137, proisagg="t");

 CREATE FUNCTION min(date) RETURNS date LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2138, proisagg="t");

 CREATE FUNCTION min("time") RETURNS "time" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2139, proisagg="t");

 CREATE FUNCTION min(timetz) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2140, proisagg="t");

 CREATE FUNCTION min(money) RETURNS money LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2141, proisagg="t");

 CREATE FUNCTION min("timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2142, proisagg="t");

 CREATE FUNCTION min(timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2143, proisagg="t");

 CREATE FUNCTION min("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2144, proisagg="t");

 CREATE FUNCTION min(text) RETURNS text LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2145, proisagg="t");

 CREATE FUNCTION min("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2146, proisagg="t");

 CREATE FUNCTION min(anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2051, proisagg="t");

 CREATE FUNCTION min(bpchar) RETURNS bpchar LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2245, proisagg="t");

 CREATE FUNCTION min(tid) RETURNS tid LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2798, proisagg="t");

 CREATE FUNCTION min(gpxlogloc) RETURNS gpxlogloc LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3333, proisagg="t");

-- count has two forms: count(any) and count(*) 
 CREATE FUNCTION count("any") RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2147, proisagg="t");
-- #define COUNT_ANY_OID 2147

 CREATE FUNCTION count() RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2803, proisagg="t");
-- #define COUNT_STAR_OID  2803

 CREATE FUNCTION var_pop(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2718, proisagg="t");

 CREATE FUNCTION var_pop(int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2719, proisagg="t");

 CREATE FUNCTION var_pop(int2) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2720, proisagg="t");

 CREATE FUNCTION var_pop(float4) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2721, proisagg="t");

 CREATE FUNCTION var_pop(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2722, proisagg="t");

 CREATE FUNCTION var_pop("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2723, proisagg="t");

 CREATE FUNCTION var_samp(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2641, proisagg="t");

 CREATE FUNCTION var_samp(int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2642, proisagg="t");

 CREATE FUNCTION var_samp(int2) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2643, proisagg="t");

 CREATE FUNCTION var_samp(float4) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2644, proisagg="t");

 CREATE FUNCTION var_samp(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2645, proisagg="t");

 CREATE FUNCTION var_samp("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2646, proisagg="t");

 CREATE FUNCTION variance(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2148, proisagg="t");

 CREATE FUNCTION variance(int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2149, proisagg="t");

 CREATE FUNCTION variance(int2) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2150, proisagg="t");

 CREATE FUNCTION variance(float4) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2151, proisagg="t");

 CREATE FUNCTION variance(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2152, proisagg="t");

 CREATE FUNCTION variance("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2153, proisagg="t");

 CREATE FUNCTION stddev_pop(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2724, proisagg="t");

 CREATE FUNCTION stddev_pop(int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2725, proisagg="t");

 CREATE FUNCTION stddev_pop(int2) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2726, proisagg="t");

 CREATE FUNCTION stddev_pop(float4) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2727, proisagg="t");

 CREATE FUNCTION stddev_pop(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2728, proisagg="t");

 CREATE FUNCTION stddev_pop("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2729, proisagg="t");

 CREATE FUNCTION stddev_samp(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2712, proisagg="t");

 CREATE FUNCTION stddev_samp(int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2713, proisagg="t");

 CREATE FUNCTION stddev_samp(int2) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2714, proisagg="t");

 CREATE FUNCTION stddev_samp(float4) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2715, proisagg="t");

 CREATE FUNCTION stddev_samp(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2716, proisagg="t");

 CREATE FUNCTION stddev_samp("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2717, proisagg="t");

 CREATE FUNCTION stddev(int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2154, proisagg="t");

 CREATE FUNCTION stddev(int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2155, proisagg="t");

 CREATE FUNCTION stddev(int2) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2156, proisagg="t");

 CREATE FUNCTION stddev(float4) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2157, proisagg="t");

 CREATE FUNCTION stddev(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2158, proisagg="t");

 CREATE FUNCTION stddev("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2159, proisagg="t");

-- MPP Aggregate -- array_sum -- special for prospective customer. 
 CREATE FUNCTION array_sum(_int4) RETURNS _int4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=6013, proisagg="t");

 CREATE FUNCTION regr_count(float8, float8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2818, proisagg="t");

 CREATE FUNCTION regr_sxx(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2819, proisagg="t");

 CREATE FUNCTION regr_syy(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2820, proisagg="t");

 CREATE FUNCTION regr_sxy(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2821, proisagg="t");

 CREATE FUNCTION regr_avgx(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2822, proisagg="t");

 CREATE FUNCTION regr_avgy(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2823, proisagg="t");

 CREATE FUNCTION regr_r2(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2824, proisagg="t");

 CREATE FUNCTION regr_slope(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2825, proisagg="t");

 CREATE FUNCTION regr_intercept(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2826, proisagg="t");

 CREATE FUNCTION covar_pop(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2827, proisagg="t");

 CREATE FUNCTION covar_samp(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2828, proisagg="t");

 CREATE FUNCTION corr(float8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2829, proisagg="t");

 CREATE FUNCTION text_pattern_lt(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_lt' WITH (OID=2160);

 CREATE FUNCTION text_pattern_le(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_le' WITH (OID=2161);

 CREATE FUNCTION text_pattern_eq(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texteq' WITH (OID=2162);

 CREATE FUNCTION text_pattern_ge(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_ge' WITH (OID=2163);

 CREATE FUNCTION text_pattern_gt(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_gt' WITH (OID=2164);

 CREATE FUNCTION text_pattern_ne(text, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textne' WITH (OID=2165);

 CREATE FUNCTION bttext_pattern_cmp(text, text) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttext_pattern_cmp' WITH (OID=2166);

-- Window functions (similar to aggregates) 
 CREATE FUNCTION row_number() RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7000, proiswin="t");
-- #define ROW_NUMBER_OID 7000
-- #define ROW_NUMBER_TYPE 20

 CREATE FUNCTION rank() RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7001, proiswin="t");

 CREATE FUNCTION dense_rank() RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7002, proiswin="t");

 CREATE FUNCTION percent_rank() RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7003, proiswin="t");

 CREATE FUNCTION cume_dist() RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7004, proiswin="t");
-- #define CUME_DIST_OID 7004

 CREATE FUNCTION ntile(int4) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7005, proiswin="t");

 CREATE FUNCTION ntile(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7006, proiswin="t");

 CREATE FUNCTION ntile("numeric") RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7007, proiswin="t");

--  XXX for now, window functions with arguments, just cite numeric = 1700, need to overload. 
 CREATE FUNCTION first_value(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7017, proiswin="t");

 CREATE FUNCTION first_value("char") RETURNS "char" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7018, proiswin="t");

 CREATE FUNCTION first_value(cidr) RETURNS cidr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7019, proiswin="t");

 CREATE FUNCTION first_value(circle) RETURNS circle LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7020, proiswin="t");

 CREATE FUNCTION first_value(float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7021, proiswin="t");

 CREATE FUNCTION first_value(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7022, proiswin="t");

 CREATE FUNCTION first_value(inet) RETURNS inet LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7023, proiswin="t");

 CREATE FUNCTION first_value("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7024, proiswin="t");

 CREATE FUNCTION first_value(line) RETURNS line LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7025, proiswin="t");

 CREATE FUNCTION first_value(lseg) RETURNS lseg LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7026, proiswin="t");

 CREATE FUNCTION first_value(macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7027, proiswin="t");

 CREATE FUNCTION first_value(int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7028, proiswin="t");

 CREATE FUNCTION first_value(int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7029, proiswin="t");

 CREATE FUNCTION first_value(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7030, proiswin="t");

 CREATE FUNCTION first_value(money) RETURNS money LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7031, proiswin="t");

 CREATE FUNCTION first_value(name) RETURNS name LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7032, proiswin="t");

 CREATE FUNCTION first_value("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7033, proiswin="t");

 CREATE FUNCTION first_value(oid) RETURNS oid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7034, proiswin="t");

 CREATE FUNCTION first_value(path) RETURNS path LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7035, proiswin="t");

 CREATE FUNCTION first_value(point) RETURNS point LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7036, proiswin="t");

 CREATE FUNCTION first_value(polygon) RETURNS polygon LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7037, proiswin="t");

 CREATE FUNCTION first_value(reltime) RETURNS reltime LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7038, proiswin="t");

 CREATE FUNCTION first_value(text) RETURNS text LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7039, proiswin="t");

 CREATE FUNCTION first_value(tid) RETURNS tid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7040, proiswin="t");

 CREATE FUNCTION first_value("time") RETURNS "time" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7041, proiswin="t");

 CREATE FUNCTION first_value("timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7042, proiswin="t");

 CREATE FUNCTION first_value(timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7043, proiswin="t");

 CREATE FUNCTION first_value(timetz) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7044, proiswin="t");

 CREATE FUNCTION first_value(varbit) RETURNS varbit LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7045, proiswin="t");

 CREATE FUNCTION first_value("varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7046, proiswin="t");

 CREATE FUNCTION first_value(xid) RETURNS xid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7047, proiswin="t");

 CREATE FUNCTION first_value(bytea) RETURNS bytea LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7232, proiswin="t");

 CREATE FUNCTION first_value("bit") RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7256, proiswin="t");

 CREATE FUNCTION first_value(box) RETURNS box LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7272, proiswin="t");

 CREATE FUNCTION first_value(anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7288, proiswin="t");

 CREATE FUNCTION last_value(int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7012, proiswin="t");

 CREATE FUNCTION last_value(int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7013, proiswin="t");

 CREATE FUNCTION last_value(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7014, proiswin="t");

 CREATE FUNCTION last_value("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7015, proiswin="t");

 CREATE FUNCTION last_value(text) RETURNS text LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7016, proiswin="t");

 CREATE FUNCTION last_value(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7063, proiswin="t");

 CREATE FUNCTION last_value("char") RETURNS "char" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7072, proiswin="t");

 CREATE FUNCTION last_value(cidr) RETURNS cidr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7073, proiswin="t");

 CREATE FUNCTION last_value(circle) RETURNS circle LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7048, proiswin="t");

 CREATE FUNCTION last_value(float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7049, proiswin="t");

 CREATE FUNCTION last_value(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7050, proiswin="t");

 CREATE FUNCTION last_value(inet) RETURNS inet LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7051, proiswin="t");

 CREATE FUNCTION last_value("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7052, proiswin="t");

 CREATE FUNCTION last_value(line) RETURNS line LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7053, proiswin="t");

 CREATE FUNCTION last_value(lseg) RETURNS lseg LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7054, proiswin="t");

 CREATE FUNCTION last_value(macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7055, proiswin="t");

 CREATE FUNCTION last_value(money) RETURNS money LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7056, proiswin="t");

 CREATE FUNCTION last_value(name) RETURNS name LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7057, proiswin="t");

 CREATE FUNCTION last_value(oid) RETURNS oid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7058, proiswin="t");

 CREATE FUNCTION last_value(path) RETURNS path LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7059, proiswin="t");

 CREATE FUNCTION last_value(point) RETURNS point LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7060, proiswin="t");

 CREATE FUNCTION last_value(polygon) RETURNS polygon LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7061, proiswin="t");

 CREATE FUNCTION last_value(reltime) RETURNS reltime LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7062, proiswin="t");

 CREATE FUNCTION last_value(tid) RETURNS tid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7064, proiswin="t");

 CREATE FUNCTION last_value("time") RETURNS "time" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7065, proiswin="t");

 CREATE FUNCTION last_value("timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7066, proiswin="t");

 CREATE FUNCTION last_value(timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7067, proiswin="t");

 CREATE FUNCTION last_value(timetz) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7068, proiswin="t");

 CREATE FUNCTION last_value(varbit) RETURNS varbit LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7069, proiswin="t");

 CREATE FUNCTION last_value("varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7070, proiswin="t");

 CREATE FUNCTION last_value(xid) RETURNS xid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7071, proiswin="t");

 CREATE FUNCTION last_value(bytea) RETURNS bytea LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7238, proiswin="t");

 CREATE FUNCTION last_value("bit") RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7258, proiswin="t");

 CREATE FUNCTION last_value(box) RETURNS box LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7274, proiswin="t");

 CREATE FUNCTION last_value(anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7290, proiswin="t");

 CREATE FUNCTION lag(bool, int8, bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7675, proiswin="t");

 CREATE FUNCTION lag(bool, int8) RETURNS bool LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7491, proiswin="t");

 CREATE FUNCTION lag(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7493, proiswin="t");

 CREATE FUNCTION lag("char", int8, "char") RETURNS "char" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7495, proiswin="t");

 CREATE FUNCTION lag("char", int8) RETURNS "char" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7497, proiswin="t");

 CREATE FUNCTION lag("char") RETURNS "char" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7499, proiswin="t");

 CREATE FUNCTION lag(cidr, int8, cidr) RETURNS cidr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7501, proiswin="t");

 CREATE FUNCTION lag(cidr, int8) RETURNS cidr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7503, proiswin="t");

 CREATE FUNCTION lag(cidr) RETURNS cidr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7505, proiswin="t");

 CREATE FUNCTION lag(circle, int8, circle) RETURNS circle LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7507, proiswin="t");

 CREATE FUNCTION lag(circle, int8) RETURNS circle LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7509, proiswin="t");

 CREATE FUNCTION lag(circle) RETURNS circle LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7511, proiswin="t");

 CREATE FUNCTION lag(float4, int8, float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7513, proiswin="t");

 CREATE FUNCTION lag(float4, int8) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7515, proiswin="t");

 CREATE FUNCTION lag(float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7517, proiswin="t");

 CREATE FUNCTION lag(float8, int8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7519, proiswin="t");

 CREATE FUNCTION lag(float8, int8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7521, proiswin="t");

 CREATE FUNCTION lag(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7523, proiswin="t");

 CREATE FUNCTION lag(inet, int8, inet) RETURNS inet LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7525, proiswin="t");

 CREATE FUNCTION lag(inet, int8) RETURNS inet LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7527, proiswin="t");

 CREATE FUNCTION lag(inet) RETURNS inet LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7529, proiswin="t");

 CREATE FUNCTION lag("interval", int8, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7531, proiswin="t");

 CREATE FUNCTION lag("interval", int8) RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7533, proiswin="t");

 CREATE FUNCTION lag("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7535, proiswin="t");

 CREATE FUNCTION lag(line, int8, line) RETURNS line LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7537, proiswin="t");

 CREATE FUNCTION lag(line, int8) RETURNS line LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7539, proiswin="t");

 CREATE FUNCTION lag(line) RETURNS line LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7541, proiswin="t");

 CREATE FUNCTION lag(lseg, int8, lseg) RETURNS lseg LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7543, proiswin="t");

 CREATE FUNCTION lag(lseg, int8) RETURNS lseg LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7545, proiswin="t");

 CREATE FUNCTION lag(lseg) RETURNS lseg LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7547, proiswin="t");

 CREATE FUNCTION lag(macaddr, int8, macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7549, proiswin="t");

 CREATE FUNCTION lag(macaddr, int8) RETURNS macaddr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7551, proiswin="t");

 CREATE FUNCTION lag(macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7553, proiswin="t");

 CREATE FUNCTION lag(int2, int8, int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7555, proiswin="t");

 CREATE FUNCTION lag(int2, int8) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7557, proiswin="t");

 CREATE FUNCTION lag(int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7559, proiswin="t");

 CREATE FUNCTION lag(int4, int8, int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7561, proiswin="t");

 CREATE FUNCTION lag(int4, int8) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7563, proiswin="t");

 CREATE FUNCTION lag(int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7565, proiswin="t");

 CREATE FUNCTION lag(int8, int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7567, proiswin="t");

 CREATE FUNCTION lag(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7569, proiswin="t");

 CREATE FUNCTION lag(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7571, proiswin="t");

 CREATE FUNCTION lag(money, int8, money) RETURNS money LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7573, proiswin="t");

 CREATE FUNCTION lag(money, int8) RETURNS money LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7575, proiswin="t");

 CREATE FUNCTION lag(money) RETURNS money LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7577, proiswin="t");

 CREATE FUNCTION lag(name, int8, name) RETURNS name LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7579, proiswin="t");

 CREATE FUNCTION lag(name, int8) RETURNS name LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7581, proiswin="t");

 CREATE FUNCTION lag(name) RETURNS name LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7583, proiswin="t");

 CREATE FUNCTION lag("numeric", int8, "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7585, proiswin="t");

 CREATE FUNCTION lag("numeric", int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7587, proiswin="t");

 CREATE FUNCTION lag("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7589, proiswin="t");

 CREATE FUNCTION lag(oid, int8, oid) RETURNS oid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7591, proiswin="t");

 CREATE FUNCTION lag(oid, int8) RETURNS oid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7593, proiswin="t");

 CREATE FUNCTION lag(oid) RETURNS oid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7595, proiswin="t");

 CREATE FUNCTION lag(path, int8, path) RETURNS path LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7597, proiswin="t");

 CREATE FUNCTION lag(path, int8) RETURNS path LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7599, proiswin="t");

 CREATE FUNCTION lag(path) RETURNS path LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7601, proiswin="t");

 CREATE FUNCTION lag(point, int8, point) RETURNS point LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7603, proiswin="t");

 CREATE FUNCTION lag(point, int8) RETURNS point LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7605, proiswin="t");

 CREATE FUNCTION lag(point) RETURNS point LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7607, proiswin="t");

 CREATE FUNCTION lag(polygon, int8, polygon) RETURNS polygon LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7609, proiswin="t");

 CREATE FUNCTION lag(polygon, int8) RETURNS polygon LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7611, proiswin="t");

 CREATE FUNCTION lag(polygon) RETURNS polygon LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7613, proiswin="t");

 CREATE FUNCTION lag(reltime, int8, reltime) RETURNS reltime LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7615, proiswin="t");

 CREATE FUNCTION lag(reltime, int8) RETURNS reltime LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7617, proiswin="t");

 CREATE FUNCTION lag(reltime) RETURNS reltime LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7619, proiswin="t");

 CREATE FUNCTION lag(text, int8, text) RETURNS text LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7621, proiswin="t");

 CREATE FUNCTION lag(text, int8) RETURNS text LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7623, proiswin="t");

 CREATE FUNCTION lag(text) RETURNS text LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7625, proiswin="t");

 CREATE FUNCTION lag(tid, int8, tid) RETURNS tid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7627, proiswin="t");

 CREATE FUNCTION lag(tid, int8) RETURNS tid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7629, proiswin="t");

 CREATE FUNCTION lag(tid) RETURNS tid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7631, proiswin="t");

 CREATE FUNCTION lag("time", int8, "time") RETURNS "time" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7633, proiswin="t");

 CREATE FUNCTION lag("time", int8) RETURNS "time" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7635, proiswin="t");

 CREATE FUNCTION lag("time") RETURNS "time" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7637, proiswin="t");

 CREATE FUNCTION lag("timestamp", int8, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7639, proiswin="t");

 CREATE FUNCTION lag("timestamp", int8) RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7641, proiswin="t");

 CREATE FUNCTION lag("timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7643, proiswin="t");

 CREATE FUNCTION lag(timestamptz, int8, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7645, proiswin="t");

 CREATE FUNCTION lag(timestamptz, int8) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7647, proiswin="t");

 CREATE FUNCTION lag(timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7649, proiswin="t");

 CREATE FUNCTION lag(timetz, int8, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7651, proiswin="t");

 CREATE FUNCTION lag(timetz, int8) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7653, proiswin="t");

 CREATE FUNCTION lag(timetz) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7655, proiswin="t");

 CREATE FUNCTION lag(varbit, int8, varbit) RETURNS varbit LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7657, proiswin="t");

 CREATE FUNCTION lag(varbit, int8) RETURNS varbit LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7659, proiswin="t");

 CREATE FUNCTION lag(varbit) RETURNS varbit LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7661, proiswin="t");

 CREATE FUNCTION lag("varchar", int8, "varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7663, proiswin="t");

 CREATE FUNCTION lag("varchar", int8) RETURNS "varchar" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7665, proiswin="t");

 CREATE FUNCTION lag("varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7667, proiswin="t");

 CREATE FUNCTION lag(xid, int8, xid) RETURNS xid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7669, proiswin="t");

 CREATE FUNCTION lag(xid, int8) RETURNS xid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7671, proiswin="t");

 CREATE FUNCTION lag(xid) RETURNS xid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7673, proiswin="t");

 CREATE FUNCTION lag(anyarray, int8, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7211, proiswin="t");

 CREATE FUNCTION lag(anyarray, int8) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7212, proiswin="t");

 CREATE FUNCTION lag(anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7213, proiswin="t");

 CREATE FUNCTION lag(bytea) RETURNS bytea LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7226, proiswin="t");

 CREATE FUNCTION lag(bytea, int8) RETURNS bytea LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7228, proiswin="t");

 CREATE FUNCTION lag(bytea, int8, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7230, proiswin="t");

 CREATE FUNCTION lag("bit") RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7250, proiswin="t");

 CREATE FUNCTION lag("bit", int8) RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7252, proiswin="t");

 CREATE FUNCTION lag("bit", int8, "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7254, proiswin="t");

 CREATE FUNCTION lag(box) RETURNS box LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7266, proiswin="t");

 CREATE FUNCTION lag(box, int8) RETURNS box LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7268, proiswin="t");

 CREATE FUNCTION lag(box, int8, box) RETURNS box LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7270, proiswin="t");

 CREATE FUNCTION lead(int4, int8, int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7011, proiswin="t");

 CREATE FUNCTION lead(int4, int8) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7074, proiswin="t");

 CREATE FUNCTION lead(int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7075, proiswin="t");

 CREATE FUNCTION lead(bool, int8, bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7310, proiswin="t");

 CREATE FUNCTION lead(bool, int8) RETURNS bool LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7312, proiswin="t");

 CREATE FUNCTION lead(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7314, proiswin="t");

 CREATE FUNCTION lead("char", int8, "char") RETURNS "char" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7316, proiswin="t");

 CREATE FUNCTION lead("char", int8) RETURNS "char" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7318, proiswin="t");

 CREATE FUNCTION lead("char") RETURNS "char" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7320, proiswin="t");

 CREATE FUNCTION lead(cidr, int8, cidr) RETURNS cidr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7322, proiswin="t");

 CREATE FUNCTION lead(cidr, int8) RETURNS cidr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7324, proiswin="t");

 CREATE FUNCTION lead(cidr) RETURNS cidr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7326, proiswin="t");

 CREATE FUNCTION lead(circle, int8, circle) RETURNS circle LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7328, proiswin="t");

 CREATE FUNCTION lead(circle, int8) RETURNS circle LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7330, proiswin="t");

 CREATE FUNCTION lead(circle) RETURNS circle LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7332, proiswin="t");

 CREATE FUNCTION lead(float4, int8, float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7334, proiswin="t");

 CREATE FUNCTION lead(float4, int8) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7336, proiswin="t");

 CREATE FUNCTION lead(float4) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7338, proiswin="t");

 CREATE FUNCTION lead(float8, int8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7340, proiswin="t");

 CREATE FUNCTION lead(float8, int8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7342, proiswin="t");

 CREATE FUNCTION lead(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7344, proiswin="t");

 CREATE FUNCTION lead(inet, int8, inet) RETURNS inet LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7346, proiswin="t");

 CREATE FUNCTION lead(inet, int8) RETURNS inet LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7348, proiswin="t");

 CREATE FUNCTION lead(inet) RETURNS inet LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7350, proiswin="t");

 CREATE FUNCTION lead("interval", int8, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7352, proiswin="t");

 CREATE FUNCTION lead("interval", int8) RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7354, proiswin="t");

 CREATE FUNCTION lead("interval") RETURNS "interval" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7356, proiswin="t");

 CREATE FUNCTION lead(line, int8, line) RETURNS line LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7358, proiswin="t");

 CREATE FUNCTION lead(line, int8) RETURNS line LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7360, proiswin="t");

 CREATE FUNCTION lead(line) RETURNS line LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7362, proiswin="t");

 CREATE FUNCTION lead(lseg, int8, lseg) RETURNS lseg LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7364, proiswin="t");

 CREATE FUNCTION lead(lseg, int8) RETURNS lseg LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7366, proiswin="t");

 CREATE FUNCTION lead(lseg) RETURNS lseg LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7368, proiswin="t");

 CREATE FUNCTION lead(macaddr, int8, macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7370, proiswin="t");

 CREATE FUNCTION lead(macaddr, int8) RETURNS macaddr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7372, proiswin="t");

 CREATE FUNCTION lead(macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7374, proiswin="t");

 CREATE FUNCTION lead(int2, int8, int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7376, proiswin="t");

 CREATE FUNCTION lead(int2, int8) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7378, proiswin="t");

 CREATE FUNCTION lead(int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7380, proiswin="t");

 CREATE FUNCTION lead(int8, int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7382, proiswin="t");

 CREATE FUNCTION lead(int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7384, proiswin="t");

 CREATE FUNCTION lead(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7386, proiswin="t");

 CREATE FUNCTION lead(money, int8, money) RETURNS money LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7388, proiswin="t");

 CREATE FUNCTION lead(money, int8) RETURNS money LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7390, proiswin="t");

 CREATE FUNCTION lead(money) RETURNS money LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7392, proiswin="t");

 CREATE FUNCTION lead(name, int8, name) RETURNS name LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7394, proiswin="t");

 CREATE FUNCTION lead(name, int8) RETURNS name LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7396, proiswin="t");

 CREATE FUNCTION lead(name) RETURNS name LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7398, proiswin="t");

 CREATE FUNCTION lead("numeric", int8, "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7400, proiswin="t");

 CREATE FUNCTION lead("numeric", int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7402, proiswin="t");

 CREATE FUNCTION lead("numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7404, proiswin="t");

 CREATE FUNCTION lead(oid, int8, oid) RETURNS oid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7406, proiswin="t");

 CREATE FUNCTION lead(oid, int8) RETURNS oid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7408, proiswin="t");

 CREATE FUNCTION lead(oid) RETURNS oid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7410, proiswin="t");

 CREATE FUNCTION lead(path, int8, path) RETURNS path LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7412, proiswin="t");

 CREATE FUNCTION lead(path, int8) RETURNS path LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7414, proiswin="t");

 CREATE FUNCTION lead(path) RETURNS path LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7416, proiswin="t");

 CREATE FUNCTION lead(point, int8, point) RETURNS point LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7418, proiswin="t");

 CREATE FUNCTION lead(point, int8) RETURNS point LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7420, proiswin="t");

 CREATE FUNCTION lead(point) RETURNS point LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7422, proiswin="t");

 CREATE FUNCTION lead(polygon, int8, polygon) RETURNS polygon LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7424, proiswin="t");

 CREATE FUNCTION lead(polygon, int8) RETURNS polygon LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7426, proiswin="t");

 CREATE FUNCTION lead(polygon) RETURNS polygon LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7428, proiswin="t");

 CREATE FUNCTION lead(reltime, int8, reltime) RETURNS reltime LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7430, proiswin="t");

 CREATE FUNCTION lead(reltime, int8) RETURNS reltime LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7432, proiswin="t");

 CREATE FUNCTION lead(reltime) RETURNS reltime LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7434, proiswin="t");

 CREATE FUNCTION lead(text, int8, text) RETURNS text LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7436, proiswin="t");

 CREATE FUNCTION lead(text, int8) RETURNS text LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7438, proiswin="t");

 CREATE FUNCTION lead(text) RETURNS text LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7440, proiswin="t");

 CREATE FUNCTION lead(tid, int8, tid) RETURNS tid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7442, proiswin="t");

 CREATE FUNCTION lead(tid, int8) RETURNS tid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7444, proiswin="t");

 CREATE FUNCTION lead(tid) RETURNS tid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7446, proiswin="t");

 CREATE FUNCTION lead("time", int8, "time") RETURNS "time" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7448, proiswin="t");

 CREATE FUNCTION lead("time", int8) RETURNS "time" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7450, proiswin="t");

 CREATE FUNCTION lead("time") RETURNS "time" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7452, proiswin="t");

 CREATE FUNCTION lead("timestamp", int8, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7454, proiswin="t");

 CREATE FUNCTION lead("timestamp", int8) RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7456, proiswin="t");

 CREATE FUNCTION lead("timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7458, proiswin="t");

 CREATE FUNCTION lead(timestamptz, int8, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7460, proiswin="t");

 CREATE FUNCTION lead(timestamptz, int8) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7462, proiswin="t");

 CREATE FUNCTION lead(timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7464, proiswin="t");

 CREATE FUNCTION lead(timetz, int8, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7466, proiswin="t");

 CREATE FUNCTION lead(timetz, int8) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7468, proiswin="t");

 CREATE FUNCTION lead(timetz) RETURNS timetz LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7470, proiswin="t");

 CREATE FUNCTION lead(varbit, int8, varbit) RETURNS varbit LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7472, proiswin="t");

 CREATE FUNCTION lead(varbit, int8) RETURNS varbit LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7474, proiswin="t");

 CREATE FUNCTION lead(varbit) RETURNS varbit LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7476, proiswin="t");

 CREATE FUNCTION lead("varchar", int8, "varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7478, proiswin="t");

 CREATE FUNCTION lead("varchar", int8) RETURNS "varchar" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7480, proiswin="t");

 CREATE FUNCTION lead("varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7482, proiswin="t");

 CREATE FUNCTION lead(xid, int8, xid) RETURNS xid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7484, proiswin="t");

 CREATE FUNCTION lead(xid, int8) RETURNS xid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7486, proiswin="t");

 CREATE FUNCTION lead(xid) RETURNS xid LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7488, proiswin="t");

 CREATE FUNCTION lead(anyarray, int8, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7214, proiswin="t");

 CREATE FUNCTION lead(anyarray, int8) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7215, proiswin="t");

 CREATE FUNCTION lead(anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7216, proiswin="t");

 CREATE FUNCTION lead(bytea) RETURNS bytea LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7220, proiswin="t");

 CREATE FUNCTION lead(bytea, int8) RETURNS bytea LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7222, proiswin="t");

 CREATE FUNCTION lead(bytea, int8, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7224, proiswin="t");

 CREATE FUNCTION lead("bit") RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7244, proiswin="t");

 CREATE FUNCTION lead("bit", int8) RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7246, proiswin="t");

 CREATE FUNCTION lead("bit", int8, "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7248, proiswin="t");

 CREATE FUNCTION lead(box) RETURNS box LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7260, proiswin="t");

 CREATE FUNCTION lead(box, int8) RETURNS box LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7262, proiswin="t");

 CREATE FUNCTION lead(box, int8, box) RETURNS box LANGUAGE internal IMMUTABLE AS 'window_dummy' WITH (OID=7264, proiswin="t");

-- We use the same procedures here as above since the types are binary compatible. 
 CREATE FUNCTION bpchar_pattern_lt(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_lt' WITH (OID=2174);

 CREATE FUNCTION bpchar_pattern_le(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_le' WITH (OID=2175);

 CREATE FUNCTION bpchar_pattern_eq(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'texteq' WITH (OID=2176);

 CREATE FUNCTION bpchar_pattern_ge(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_ge' WITH (OID=2177);

 CREATE FUNCTION bpchar_pattern_gt(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'text_pattern_gt' WITH (OID=2178);

 CREATE FUNCTION bpchar_pattern_ne(bpchar, bpchar) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'textne' WITH (OID=2179);

 CREATE FUNCTION btbpchar_pattern_cmp(bpchar, bpchar) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bttext_pattern_cmp' WITH (OID=2180);

 CREATE FUNCTION name_pattern_lt(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_lt' WITH (OID=2181);

 CREATE FUNCTION name_pattern_le(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_le' WITH (OID=2182);

 CREATE FUNCTION name_pattern_eq(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_eq' WITH (OID=2183);

 CREATE FUNCTION name_pattern_ge(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_ge' WITH (OID=2184);

 CREATE FUNCTION name_pattern_gt(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_gt' WITH (OID=2185);

 CREATE FUNCTION name_pattern_ne(name, name) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'name_pattern_ne' WITH (OID=2186);

 CREATE FUNCTION btname_pattern_cmp(name, name) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btname_pattern_cmp' WITH (OID=2187);

 CREATE FUNCTION btint48cmp(int4, int8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint48cmp' WITH (OID=2188);

 CREATE FUNCTION btint84cmp(int8, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint84cmp' WITH (OID=2189);

 CREATE FUNCTION btint24cmp(int2, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint24cmp' WITH (OID=2190);

 CREATE FUNCTION btint42cmp(int4, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint42cmp' WITH (OID=2191);

 CREATE FUNCTION btint28cmp(int2, int8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint28cmp' WITH (OID=2192);

 CREATE FUNCTION btint82cmp(int8, int2) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btint82cmp' WITH (OID=2193);

 CREATE FUNCTION btfloat48cmp(float4, float8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btfloat48cmp' WITH (OID=2194);

 CREATE FUNCTION btfloat84cmp(float8, float4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'btfloat84cmp' WITH (OID=2195);

 CREATE FUNCTION pg_options_to_table(IN options_array _text, OUT option_name text, OUT option_value text) RETURNS SETOF pg_catalog.record LANGUAGE internal STABLE STRICT AS 'pg_options_to_table' WITH (OID=2896, DESCRIPTION="convert generic options array to name/value table");

 CREATE FUNCTION regprocedurein(cstring) RETURNS regprocedure LANGUAGE internal STABLE STRICT AS 'regprocedurein' WITH (OID=2212, DESCRIPTION="I/O");

 CREATE FUNCTION regprocedureout(regprocedure) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'regprocedureout' WITH (OID=2213, DESCRIPTION="I/O");

 CREATE FUNCTION regoperin(cstring) RETURNS regoper LANGUAGE internal STABLE STRICT AS 'regoperin' WITH (OID=2214, DESCRIPTION="I/O");

 CREATE FUNCTION regoperout(regoper) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'regoperout' WITH (OID=2215, DESCRIPTION="I/O");

 CREATE FUNCTION regoperatorin(cstring) RETURNS regoperator LANGUAGE internal STABLE STRICT AS 'regoperatorin' WITH (OID=2216, DESCRIPTION="I/O");

 CREATE FUNCTION regoperatorout(regoperator) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'regoperatorout' WITH (OID=2217, DESCRIPTION="I/O");

 CREATE FUNCTION regclassin(cstring) RETURNS regclass LANGUAGE internal STABLE STRICT AS 'regclassin' WITH (OID=2218, DESCRIPTION="I/O");

 CREATE FUNCTION regclassout(regclass) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'regclassout' WITH (OID=2219, DESCRIPTION="I/O");

 CREATE FUNCTION regtypein(cstring) RETURNS regtype LANGUAGE internal STABLE STRICT AS 'regtypein' WITH (OID=2220, DESCRIPTION="I/O");

 CREATE FUNCTION regtypeout(regtype) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'regtypeout' WITH (OID=2221, DESCRIPTION="I/O");

 CREATE FUNCTION regclass(text) RETURNS regclass LANGUAGE internal STABLE STRICT AS 'text_regclass' WITH (OID=1079, DESCRIPTION="convert text to regclass");

 CREATE FUNCTION fmgr_internal_validator(oid) RETURNS void LANGUAGE internal STABLE STRICT AS 'fmgr_internal_validator' WITH (OID=2246, DESCRIPTION="(internal)");

 CREATE FUNCTION fmgr_c_validator(oid) RETURNS void LANGUAGE internal STABLE STRICT AS 'fmgr_c_validator' WITH (OID=2247, DESCRIPTION="(internal)");

 CREATE FUNCTION fmgr_sql_validator(oid) RETURNS void LANGUAGE internal STABLE STRICT AS 'fmgr_sql_validator' WITH (OID=2248, DESCRIPTION="(internal)");

 CREATE FUNCTION has_database_privilege(name, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_database_privilege_name_name' WITH (OID=2250, DESCRIPTION="user privilege on database by username, database name");

 CREATE FUNCTION has_database_privilege(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_database_privilege_name_id' WITH (OID=2251, DESCRIPTION="user privilege on database by username, database oid");

 CREATE FUNCTION has_database_privilege(oid, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_database_privilege_id_name' WITH (OID=2252, DESCRIPTION="user privilege on database by user oid, database name");

 CREATE FUNCTION has_database_privilege(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_database_privilege_id_id' WITH (OID=2253, DESCRIPTION="user privilege on database by user oid, database oid");

 CREATE FUNCTION has_database_privilege(text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_database_privilege_name' WITH (OID=2254, DESCRIPTION="current user privilege on database by database name");

 CREATE FUNCTION has_database_privilege(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_database_privilege_id' WITH (OID=2255, DESCRIPTION="current user privilege on database by database oid");

 CREATE FUNCTION has_function_privilege(name, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_function_privilege_name_name' WITH (OID=2256, DESCRIPTION="user privilege on function by username, function name");

 CREATE FUNCTION has_function_privilege(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_function_privilege_name_id' WITH (OID=2257, DESCRIPTION="user privilege on function by username, function oid");

 CREATE FUNCTION has_function_privilege(oid, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_function_privilege_id_name' WITH (OID=2258, DESCRIPTION="user privilege on function by user oid, function name");

 CREATE FUNCTION has_function_privilege(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_function_privilege_id_id' WITH (OID=2259, DESCRIPTION="user privilege on function by user oid, function oid");

 CREATE FUNCTION has_function_privilege(text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_function_privilege_name' WITH (OID=2260, DESCRIPTION="current user privilege on function by function name");

 CREATE FUNCTION has_function_privilege(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_function_privilege_id' WITH (OID=2261, DESCRIPTION="current user privilege on function by function oid");

 CREATE FUNCTION has_language_privilege(name, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_language_privilege_name_name' WITH (OID=2262, DESCRIPTION="user privilege on language by username, language name");

 CREATE FUNCTION has_language_privilege(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_language_privilege_name_id' WITH (OID=2263, DESCRIPTION="user privilege on language by username, language oid");

 CREATE FUNCTION has_language_privilege(oid, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_language_privilege_id_name' WITH (OID=2264, DESCRIPTION="user privilege on language by user oid, language name");

 CREATE FUNCTION has_language_privilege(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_language_privilege_id_id' WITH (OID=2265, DESCRIPTION="user privilege on language by user oid, language oid");

 CREATE FUNCTION has_language_privilege(text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_language_privilege_name' WITH (OID=2266, DESCRIPTION="current user privilege on language by language name");

 CREATE FUNCTION has_language_privilege(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_language_privilege_id' WITH (OID=2267, DESCRIPTION="current user privilege on language by language oid");

 CREATE FUNCTION has_schema_privilege(name, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_schema_privilege_name_name' WITH (OID=2268, DESCRIPTION="user privilege on schema by username, schema name");

 CREATE FUNCTION has_schema_privilege(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_schema_privilege_name_id' WITH (OID=2269, DESCRIPTION="user privilege on schema by username, schema oid");

 CREATE FUNCTION has_schema_privilege(oid, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_schema_privilege_id_name' WITH (OID=2270, DESCRIPTION="user privilege on schema by user oid, schema name");

 CREATE FUNCTION has_schema_privilege(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_schema_privilege_id_id' WITH (OID=2271, DESCRIPTION="user privilege on schema by user oid, schema oid");

 CREATE FUNCTION has_schema_privilege(text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_schema_privilege_name' WITH (OID=2272, DESCRIPTION="current user privilege on schema by schema name");

 CREATE FUNCTION has_schema_privilege(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_schema_privilege_id' WITH (OID=2273, DESCRIPTION="current user privilege on schema by schema oid");

 CREATE FUNCTION has_tablespace_privilege(name, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_tablespace_privilege_name_name' WITH (OID=2390, DESCRIPTION="user privilege on tablespace by username, tablespace name");

 CREATE FUNCTION has_tablespace_privilege(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_tablespace_privilege_name_id' WITH (OID=2391, DESCRIPTION="user privilege on tablespace by username, tablespace oid");

 CREATE FUNCTION has_tablespace_privilege(oid, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_tablespace_privilege_id_name' WITH (OID=2392, DESCRIPTION="user privilege on tablespace by user oid, tablespace name");

 CREATE FUNCTION has_tablespace_privilege(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_tablespace_privilege_id_id' WITH (OID=2393, DESCRIPTION="user privilege on tablespace by user oid, tablespace oid");

 CREATE FUNCTION has_tablespace_privilege(text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_tablespace_privilege_name' WITH (OID=2394, DESCRIPTION="current user privilege on tablespace by tablespace name");

 CREATE FUNCTION has_tablespace_privilege(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_tablespace_privilege_id' WITH (OID=2395, DESCRIPTION="current user privilege on tablespace by tablespace oid");

 CREATE FUNCTION has_foreign_data_wrapper_privilege(name, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_foreign_data_wrapper_privilege_name_name' WITH (OID=3112, DESCRIPTION="user privilege on foreign data wrapper by username, foreign data wrapper name");

 CREATE FUNCTION has_foreign_data_wrapper_privilege(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_foreign_data_wrapper_privilege_name_id' WITH (OID=3113, DESCRIPTION="user privilege on foreign data wrapper by username, foreign data wrapper oid");

 CREATE FUNCTION has_foreign_data_wrapper_privilege(oid, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_foreign_data_wrapper_privilege_id_name' WITH (OID=3114, DESCRIPTION="user privilege on foreign data wrapper by user oid, foreign data wrapper name");

 CREATE FUNCTION has_foreign_data_wrapper_privilege(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_foreign_data_wrapper_privilege_id_id' WITH (OID=3115, DESCRIPTION="user privilege on foreign data wrapper by user oid, foreign data wrapper oid");

 CREATE FUNCTION has_foreign_data_wrapper_privilege(text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_foreign_data_wrapper_privilege_name' WITH (OID=3116, DESCRIPTION="current user privilege on foreign data wrapper by foreign data wrapper name");

 CREATE FUNCTION has_foreign_data_wrapper_privilege(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_foreign_data_wrapper_privilege_id' WITH (OID=3117, DESCRIPTION="current user privilege on foreign data wrapper by foreign data wrapper oid");

 CREATE FUNCTION has_server_privilege(name, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_server_privilege_name_name' WITH (OID=3118, DESCRIPTION="user privilege on server by username, server name");

 CREATE FUNCTION has_server_privilege(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_server_privilege_name_id' WITH (OID=3119, DESCRIPTION="user privilege on server by username, server oid");

 CREATE FUNCTION has_server_privilege(oid, text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_server_privilege_id_name' WITH (OID=3120, DESCRIPTION="user privilege on server by user oid, server name");

 CREATE FUNCTION has_server_privilege(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_server_privilege_id_id' WITH (OID=3121, DESCRIPTION="user privilege on server by user oid, server oid");

 CREATE FUNCTION has_server_privilege(text, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_server_privilege_name' WITH (OID=3122, DESCRIPTION="current user privilege on server by server name");

 CREATE FUNCTION has_server_privilege(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'has_server_privilege_id' WITH (OID=3123, DESCRIPTION="current user privilege on server by server oid");

 CREATE FUNCTION pg_has_role(name, name, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_has_role_name_name' WITH (OID=2705, DESCRIPTION="user privilege on role by username, role name");

 CREATE FUNCTION pg_has_role(name, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_has_role_name_id' WITH (OID=2706, DESCRIPTION="user privilege on role by username, role oid");

 CREATE FUNCTION pg_has_role(oid, name, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_has_role_id_name' WITH (OID=2707, DESCRIPTION="user privilege on role by user oid, role name");

 CREATE FUNCTION pg_has_role(oid, oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_has_role_id_id' WITH (OID=2708, DESCRIPTION="user privilege on role by user oid, role oid");

 CREATE FUNCTION pg_has_role(name, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_has_role_name' WITH (OID=2709, DESCRIPTION="current user privilege on role by role name");

 CREATE FUNCTION pg_has_role(oid, text) RETURNS bool LANGUAGE internal STABLE STRICT AS 'pg_has_role_id' WITH (OID=2710, DESCRIPTION="current user privilege on role by role oid");

 CREATE FUNCTION pg_column_size("any") RETURNS int4 LANGUAGE internal STABLE STRICT AS 'pg_column_size' WITH (OID=1269, DESCRIPTION="bytes required to store the value, perhaps with compression");

 CREATE FUNCTION pg_tablespace_size(oid) RETURNS int8 LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_tablespace_size_oid' WITH (OID=2322, DESCRIPTION="Calculate total disk space usage for the specified tablespace");

 CREATE FUNCTION pg_tablespace_size(name) RETURNS int8 LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_tablespace_size_name' WITH (OID=2323, DESCRIPTION="Calculate total disk space usage for the specified tablespace");

 CREATE FUNCTION pg_database_size(oid) RETURNS int8 LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_database_size_oid' WITH (OID=2324, DESCRIPTION="Calculate total disk space usage for the specified database");

 CREATE FUNCTION pg_database_size(name) RETURNS int8 LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_database_size_name' WITH (OID=2168, DESCRIPTION="Calculate total disk space usage for the specified database");

 CREATE FUNCTION pg_relation_size(oid) RETURNS int8 LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_relation_size_oid' WITH (OID=2325, DESCRIPTION="Calculate disk space usage for the specified table or index");

 CREATE FUNCTION pg_relation_size(text) RETURNS int8 LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_relation_size_name' WITH (OID=2289, DESCRIPTION="Calculate disk space usage for the specified table or index");

 CREATE FUNCTION pg_total_relation_size(oid) RETURNS int8 LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_total_relation_size_oid' WITH (OID=2286, DESCRIPTION="Calculate total disk space usage for the specified table and associated indexes and toast tables");

 CREATE FUNCTION pg_total_relation_size(text) RETURNS int8 LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_total_relation_size_name' WITH (OID=2287, DESCRIPTION="Calculate total disk space usage for the specified table and associated indexes and toast tables");

 CREATE FUNCTION pg_size_pretty(int8) RETURNS text LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_size_pretty' WITH (OID=2288, DESCRIPTION="Convert a long int to a human readable text using size units");

 CREATE FUNCTION gpdb_fdw_validator(_text, oid) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gpdb_fdw_validator' WITH (OID=2897);

 CREATE FUNCTION record_in(cstring, oid, int4) RETURNS record LANGUAGE internal VOLATILE STRICT AS 'record_in' WITH (OID=2290, DESCRIPTION="I/O");

 CREATE FUNCTION record_out(record) RETURNS cstring LANGUAGE internal VOLATILE STRICT AS 'record_out' WITH (OID=2291, DESCRIPTION="I/O");

 CREATE FUNCTION cstring_in(cstring) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'cstring_in' WITH (OID=2292, DESCRIPTION="I/O");

 CREATE FUNCTION cstring_out(cstring) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'cstring_out' WITH (OID=2293, DESCRIPTION="I/O");

 CREATE FUNCTION any_in(cstring) RETURNS "any" LANGUAGE internal IMMUTABLE STRICT AS 'any_in' WITH (OID=2294, DESCRIPTION="I/O");

 CREATE FUNCTION any_out("any") RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'any_out' WITH (OID=2295, DESCRIPTION="I/O");

 CREATE FUNCTION anyarray_in(cstring) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'anyarray_in' WITH (OID=2296, DESCRIPTION="I/O");

 CREATE FUNCTION anyarray_out(anyarray) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'anyarray_out' WITH (OID=2297, DESCRIPTION="I/O");

 CREATE FUNCTION void_in(cstring) RETURNS void LANGUAGE internal IMMUTABLE STRICT AS 'void_in' WITH (OID=2298, DESCRIPTION="I/O");

 CREATE FUNCTION void_out(void) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'void_out' WITH (OID=2299, DESCRIPTION="I/O");

 CREATE FUNCTION trigger_in(cstring) RETURNS trigger LANGUAGE internal IMMUTABLE STRICT AS 'trigger_in' WITH (OID=2300, DESCRIPTION="I/O");

 CREATE FUNCTION trigger_out(trigger) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'trigger_out' WITH (OID=2301, DESCRIPTION="I/O");

 CREATE FUNCTION language_handler_in(cstring) RETURNS language_handler LANGUAGE internal IMMUTABLE STRICT AS 'language_handler_in' WITH (OID=2302, DESCRIPTION="I/O");

 CREATE FUNCTION language_handler_out(language_handler) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'language_handler_out' WITH (OID=2303, DESCRIPTION="I/O");

 CREATE FUNCTION internal_in(cstring) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'internal_in' WITH (OID=2304, DESCRIPTION="I/O");

 CREATE FUNCTION internal_out(internal) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'internal_out' WITH (OID=2305, DESCRIPTION="I/O");

 CREATE FUNCTION opaque_in(cstring) RETURNS opaque LANGUAGE internal IMMUTABLE STRICT AS 'opaque_in' WITH (OID=2306, DESCRIPTION="I/O");

 CREATE FUNCTION opaque_out(opaque) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'opaque_out' WITH (OID=2307, DESCRIPTION="I/O");

 CREATE FUNCTION anyelement_in(cstring) RETURNS anyelement LANGUAGE internal IMMUTABLE STRICT AS 'anyelement_in' WITH (OID=2312, DESCRIPTION="I/O");

 CREATE FUNCTION anyelement_out(anyelement) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'anyelement_out' WITH (OID=2313, DESCRIPTION="I/O");

 CREATE FUNCTION shell_in(cstring) RETURNS opaque LANGUAGE internal IMMUTABLE STRICT AS 'shell_in' WITH (OID=2398, DESCRIPTION="I/O");

 CREATE FUNCTION shell_out(opaque) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'shell_out' WITH (OID=2399, DESCRIPTION="I/O");

 CREATE FUNCTION domain_in(cstring, oid, int4) RETURNS "any" LANGUAGE internal VOLATILE AS 'domain_in' WITH (OID=2597, DESCRIPTION="I/O");

 CREATE FUNCTION domain_recv(internal, oid, int4) RETURNS "any" LANGUAGE internal VOLATILE AS 'domain_recv' WITH (OID=2598, DESCRIPTION="I/O");

-- cryptographic
 CREATE FUNCTION md5(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'md5_text' WITH (OID=2311, DESCRIPTION="calculates md5 hash");

 CREATE FUNCTION md5(bytea) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'md5_bytea' WITH (OID=2321, DESCRIPTION="calculates md5 hash");

-- crosstype operations for date vs. timestamp and timestamptz 
 CREATE FUNCTION date_lt_timestamp(date, "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_lt_timestamp' WITH (OID=2338, DESCRIPTION="less-than");

 CREATE FUNCTION date_le_timestamp(date, "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_le_timestamp' WITH (OID=2339, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION date_eq_timestamp(date, "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_eq_timestamp' WITH (OID=2340, DESCRIPTION="equal");

 CREATE FUNCTION date_gt_timestamp(date, "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_gt_timestamp' WITH (OID=2341, DESCRIPTION="greater-than");

 CREATE FUNCTION date_ge_timestamp(date, "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_ge_timestamp' WITH (OID=2342, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION date_ne_timestamp(date, "timestamp") RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'date_ne_timestamp' WITH (OID=2343, DESCRIPTION="not equal");

 CREATE FUNCTION date_cmp_timestamp(date, "timestamp") RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'date_cmp_timestamp' WITH (OID=2344, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION date_lt_timestamptz(date, timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'date_lt_timestamptz' WITH (OID=2351, DESCRIPTION="less-than");

 CREATE FUNCTION date_le_timestamptz(date, timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'date_le_timestamptz' WITH (OID=2352, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION date_eq_timestamptz(date, timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'date_eq_timestamptz' WITH (OID=2353, DESCRIPTION="equal");

 CREATE FUNCTION date_gt_timestamptz(date, timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'date_gt_timestamptz' WITH (OID=2354, DESCRIPTION="greater-than");

 CREATE FUNCTION date_ge_timestamptz(date, timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'date_ge_timestamptz' WITH (OID=2355, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION date_ne_timestamptz(date, timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'date_ne_timestamptz' WITH (OID=2356, DESCRIPTION="not equal");

 CREATE FUNCTION date_cmp_timestamptz(date, timestamptz) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'date_cmp_timestamptz' WITH (OID=2357, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION timestamp_lt_date("timestamp", date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_lt_date' WITH (OID=2364, DESCRIPTION="less-than");

 CREATE FUNCTION timestamp_le_date("timestamp", date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_le_date' WITH (OID=2365, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION timestamp_eq_date("timestamp", date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_eq_date' WITH (OID=2366, DESCRIPTION="equal");

 CREATE FUNCTION timestamp_gt_date("timestamp", date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_gt_date' WITH (OID=2367, DESCRIPTION="greater-than");

 CREATE FUNCTION timestamp_ge_date("timestamp", date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ge_date' WITH (OID=2368, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION timestamp_ne_date("timestamp", date) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_ne_date' WITH (OID=2369, DESCRIPTION="not equal");

 CREATE FUNCTION timestamp_cmp_date("timestamp", date) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_cmp_date' WITH (OID=2370, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION timestamptz_lt_date(timestamptz, date) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_lt_date' WITH (OID=2377, DESCRIPTION="less-than");

 CREATE FUNCTION timestamptz_le_date(timestamptz, date) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_le_date' WITH (OID=2378, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION timestamptz_eq_date(timestamptz, date) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_eq_date' WITH (OID=2379, DESCRIPTION="equal");

 CREATE FUNCTION timestamptz_gt_date(timestamptz, date) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_gt_date' WITH (OID=2380, DESCRIPTION="greater-than");

 CREATE FUNCTION timestamptz_ge_date(timestamptz, date) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_ge_date' WITH (OID=2381, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION timestamptz_ne_date(timestamptz, date) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_ne_date' WITH (OID=2382, DESCRIPTION="not equal");

 CREATE FUNCTION timestamptz_cmp_date(timestamptz, date) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'timestamptz_cmp_date' WITH (OID=2383, DESCRIPTION="less-equal-greater");

-- crosstype operations for timestamp vs. timestamptz 
 CREATE FUNCTION timestamp_lt_timestamptz("timestamp", timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamp_lt_timestamptz' WITH (OID=2520, DESCRIPTION="less-than");

 CREATE FUNCTION timestamp_le_timestamptz("timestamp", timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamp_le_timestamptz' WITH (OID=2521, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION timestamp_eq_timestamptz("timestamp", timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamp_eq_timestamptz' WITH (OID=2522, DESCRIPTION="equal");

 CREATE FUNCTION timestamp_gt_timestamptz("timestamp", timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamp_gt_timestamptz' WITH (OID=2523, DESCRIPTION="greater-than");

 CREATE FUNCTION timestamp_ge_timestamptz("timestamp", timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamp_ge_timestamptz' WITH (OID=2524, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION timestamp_ne_timestamptz("timestamp", timestamptz) RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamp_ne_timestamptz' WITH (OID=2525, DESCRIPTION="not equal");

 CREATE FUNCTION timestamp_cmp_timestamptz("timestamp", timestamptz) RETURNS int4 LANGUAGE internal STABLE STRICT AS 'timestamp_cmp_timestamptz' WITH (OID=2526, DESCRIPTION="less-equal-greater");

 CREATE FUNCTION timestamptz_lt_timestamp(timestamptz, "timestamp") RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_lt_timestamp' WITH (OID=2527, DESCRIPTION="less-than");

 CREATE FUNCTION timestamptz_le_timestamp(timestamptz, "timestamp") RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_le_timestamp' WITH (OID=2528, DESCRIPTION="less-than-or-equal");

 CREATE FUNCTION timestamptz_eq_timestamp(timestamptz, "timestamp") RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_eq_timestamp' WITH (OID=2529, DESCRIPTION="equal");

 CREATE FUNCTION timestamptz_gt_timestamp(timestamptz, "timestamp") RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_gt_timestamp' WITH (OID=2530, DESCRIPTION="greater-than");

 CREATE FUNCTION timestamptz_ge_timestamp(timestamptz, "timestamp") RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_ge_timestamp' WITH (OID=2531, DESCRIPTION="greater-than-or-equal");

 CREATE FUNCTION timestamptz_ne_timestamp(timestamptz, "timestamp") RETURNS bool LANGUAGE internal STABLE STRICT AS 'timestamptz_ne_timestamp' WITH (OID=2532, DESCRIPTION="not equal");

 CREATE FUNCTION timestamptz_cmp_timestamp(timestamptz, "timestamp") RETURNS int4 LANGUAGE internal STABLE STRICT AS 'timestamptz_cmp_timestamp' WITH (OID=2533, DESCRIPTION="less-equal-greater");

-- send/receive functions 
 CREATE FUNCTION array_recv(internal, oid, int4) RETURNS anyarray LANGUAGE internal STABLE STRICT AS 'array_recv' WITH (OID=2400, DESCRIPTION="I/O");

 CREATE FUNCTION array_send(anyarray) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'array_send' WITH (OID=2401, DESCRIPTION="I/O");

 CREATE FUNCTION record_recv(internal, oid, int4) RETURNS record LANGUAGE internal VOLATILE STRICT AS 'record_recv' WITH (OID=2402, DESCRIPTION="I/O");

 CREATE FUNCTION record_send(record) RETURNS bytea LANGUAGE internal VOLATILE STRICT AS 'record_send' WITH (OID=2403, DESCRIPTION="I/O");

 CREATE FUNCTION int2recv(internal) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'int2recv' WITH (OID=2404, DESCRIPTION="I/O");

 CREATE FUNCTION int2send(int2) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int2send' WITH (OID=2405, DESCRIPTION="I/O");

 CREATE FUNCTION int4recv(internal) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'int4recv' WITH (OID=2406, DESCRIPTION="I/O");

 CREATE FUNCTION int4send(int4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int4send' WITH (OID=2407, DESCRIPTION="I/O");

 CREATE FUNCTION int8recv(internal) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8recv' WITH (OID=2408, DESCRIPTION="I/O");

 CREATE FUNCTION int8send(int8) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int8send' WITH (OID=2409, DESCRIPTION="I/O");

 CREATE FUNCTION int2vectorrecv(internal) RETURNS int2vector LANGUAGE internal IMMUTABLE STRICT AS 'int2vectorrecv' WITH (OID=2410, DESCRIPTION="I/O");

 CREATE FUNCTION int2vectorsend(int2vector) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int2vectorsend' WITH (OID=2411, DESCRIPTION="I/O");

 CREATE FUNCTION bytearecv(internal) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'bytearecv' WITH (OID=2412, DESCRIPTION="I/O");

 CREATE FUNCTION byteasend(bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'byteasend' WITH (OID=2413, DESCRIPTION="I/O");

 CREATE FUNCTION textrecv(internal) RETURNS text LANGUAGE internal STABLE STRICT AS 'textrecv' WITH (OID=2414, DESCRIPTION="I/O");

 CREATE FUNCTION textsend(text) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'textsend' WITH (OID=2415, DESCRIPTION="I/O");

 CREATE FUNCTION unknownrecv(internal) RETURNS unknown LANGUAGE internal IMMUTABLE STRICT AS 'unknownrecv' WITH (OID=2416, DESCRIPTION="I/O");

 CREATE FUNCTION unknownsend(unknown) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'unknownsend' WITH (OID=2417, DESCRIPTION="I/O");

 CREATE FUNCTION oidrecv(internal) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'oidrecv' WITH (OID=2418, DESCRIPTION="I/O");

 CREATE FUNCTION oidsend(oid) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'oidsend' WITH (OID=2419, DESCRIPTION="I/O");

 CREATE FUNCTION oidvectorrecv(internal) RETURNS oidvector LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorrecv' WITH (OID=2420, DESCRIPTION="I/O");

 CREATE FUNCTION oidvectorsend(oidvector) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'oidvectorsend' WITH (OID=2421, DESCRIPTION="I/O");

 CREATE FUNCTION namerecv(internal) RETURNS name LANGUAGE internal STABLE STRICT AS 'namerecv' WITH (OID=2422, DESCRIPTION="I/O");

 CREATE FUNCTION namesend(name) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'namesend' WITH (OID=2423, DESCRIPTION="I/O");

 CREATE FUNCTION float4recv(internal) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'float4recv' WITH (OID=2424, DESCRIPTION="I/O");

 CREATE FUNCTION float4send(float4) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'float4send' WITH (OID=2425, DESCRIPTION="I/O");

 CREATE FUNCTION float8recv(internal) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8recv' WITH (OID=2426, DESCRIPTION="I/O");

 CREATE FUNCTION float8send(float8) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'float8send' WITH (OID=2427, DESCRIPTION="I/O");

 CREATE FUNCTION point_recv(internal) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'point_recv' WITH (OID=2428, DESCRIPTION="I/O");

 CREATE FUNCTION point_send(point) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'point_send' WITH (OID=2429, DESCRIPTION="I/O");

 CREATE FUNCTION bpcharrecv(internal, oid, int4) RETURNS bpchar LANGUAGE internal STABLE STRICT AS 'bpcharrecv' WITH (OID=2430, DESCRIPTION="I/O");

 CREATE FUNCTION bpcharsend(bpchar) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'bpcharsend' WITH (OID=2431, DESCRIPTION="I/O");

 CREATE FUNCTION varcharrecv(internal, oid, int4) RETURNS "varchar" LANGUAGE internal STABLE STRICT AS 'varcharrecv' WITH (OID=2432, DESCRIPTION="I/O");

 CREATE FUNCTION varcharsend("varchar") RETURNS bytea LANGUAGE internal STABLE STRICT AS 'varcharsend' WITH (OID=2433, DESCRIPTION="I/O");

 CREATE FUNCTION charrecv(internal) RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'charrecv' WITH (OID=2434, DESCRIPTION="I/O");

 CREATE FUNCTION charsend("char") RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'charsend' WITH (OID=2435, DESCRIPTION="I/O");

 CREATE FUNCTION boolrecv(internal) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'boolrecv' WITH (OID=2436, DESCRIPTION="I/O");

 CREATE FUNCTION boolsend(bool) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'boolsend' WITH (OID=2437, DESCRIPTION="I/O");

 CREATE FUNCTION tidrecv(internal) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'tidrecv' WITH (OID=2438, DESCRIPTION="I/O");

 CREATE FUNCTION tidsend(tid) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'tidsend' WITH (OID=2439, DESCRIPTION="I/O");

 CREATE FUNCTION xidrecv(internal) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'xidrecv' WITH (OID=2440, DESCRIPTION="I/O");

 CREATE FUNCTION xidsend(xid) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'xidsend' WITH (OID=2441, DESCRIPTION="I/O");

 CREATE FUNCTION cidrecv(internal) RETURNS cid LANGUAGE internal IMMUTABLE STRICT AS 'cidrecv' WITH (OID=2442, DESCRIPTION="I/O");

 CREATE FUNCTION cidsend(cid) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'cidsend' WITH (OID=2443, DESCRIPTION="I/O");

 CREATE FUNCTION regprocrecv(internal) RETURNS regproc LANGUAGE internal IMMUTABLE STRICT AS 'regprocrecv' WITH (OID=2444, DESCRIPTION="I/O");

 CREATE FUNCTION regprocsend(regproc) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'regprocsend' WITH (OID=2445, DESCRIPTION="I/O");

 CREATE FUNCTION regprocedurerecv(internal) RETURNS regprocedure LANGUAGE internal IMMUTABLE STRICT AS 'regprocedurerecv' WITH (OID=2446, DESCRIPTION="I/O");

 CREATE FUNCTION regproceduresend(regprocedure) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'regproceduresend' WITH (OID=2447, DESCRIPTION="I/O");

 CREATE FUNCTION regoperrecv(internal) RETURNS regoper LANGUAGE internal IMMUTABLE STRICT AS 'regoperrecv' WITH (OID=2448, DESCRIPTION="I/O");

 CREATE FUNCTION regopersend(regoper) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'regopersend' WITH (OID=2449, DESCRIPTION="I/O");

 CREATE FUNCTION regoperatorrecv(internal) RETURNS regoperator LANGUAGE internal IMMUTABLE STRICT AS 'regoperatorrecv' WITH (OID=2450, DESCRIPTION="I/O");

 CREATE FUNCTION regoperatorsend(regoperator) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'regoperatorsend' WITH (OID=2451, DESCRIPTION="I/O");

 CREATE FUNCTION regclassrecv(internal) RETURNS regclass LANGUAGE internal IMMUTABLE STRICT AS 'regclassrecv' WITH (OID=2452, DESCRIPTION="I/O");

 CREATE FUNCTION regclasssend(regclass) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'regclasssend' WITH (OID=2453, DESCRIPTION="I/O");

 CREATE FUNCTION regtyperecv(internal) RETURNS regtype LANGUAGE internal IMMUTABLE STRICT AS 'regtyperecv' WITH (OID=2454, DESCRIPTION="I/O");

 CREATE FUNCTION regtypesend(regtype) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'regtypesend' WITH (OID=2455, DESCRIPTION="I/O");

 CREATE FUNCTION bit_recv(internal, oid, int4) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'bit_recv' WITH (OID=2456, DESCRIPTION="I/O");

 CREATE FUNCTION bit_send("bit") RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'bit_send' WITH (OID=2457, DESCRIPTION="I/O");

 CREATE FUNCTION varbit_recv(internal, oid, int4) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'varbit_recv' WITH (OID=2458, DESCRIPTION="I/O");

 CREATE FUNCTION varbit_send(varbit) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'varbit_send' WITH (OID=2459, DESCRIPTION="I/O");

 CREATE FUNCTION numeric_recv(internal, oid, int4) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'numeric_recv' WITH (OID=2460, DESCRIPTION="I/O");

 CREATE FUNCTION numeric_send("numeric") RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'numeric_send' WITH (OID=2461, DESCRIPTION="I/O");

 CREATE FUNCTION abstimerecv(internal) RETURNS abstime LANGUAGE internal IMMUTABLE STRICT AS 'abstimerecv' WITH (OID=2462, DESCRIPTION="I/O");

 CREATE FUNCTION abstimesend(abstime) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'abstimesend' WITH (OID=2463, DESCRIPTION="I/O");

 CREATE FUNCTION reltimerecv(internal) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'reltimerecv' WITH (OID=2464, DESCRIPTION="I/O");

 CREATE FUNCTION reltimesend(reltime) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'reltimesend' WITH (OID=2465, DESCRIPTION="I/O");

 CREATE FUNCTION tintervalrecv(internal) RETURNS tinterval LANGUAGE internal IMMUTABLE STRICT AS 'tintervalrecv' WITH (OID=2466, DESCRIPTION="I/O");

 CREATE FUNCTION tintervalsend(tinterval) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'tintervalsend' WITH (OID=2467, DESCRIPTION="I/O");

 CREATE FUNCTION date_recv(internal) RETURNS date LANGUAGE internal IMMUTABLE STRICT AS 'date_recv' WITH (OID=2468, DESCRIPTION="I/O");

 CREATE FUNCTION date_send(date) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'date_send' WITH (OID=2469, DESCRIPTION="I/O");

 CREATE FUNCTION time_recv(internal, oid, int4) RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'time_recv' WITH (OID=2470, DESCRIPTION="I/O");

 CREATE FUNCTION time_send("time") RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'time_send' WITH (OID=2471, DESCRIPTION="I/O");

 CREATE FUNCTION timetz_recv(internal, oid, int4) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'timetz_recv' WITH (OID=2472, DESCRIPTION="I/O");

 CREATE FUNCTION timetz_send(timetz) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'timetz_send' WITH (OID=2473, DESCRIPTION="I/O");

 CREATE FUNCTION timestamp_recv(internal, oid, int4) RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_recv' WITH (OID=2474, DESCRIPTION="I/O");

 CREATE FUNCTION timestamp_send("timestamp") RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'timestamp_send' WITH (OID=2475, DESCRIPTION="I/O");

 CREATE FUNCTION timestamptz_recv(internal, oid, int4) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_recv' WITH (OID=2476, DESCRIPTION="I/O");

 CREATE FUNCTION timestamptz_send(timestamptz) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'timestamptz_send' WITH (OID=2477, DESCRIPTION="I/O");

 CREATE FUNCTION interval_recv(internal, oid, int4) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'interval_recv' WITH (OID=2478, DESCRIPTION="I/O");

 CREATE FUNCTION interval_send("interval") RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'interval_send' WITH (OID=2479, DESCRIPTION="I/O");

 CREATE FUNCTION lseg_recv(internal) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lseg_recv' WITH (OID=2480, DESCRIPTION="I/O");

 CREATE FUNCTION lseg_send(lseg) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'lseg_send' WITH (OID=2481, DESCRIPTION="I/O");

 CREATE FUNCTION path_recv(internal) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'path_recv' WITH (OID=2482, DESCRIPTION="I/O");

 CREATE FUNCTION path_send(path) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'path_send' WITH (OID=2483, DESCRIPTION="I/O");

 CREATE FUNCTION box_recv(internal) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'box_recv' WITH (OID=2484, DESCRIPTION="I/O");

 CREATE FUNCTION box_send(box) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'box_send' WITH (OID=2485, DESCRIPTION="I/O");

 CREATE FUNCTION poly_recv(internal) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'poly_recv' WITH (OID=2486, DESCRIPTION="I/O");

 CREATE FUNCTION poly_send(polygon) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'poly_send' WITH (OID=2487, DESCRIPTION="I/O");

 CREATE FUNCTION line_recv(internal) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'line_recv' WITH (OID=2488, DESCRIPTION="I/O");

 CREATE FUNCTION line_send(line) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'line_send' WITH (OID=2489, DESCRIPTION="I/O");

 CREATE FUNCTION circle_recv(internal) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'circle_recv' WITH (OID=2490, DESCRIPTION="I/O");

 CREATE FUNCTION circle_send(circle) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'circle_send' WITH (OID=2491, DESCRIPTION="I/O");

 CREATE FUNCTION cash_recv(internal) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'cash_recv' WITH (OID=2492, DESCRIPTION="I/O");

 CREATE FUNCTION cash_send(money) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'cash_send' WITH (OID=2493, DESCRIPTION="I/O");

 CREATE FUNCTION macaddr_recv(internal) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_recv' WITH (OID=2494, DESCRIPTION="I/O");

 CREATE FUNCTION macaddr_send(macaddr) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'macaddr_send' WITH (OID=2495, DESCRIPTION="I/O");

 CREATE FUNCTION inet_recv(internal) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'inet_recv' WITH (OID=2496, DESCRIPTION="I/O");

 CREATE FUNCTION inet_send(inet) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'inet_send' WITH (OID=2497, DESCRIPTION="I/O");

 CREATE FUNCTION cidr_recv(internal) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'cidr_recv' WITH (OID=2498, DESCRIPTION="I/O");

 CREATE FUNCTION cidr_send(cidr) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'cidr_send' WITH (OID=2499, DESCRIPTION="I/O");

 CREATE FUNCTION cstring_recv(internal) RETURNS cstring LANGUAGE internal STABLE STRICT AS 'cstring_recv' WITH (OID=2500, DESCRIPTION="I/O");

 CREATE FUNCTION cstring_send(cstring) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'cstring_send' WITH (OID=2501, DESCRIPTION="I/O");

 CREATE FUNCTION anyarray_recv(internal) RETURNS anyarray LANGUAGE internal STABLE STRICT AS 'anyarray_recv' WITH (OID=2502, DESCRIPTION="I/O");

 CREATE FUNCTION anyarray_send(anyarray) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'anyarray_send' WITH (OID=2503, DESCRIPTION="I/O");

-- System-view support functions with pretty-print option 
 CREATE FUNCTION pg_get_ruledef(oid, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_ruledef_ext' WITH (OID=2504, DESCRIPTION="source text of a rule with pretty-print option");

 CREATE FUNCTION pg_get_viewdef(text, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_viewdef_name_ext' WITH (OID=2505, DESCRIPTION="select statement of a view with pretty-print option");

 CREATE FUNCTION pg_get_viewdef(oid, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_viewdef_ext' WITH (OID=2506, DESCRIPTION="select statement of a view with pretty-print option");

 CREATE FUNCTION pg_get_indexdef(oid, int4, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_indexdef_ext' WITH (OID=2507, DESCRIPTION="index description (full create statement or single expression) with pretty-print option");

 CREATE FUNCTION pg_get_constraintdef(oid, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_constraintdef_ext' WITH (OID=2508, DESCRIPTION="constraint description with pretty-print option");

 CREATE FUNCTION pg_get_expr(text, oid, bool) RETURNS text LANGUAGE internal STABLE STRICT AS 'pg_get_expr_ext' WITH (OID=2509, DESCRIPTION="deparse an encoded expression with pretty-print option");

 CREATE FUNCTION pg_prepared_statement() RETURNS SETOF record LANGUAGE internal STABLE STRICT AS 'pg_prepared_statement' WITH (OID=2510, DESCRIPTION="get the prepared statements for this session");

 CREATE FUNCTION pg_cursor() RETURNS SETOF record LANGUAGE internal STABLE STRICT AS 'pg_cursor' WITH (OID=2511, DESCRIPTION="get the open cursors for this session");

 CREATE FUNCTION pg_timezone_abbrevs(OUT abbrev text, OUT utc_offset "interval", OUT is_dst bool) RETURNS SETOF pg_catalog.record LANGUAGE internal STABLE STRICT AS 'pg_timezone_abbrevs' WITH (OID=2599, DESCRIPTION="get the available time zone abbreviations");

 CREATE FUNCTION pg_timezone_names(OUT name text, OUT abbrev text, OUT utc_offset "interval", OUT is_dst bool) RETURNS SETOF pg_catalog.record LANGUAGE internal STABLE STRICT AS 'pg_timezone_names' WITH (OID=2856, DESCRIPTION="get the available time zone names");

-- non-persistent series generator
 CREATE FUNCTION generate_series(int4, int4, int4) RETURNS SETOF int4 LANGUAGE internal VOLATILE STRICT AS 'generate_series_step_int4' WITH (OID=1066, DESCRIPTION="non-persistent series generator");

 CREATE FUNCTION generate_series(int4, int4) RETURNS SETOF int4 LANGUAGE internal VOLATILE STRICT AS 'generate_series_int4' WITH (OID=1067, DESCRIPTION="non-persistent series generator");

 CREATE FUNCTION generate_series(int8, int8, int8) RETURNS SETOF int8 LANGUAGE internal VOLATILE STRICT AS 'generate_series_step_int8' WITH (OID=1068, DESCRIPTION="non-persistent series generator");

 CREATE FUNCTION generate_series(int8, int8) RETURNS SETOF int8 LANGUAGE internal VOLATILE STRICT AS 'generate_series_int8' WITH (OID=1069, DESCRIPTION="non-persistent series generator");

 CREATE FUNCTION generate_series("timestamp", "timestamp", "interval") RETURNS SETOF "timestamp" LANGUAGE internal VOLATILE STRICT AS 'generate_series_timestamp' WITH (OID=2916, DESCRIPTION="non-persistent series generator");

 CREATE FUNCTION generate_series("timestamptz", "timestamptz", "interval") RETURNS SETOF "timestamptz" LANGUAGE internal VOLATILE STRICT AS 'generate_series_timestamptz' WITH (OID=2917, DESCRIPTION="non-persistent series generator");

-- boolean aggregates
 CREATE FUNCTION booland_statefunc(bool, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'booland_statefunc' WITH (OID=2515, DESCRIPTION="boolean-and aggregate transition function");

 CREATE FUNCTION boolor_statefunc(bool, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'boolor_statefunc' WITH (OID=2516, DESCRIPTION="boolean-or aggregate transition function");

 CREATE FUNCTION bool_and(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2517, DESCRIPTION="boolean-and aggregate", proisagg="t");

-- ANY, SOME? These names conflict with subquery operators. See doc.
 CREATE FUNCTION bool_or(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2518, DESCRIPTION="boolean-or aggregate", proisagg="t");

 CREATE FUNCTION "every"(bool) RETURNS bool LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2519, DESCRIPTION="boolean-and aggregate", proisagg="t");

-- bitwise integer aggregates
 CREATE FUNCTION bit_and(int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2236, DESCRIPTION="bitwise-and smallint aggregate", proisagg="t");

 CREATE FUNCTION bit_or(int2) RETURNS int2 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2237, DESCRIPTION="bitwise-or smallint aggregate", proisagg="t");

 CREATE FUNCTION bit_and(int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2238, DESCRIPTION="bitwise-and integer aggregate", proisagg="t");

 CREATE FUNCTION bit_or(int4) RETURNS int4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2239, DESCRIPTION="bitwise-or integer aggregate", proisagg="t");

 CREATE FUNCTION bit_and(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2240, DESCRIPTION="bitwise-and bigint aggregate", proisagg="t");

 CREATE FUNCTION bit_or(int8) RETURNS int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2241, DESCRIPTION="bitwise-or bigint aggregate", proisagg="t");

 CREATE FUNCTION bit_and("bit") RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2242, DESCRIPTION="bitwise-and bit aggregate", proisagg="t");

 CREATE FUNCTION bit_or("bit") RETURNS "bit" LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2243, DESCRIPTION="bitwise-or bit aggregate", proisagg="t");

-- formerly-missing interval + datetime operators
 CREATE FUNCTION interval_pl_date("interval", date) RETURNS pg_catalog."timestamp" LANGUAGE sql IMMUTABLE STRICT AS $$select $2 + $1$$  WITH (OID=2546);

 CREATE FUNCTION interval_pl_timetz("interval", timetz) RETURNS pg_catalog.timetz LANGUAGE sql IMMUTABLE STRICT AS $$select $2 + $1$$  WITH (OID=2547);

 CREATE FUNCTION interval_pl_timestamp("interval", "timestamp") RETURNS pg_catalog."timestamp" LANGUAGE sql IMMUTABLE STRICT AS $$select $2 + $1$$  WITH (OID=2548);

 CREATE FUNCTION interval_pl_timestamptz("interval", timestamptz) RETURNS pg_catalog.timestamptz LANGUAGE sql STABLE STRICT AS $$select $2 + $1$$  WITH (OID=2549);

 CREATE FUNCTION integer_pl_date(int4, date) RETURNS pg_catalog.date LANGUAGE sql IMMUTABLE STRICT AS $$select $2 + $1$$  WITH (OID=2550);

 CREATE FUNCTION pg_tablespace_databases(oid) RETURNS SETOF oid LANGUAGE internal STABLE STRICT AS 'pg_tablespace_databases' WITH (OID=2556, DESCRIPTION="returns database oids in a tablespace");

 CREATE FUNCTION bool(int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'int4_bool' WITH (OID=2557, DESCRIPTION="convert int4 to boolean");

 CREATE FUNCTION int4(bool) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'bool_int4' WITH (OID=2558, DESCRIPTION="convert boolean to int4");

 CREATE FUNCTION lastval() RETURNS int8 LANGUAGE internal VOLATILE STRICT AS 'lastval' WITH (OID=2559, DESCRIPTION="current value from last used sequence");

-- start time function
 CREATE FUNCTION pg_postmaster_start_time() RETURNS timestamptz LANGUAGE internal STABLE STRICT AS 'pgsql_postmaster_start_time' WITH (OID=2560, DESCRIPTION="postmaster start time");

-- new functions for Y-direction rtree opclasses 
 CREATE FUNCTION box_below(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_below' WITH (OID=2562, DESCRIPTION="is below");

 CREATE FUNCTION box_overbelow(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overbelow' WITH (OID=2563, DESCRIPTION="overlaps or is below");

 CREATE FUNCTION box_overabove(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_overabove' WITH (OID=2564, DESCRIPTION="overlaps or is above");

 CREATE FUNCTION box_above(box, box) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'box_above' WITH (OID=2565, DESCRIPTION="is above");

 CREATE FUNCTION poly_below(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_below' WITH (OID=2566, DESCRIPTION="is below");

 CREATE FUNCTION poly_overbelow(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overbelow' WITH (OID=2567, DESCRIPTION="overlaps or is below");

 CREATE FUNCTION poly_overabove(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_overabove' WITH (OID=2568, DESCRIPTION="overlaps or is above");

 CREATE FUNCTION poly_above(polygon, polygon) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'poly_above' WITH (OID=2569, DESCRIPTION="is above");

 CREATE FUNCTION circle_overbelow(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overbelow' WITH (OID=2587, DESCRIPTION="overlaps or is below");

 CREATE FUNCTION circle_overabove(circle, circle) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'circle_overabove' WITH (OID=2588, DESCRIPTION="overlaps or is above");

-- support functions for GiST r-tree emulation
 CREATE FUNCTION gist_box_consistent(internal, box, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_consistent' WITH (OID=2578, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_box_compress(internal) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_compress' WITH (OID=2579, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_box_decompress(internal) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_decompress' WITH (OID=2580, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_box_penalty(internal, internal, internal) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_penalty' WITH (OID=2581, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_box_picksplit(internal, internal) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_picksplit' WITH (OID=2582, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_box_union(internal, internal) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_union' WITH (OID=2583, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_box_same(box, box, internal) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_box_same' WITH (OID=2584, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_poly_consistent(internal, polygon, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gist_poly_consistent' WITH (OID=2585, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_poly_compress(internal) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_poly_compress' WITH (OID=2586, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_circle_consistent(internal, circle, int4) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gist_circle_consistent' WITH (OID=2591, DESCRIPTION="GiST support");

 CREATE FUNCTION gist_circle_compress(internal) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'gist_circle_compress' WITH (OID=2592, DESCRIPTION="GiST support");

-- GIN
 CREATE FUNCTION gingettuple(internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'gingettuple' WITH (OID=2730, DESCRIPTION="gin(internal)");

 CREATE FUNCTION gingetmulti(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'gingetmulti' WITH (OID=2731, DESCRIPTION="gin(internal)");

 CREATE FUNCTION gininsert(internal, internal, internal, internal, internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'gininsert' WITH (OID=2732, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginbeginscan(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'ginbeginscan' WITH (OID=2733, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginrescan(internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'ginrescan' WITH (OID=2734, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginendscan(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'ginendscan' WITH (OID=2735, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginmarkpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'ginmarkpos' WITH (OID=2736, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginrestrpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'ginrestrpos' WITH (OID=2737, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginbuild(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'ginbuild' WITH (OID=2738, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginbulkdelete(internal, internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'ginbulkdelete' WITH (OID=2739, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginvacuumcleanup(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'ginvacuumcleanup' WITH (OID=2740, DESCRIPTION="gin(internal)");

 CREATE FUNCTION gincostestimate(internal, internal, internal, internal, internal, internal, internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'gincostestimate' WITH (OID=2741, DESCRIPTION="gin(internal)");

 CREATE FUNCTION ginoptions(_text, bool) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'ginoptions' WITH (OID=2788, DESCRIPTION="gin(internal)");

-- Greenplum Analytic functions
 CREATE FUNCTION matrix_transpose(anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'matrix_transpose' WITH (OID=3200, DESCRIPTION="transpose a two dimensional matrix");

 CREATE FUNCTION matrix_multiply(_int2, _int2) RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_multiply' WITH (OID=3201, DESCRIPTION="perform matrix multiplication on two matrices");

 CREATE FUNCTION matrix_multiply(_int4, _int4) RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_multiply' WITH (OID=3202, DESCRIPTION="perform matrix multiplication on two matrices");

 CREATE FUNCTION matrix_multiply(_int8, _int8) RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_multiply' WITH (OID=3203, DESCRIPTION="perform matrix multiplication on two matrices");

 CREATE FUNCTION matrix_multiply(_float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_multiply' WITH (OID=3204, DESCRIPTION="perform matrix multiplication on two matrices");

 CREATE FUNCTION matrix_multiply(_int8, int8) RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'int8_matrix_smultiply' WITH (OID=3205, DESCRIPTION="multiply a matrix by a scalar value");

 CREATE FUNCTION matrix_multiply(_float8, float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_matrix_smultiply' WITH (OID=3206, DESCRIPTION="multiply a matrix by a scalar value");

 CREATE FUNCTION matrix_add(_int2, _int2) RETURNS _int2 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_add' WITH (OID=3208, DESCRIPTION="perform matrix addition on two conformable matrices");

 CREATE FUNCTION matrix_add(_int4, _int4) RETURNS _int4 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_add' WITH (OID=3209, DESCRIPTION="perform matrix addition on two conformable matrices");

 CREATE FUNCTION matrix_add(_int8, _int8) RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_add' WITH (OID=3210, DESCRIPTION="perform matrix addition on two conformable matrices");

 CREATE FUNCTION matrix_add(_float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_add' WITH (OID=3211, DESCRIPTION="perform matrix addition on two conformable matrices");

 CREATE FUNCTION int2_matrix_accum(_int8, _int2) RETURNS _int8 LANGUAGE internal IMMUTABLE AS 'matrix_add' WITH (OID=3212, DESCRIPTION="perform matrix addition on two conformable matrices");

 CREATE FUNCTION int4_matrix_accum(_int8, _int4) RETURNS _int8 LANGUAGE internal IMMUTABLE AS 'matrix_add' WITH (OID=3213, DESCRIPTION="perform matrix addition on two conformable matrices");

 CREATE FUNCTION int8_matrix_accum(_int8, _int8) RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_add' WITH (OID=3214, DESCRIPTION="perform matrix addition on two conformable matrices");

 CREATE FUNCTION float8_matrix_accum(_float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'matrix_add' WITH (OID=3215, DESCRIPTION="perform matrix addition on two conformable matrices");

 CREATE FUNCTION sum(_int2) RETURNS _int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3216, proisagg="t");

 CREATE FUNCTION sum(_int4) RETURNS _int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3217, proisagg="t");

 CREATE FUNCTION sum(_int8) RETURNS _int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3218, proisagg="t");

 CREATE FUNCTION sum(_float8) RETURNS _float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3219, proisagg="t");

-- 3220 - reserved for sum(numeric[]) 
 CREATE FUNCTION int4_pivot_accum(_int4, _text, text, int4) RETURNS _int4 LANGUAGE internal IMMUTABLE AS 'int4_pivot_accum' WITH (OID=3225);

 CREATE FUNCTION pivot_sum(_text, text, int4) RETURNS _int4 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3226, proisagg="t");

 CREATE FUNCTION int8_pivot_accum(_int8, _text, text, int8) RETURNS _int8 LANGUAGE internal IMMUTABLE AS 'int8_pivot_accum' WITH (OID=3227);

 CREATE FUNCTION pivot_sum(_text, text, int8) RETURNS _int8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3228, proisagg="t");

 CREATE FUNCTION float8_pivot_accum(_float8, _text, text, float8) RETURNS _float8 LANGUAGE internal IMMUTABLE AS 'float8_pivot_accum' WITH (OID=3229);

 CREATE FUNCTION pivot_sum(_text, text, float8) RETURNS _float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3230, proisagg="t");

 CREATE FUNCTION unnest(anyarray) RETURNS SETOF anyelement LANGUAGE internal IMMUTABLE STRICT AS 'unnest' WITH (OID=3240);

-- 3241-324? reserved for unpivot, see pivot.c 

-- 3250 = nb_classification class 
-- 3251 = nb_classification type 

 CREATE FUNCTION nb_classify_accum(nb_classification, _text, int8, _int8, _int8) RETURNS nb_classification LANGUAGE internal IMMUTABLE AS 'nb_classify_accum' WITH (OID=3252);

 CREATE FUNCTION nb_classify_combine(nb_classification, nb_classification) RETURNS nb_classification LANGUAGE internal IMMUTABLE AS 'nb_classify_combine' WITH (OID=3253);

 CREATE FUNCTION nb_classify_final(nb_classification) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'nb_classify_final' WITH (OID=3254);

 CREATE FUNCTION nb_classify(_text, int8, _int8, _int8) RETURNS text LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3255, proisagg="t");

 CREATE FUNCTION nb_classify_probabilities(nb_classification) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'nb_classify_probabilities' WITH (OID=3267);

 CREATE FUNCTION nb_probabilities(_text, int8, _int8, _int8) RETURNS _float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3268, proisagg="t");

 CREATE FUNCTION pinv(_float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'pseudoinverse' WITH (OID=3256);

 CREATE FUNCTION float8_mregr_accum(_float8, float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_mregr_accum' WITH (OID=3257);

 CREATE FUNCTION float8_mregr_combine(_float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_mregr_combine' WITH (OID=3258);

 CREATE FUNCTION float8_mregr_coef(_float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_mregr_coef' WITH (OID=3259);

 CREATE FUNCTION float8_mregr_r2(_float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_mregr_r2' WITH (OID=3260);

 CREATE FUNCTION mregr_coef(float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'aggregate_dummy' WITH (OID=3261, proisagg="t");

 CREATE FUNCTION mregr_r2(float8, _float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'aggregate_dummy' WITH (OID=3262, proisagg="t");

 CREATE FUNCTION float8_mregr_tstats(_float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_mregr_tstats' WITH (OID=3263);

 CREATE FUNCTION mregr_tstats(float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3264, proisagg="t");

 CREATE FUNCTION float8_mregr_pvalues(_float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_mregr_pvalues' WITH (OID=3265);

 CREATE FUNCTION mregr_pvalues(float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=3266, proisagg="t");

 CREATE FUNCTION gpaotidin(cstring) RETURNS gpaotid LANGUAGE internal IMMUTABLE STRICT AS 'gpaotidin' WITH (OID=3302, DESCRIPTION="I/O");

 CREATE FUNCTION gpaotidout(gpaotid) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'gpaotidout' WITH (OID=3303, DESCRIPTION="I/O");

 CREATE FUNCTION gpaotidrecv(internal) RETURNS gpaotid LANGUAGE internal IMMUTABLE STRICT AS 'gpaotidrecv' WITH (OID=3304, DESCRIPTION="I/O");

 CREATE FUNCTION gpaotidsend(gpaotid) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'gpaotidsend' WITH (OID=3305, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocin(cstring) RETURNS gpxlogloc LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocin' WITH (OID=3312, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocout(gpxlogloc) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocout' WITH (OID=3313, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocrecv(internal) RETURNS gpxlogloc LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocrecv' WITH (OID=3314, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocsend(gpxlogloc) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocsend' WITH (OID=3315, DESCRIPTION="I/O");

 CREATE FUNCTION gpxlogloclarger(gpxlogloc, gpxlogloc) RETURNS gpxlogloc LANGUAGE internal IMMUTABLE STRICT AS 'gpxlogloclarger' WITH (OID=3318, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocsmaller(gpxlogloc, gpxlogloc) RETURNS gpxlogloc LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocsmaller' WITH (OID=3319, DESCRIPTION="I/O");

 CREATE FUNCTION gpxlogloceq(gpxlogloc, gpxlogloc) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxlogloceq' WITH (OID=3331, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocne(gpxlogloc, gpxlogloc) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocne' WITH (OID=3320, DESCRIPTION="I/O");

 CREATE FUNCTION gpxlogloclt(gpxlogloc, gpxlogloc) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxlogloclt' WITH (OID=3321, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocle(gpxlogloc, gpxlogloc) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocle' WITH (OID=3322, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocgt(gpxlogloc, gpxlogloc) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocgt' WITH (OID=3323, DESCRIPTION="I/O");

 CREATE FUNCTION gpxloglocge(gpxlogloc, gpxlogloc) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'gpxloglocge' WITH (OID=3324, DESCRIPTION="I/O");

-- Greenplum MPP exposed internally-defined functions. 
 CREATE FUNCTION gp_backup_launch(text, text, text, text, text) RETURNS text LANGUAGE internal VOLATILE AS 'gp_backup_launch__' WITH (OID=6003, DESCRIPTION="launch mpp backup on outboard Postgres instances");

 CREATE FUNCTION gp_restore_launch(text, text, text, text, text, text, int4, bool) RETURNS text LANGUAGE internal VOLATILE AS 'gp_restore_launch__' WITH (OID=6004, DESCRIPTION="launch mpp restore on outboard Postgres instances");

 CREATE FUNCTION gp_read_backup_file(text, text, regproc) RETURNS text LANGUAGE internal VOLATILE AS 'gp_read_backup_file__' WITH (OID=6005, DESCRIPTION="read mpp backup file on outboard Postgres instances");

 CREATE FUNCTION gp_write_backup_file(text, text, text) RETURNS text LANGUAGE internal VOLATILE AS 'gp_write_backup_file__' WITH (OID=6006, DESCRIPTION="write mpp backup file on outboard Postgres instances");

 CREATE FUNCTION gp_pgdatabase() RETURNS SETOF record LANGUAGE internal VOLATILE AS 'gp_pgdatabase__' WITH (OID=6007, DESCRIPTION="view mpp pgdatabase state");

 CREATE FUNCTION numeric_amalg(_numeric, _numeric) RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'numeric_amalg' WITH (OID=6008, DESCRIPTION="aggregate preliminary function");

 CREATE FUNCTION numeric_avg_amalg(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'numeric_avg_amalg' WITH (OID=3104, DESCRIPTION="aggregate preliminary function");

 CREATE FUNCTION int8_avg_amalg(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int8_avg_amalg' WITH (OID=6009, DESCRIPTION="aggregate preliminary function");

 CREATE FUNCTION float8_amalg(_float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_amalg' WITH (OID=6010, DESCRIPTION="aggregate preliminary function");

 CREATE FUNCTION float8_avg_amalg(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'float8_avg_amalg' WITH (OID=3111, DESCRIPTION="aggregate preliminary function");

 CREATE FUNCTION interval_amalg(_interval, _interval) RETURNS _interval LANGUAGE internal IMMUTABLE STRICT AS 'interval_amalg' WITH (OID=6011, DESCRIPTION="aggregate preliminary function");

 CREATE FUNCTION numeric_demalg(_numeric, _numeric) RETURNS _numeric LANGUAGE internal IMMUTABLE STRICT AS 'numeric_demalg' WITH (OID=6015, DESCRIPTION="aggregate inverse preliminary function");

 CREATE FUNCTION numeric_avg_demalg(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'numeric_avg_demalg' WITH (OID=3105, DESCRIPTION="aggregate inverse preliminary function");

 CREATE FUNCTION int8_avg_demalg(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'int8_avg_demalg' WITH (OID=6016, DESCRIPTION="aggregate preliminary function");

 CREATE FUNCTION float8_demalg(_float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_demalg' WITH (OID=6017);

 CREATE FUNCTION float8_avg_demalg(bytea, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'float8_avg_demalg' WITH (OID=3110, DESCRIPTION="aggregate inverse preliminary function");

 CREATE FUNCTION interval_demalg(_interval, _interval) RETURNS _interval LANGUAGE internal IMMUTABLE STRICT AS 'interval_demalg' WITH (OID=6018, DESCRIPTION="aggregate preliminary function");

 CREATE FUNCTION float8_regr_amalg(_float8, _float8) RETURNS _float8 LANGUAGE internal IMMUTABLE STRICT AS 'float8_regr_amalg' WITH (OID=6014);

 CREATE FUNCTION int8(tid) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'tidtoi8' WITH (OID=6021, DESCRIPTION="convert tid to int8");
-- #define CDB_PROC_TIDTOI8    6021

 CREATE FUNCTION gp_execution_segment() RETURNS SETOF int4 LANGUAGE internal VOLATILE AS 'mpp_execution_segment' WITH (OID=6022, DESCRIPTION="segment executing function");
-- #define MPP_EXECUTION_SEGMENT_OID 6022
-- #define MPP_EXECUTION_SEGMENT_TYPE 23

 CREATE FUNCTION pg_highest_oid() RETURNS oid LANGUAGE internal VOLATILE STRICT READS SQL DATA AS 'pg_highest_oid' WITH (OID=6023, DESCRIPTION="Highest oid used so far");

 CREATE FUNCTION gp_distributed_xacts() RETURNS SETOF record LANGUAGE internal VOLATILE AS 'gp_distributed_xacts__' WITH (OID=6035, DESCRIPTION="view mpp distributed transaction state");

 CREATE FUNCTION gp_max_distributed_xid() RETURNS xid LANGUAGE internal VOLATILE STRICT AS 'gp_max_distributed_xid' WITH (OID=6036, DESCRIPTION="Highest distributed transaction id used so far");

 CREATE FUNCTION gp_distributed_xid() RETURNS xid LANGUAGE internal VOLATILE STRICT AS 'gp_distributed_xid' WITH (OID=6037, DESCRIPTION="Current distributed transaction id");

 CREATE FUNCTION gp_transaction_log() RETURNS SETOF record LANGUAGE internal VOLATILE AS 'gp_transaction_log' WITH (OID=6043, DESCRIPTION="view logged local transaction status");

 CREATE FUNCTION gp_distributed_log() RETURNS SETOF record LANGUAGE internal VOLATILE AS 'gp_distributed_log' WITH (OID=6044, DESCRIPTION="view logged distributed transaction status");

 CREATE FUNCTION gp_changetracking_log(IN filetype int4, OUT segment_id int2, OUT dbid int2, OUT space oid, OUT db oid, OUT rel oid, OUT xlogloc gpxlogloc, OUT blocknum int4, OUT persistent_tid tid, OUT persistent_sn int8) RETURNS SETOF pg_catalog.record LANGUAGE internal VOLATILE AS 'gp_changetracking_log' WITH (OID=6435, DESCRIPTION="view logged change tracking records");

 CREATE FUNCTION gp_execution_dbid() RETURNS int4 LANGUAGE internal VOLATILE AS 'gp_execution_dbid' WITH (OID=6068, DESCRIPTION="dbid executing function");

-- Greenplum MPP window function implementation
 CREATE FUNCTION row_number_immed(internal) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'row_number_immed' WITH (OID=7100, DESCRIPTION="window immediate function");

 CREATE FUNCTION rank_immed(internal) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'rank_immed' WITH (OID=7101, DESCRIPTION="window immediate function");

 CREATE FUNCTION dense_rank_immed(internal) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'dense_rank_immed' WITH (OID=7102, DESCRIPTION="window immediate function");

 CREATE FUNCTION lag_bool(internal, bool, int8, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7490, proiswin="t");

 CREATE FUNCTION lag_bool(internal, bool, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7492, proiswin="t");

 CREATE FUNCTION lag_bool(internal, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7494, proiswin="t");

 CREATE FUNCTION lag_char(internal, "char", int8, "char") RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7496, proiswin="t");

 CREATE FUNCTION lag_char(internal, "char", int8) RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7498, proiswin="t");

 CREATE FUNCTION lag_char(internal, "char") RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7500, proiswin="t");

 CREATE FUNCTION lag_cidr(internal, cidr, int8, cidr) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7502, proiswin="t");

 CREATE FUNCTION lag_cidr(internal, cidr, int8) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7504, proiswin="t");

 CREATE FUNCTION lag_cidr(internal, cidr) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7506, proiswin="t");

 CREATE FUNCTION lag_circle(internal, circle, int8, circle) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7508, proiswin="t");

 CREATE FUNCTION lag_circle(internal, circle, int8) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7510, proiswin="t");

 CREATE FUNCTION lag_circle(internal, circle) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7512, proiswin="t");

 CREATE FUNCTION lag_float4(internal, float4, int8, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7514, proiswin="t");

 CREATE FUNCTION lag_float4(internal, float4, int8) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7516, proiswin="t");

 CREATE FUNCTION lag_float4(internal, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7518, proiswin="t");

 CREATE FUNCTION lag_float8(internal, float8, int8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7520, proiswin="t");

 CREATE FUNCTION lag_float8(internal, float8, int8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7522, proiswin="t");

 CREATE FUNCTION lag_float8(internal, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7524, proiswin="t");

 CREATE FUNCTION lag_inet(internal, inet, int8, inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7526, proiswin="t");

 CREATE FUNCTION lag_inet(internal, inet, int8) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7528, proiswin="t");

 CREATE FUNCTION lag_inet(internal, inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7530, proiswin="t");

 CREATE FUNCTION lag_interval(internal, "interval", int8, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7532, proiswin="t");

 CREATE FUNCTION lag_interval(internal, "interval", int8) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7534, proiswin="t");

 CREATE FUNCTION lag_interval(internal, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7536, proiswin="t");

 CREATE FUNCTION lag_line(internal, line, int8, line) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7538, proiswin="t");

 CREATE FUNCTION lag_line(internal, line, int8) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7540, proiswin="t");

 CREATE FUNCTION lag_line(internal, line) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7542, proiswin="t");

 CREATE FUNCTION lag_lseg(internal, lseg, int8, lseg) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7544, proiswin="t");

 CREATE FUNCTION lag_lseg(internal, lseg, int8) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7546, proiswin="t");

 CREATE FUNCTION lag_lseg(internal, lseg) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7548, proiswin="t");

 CREATE FUNCTION lag_macaddr(internal, macaddr, int8, macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7550, proiswin="t");

 CREATE FUNCTION lag_macaddr(internal, macaddr, int8) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7552, proiswin="t");

 CREATE FUNCTION lag_macaddr(internal, macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7554, proiswin="t");

 CREATE FUNCTION lag_smallint(internal, int2, int8, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7556, proiswin="t");

 CREATE FUNCTION lag_smallint(internal, int2, int8) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7558, proiswin="t");

 CREATE FUNCTION lag_smallint(internal, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7560, proiswin="t");

 CREATE FUNCTION lag_int4(internal, int4, int8, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7562, proiswin="t");

 CREATE FUNCTION lag_int4(internal, int4, int8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7564, proiswin="t");

 CREATE FUNCTION lag_int4(internal, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7566, proiswin="t");

 CREATE FUNCTION lag_int8(internal, int8, int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7568, proiswin="t");

 CREATE FUNCTION lag_int8(internal, int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7570, proiswin="t");

 CREATE FUNCTION lag_int8(internal, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7572, proiswin="t");

 CREATE FUNCTION lag_money(internal, money, int8, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7574, proiswin="t");

 CREATE FUNCTION lag_money(internal, money, int8) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7576, proiswin="t");

 CREATE FUNCTION lag_money(internal, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7578, proiswin="t");

 CREATE FUNCTION lag_name(internal, name, int8, name) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7580, proiswin="t");

 CREATE FUNCTION lag_name(internal, name, int8) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7582, proiswin="t");

 CREATE FUNCTION lag_name(internal, name) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7584, proiswin="t");

 CREATE FUNCTION lag_numeric(internal, "numeric", int8, "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7586, proiswin="t");

 CREATE FUNCTION lag_numeric(internal, "numeric", int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7588, proiswin="t");

 CREATE FUNCTION lag_numeric(internal, "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7590, proiswin="t");

 CREATE FUNCTION lag_oid(internal, oid, int8, oid) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7592, proiswin="t");

 CREATE FUNCTION lag_oid(internal, oid, int8) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7594, proiswin="t");

 CREATE FUNCTION lag_oid(internal, oid) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7596, proiswin="t");

 CREATE FUNCTION lag_path(internal, path, int8, path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7598, proiswin="t");

 CREATE FUNCTION lag_path(internal, path, int8) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7600, proiswin="t");

 CREATE FUNCTION lag_path(internal, path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7602, proiswin="t");

 CREATE FUNCTION lag_point(internal, point, int8, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7604, proiswin="t");

 CREATE FUNCTION lag_point(internal, point, int8) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7606, proiswin="t");

 CREATE FUNCTION lag_point(internal, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7608, proiswin="t");

 CREATE FUNCTION lag_polygon(internal, polygon, int8, polygon) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7610, proiswin="t");

 CREATE FUNCTION lag_polygon(internal, polygon, int8) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7612, proiswin="t");

 CREATE FUNCTION lag_polygon(internal, polygon) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7614, proiswin="t");

 CREATE FUNCTION lag_reltime(internal, reltime, int8, reltime) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7616, proiswin="t");

 CREATE FUNCTION lag_reltime(internal, reltime, int8) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7618, proiswin="t");

 CREATE FUNCTION lag_reltime(internal, reltime) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7620, proiswin="t");

 CREATE FUNCTION lag_text(internal, text, int8, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7622, proiswin="t");

 CREATE FUNCTION lag_text(internal, text, int8) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7624, proiswin="t");

 CREATE FUNCTION lag_text(internal, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7626, proiswin="t");

 CREATE FUNCTION lag_tid(internal, tid, int8, tid) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7628, proiswin="t");

 CREATE FUNCTION lag_tid(internal, tid, int8) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7630, proiswin="t");

 CREATE FUNCTION lag_tid(internal, tid) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7632, proiswin="t");

 CREATE FUNCTION lag_time(internal, "time", int8, "time") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7634, proiswin="t");

 CREATE FUNCTION lag_time(internal, "time", int8) RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7636, proiswin="t");

 CREATE FUNCTION lag_time(internal, "time") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7638, proiswin="t");

 CREATE FUNCTION lag_timestamp(internal, "timestamp", int8, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7640, proiswin="t");

 CREATE FUNCTION lag_timestamp(internal, "timestamp", int8) RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7642, proiswin="t");

 CREATE FUNCTION lag_timestamp(internal, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7644, proiswin="t");

 CREATE FUNCTION lag_timestamptz(internal, timestamptz, int8, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7646, proiswin="t");

 CREATE FUNCTION lag_timestamptz(internal, timestamptz, int8) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7648, proiswin="t");

 CREATE FUNCTION lag_timestamptz(internal, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7650, proiswin="t");

 CREATE FUNCTION lag_timetz(internal, timetz, int8, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7652, proiswin="t");

 CREATE FUNCTION lag_timetz(internal, timetz, int8) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7654, proiswin="t");

 CREATE FUNCTION lag_timetz(internal, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7656, proiswin="t");

 CREATE FUNCTION lag_varbit(internal, varbit, int8, varbit) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7658, proiswin="t");

 CREATE FUNCTION lag_varbit(internal, varbit, int8) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7660, proiswin="t");

 CREATE FUNCTION lag_varbit(internal, varbit) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7662, proiswin="t");

 CREATE FUNCTION lag_varchar(internal, "varchar", int8, "varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7664, proiswin="t");

 CREATE FUNCTION lag_varchar(internal, "varchar", int8) RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7666, proiswin="t");

 CREATE FUNCTION lag_varchar(internal, "varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7668, proiswin="t");

 CREATE FUNCTION lag_xid(internal, xid, int8, xid) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7670, proiswin="t");

 CREATE FUNCTION lag_xid(internal, xid, int8) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7672, proiswin="t");

 CREATE FUNCTION lag_xid(internal, xid) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7674, proiswin="t");

 CREATE FUNCTION lag_any(internal, anyarray, int8, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7208, proiswin="t");

 CREATE FUNCTION lag_any(internal, anyarray, int8) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7209, proiswin="t");

 CREATE FUNCTION lag_any(internal, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7210, proiswin="t");

 CREATE FUNCTION lag_bytea(internal, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7227, proiswin="t");

 CREATE FUNCTION lag_bytea(internal, bytea, int8) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7229, proiswin="t");

 CREATE FUNCTION lag_bytea(internal, bytea, int8, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7231, proiswin="t");

 CREATE FUNCTION lag_bit(internal, "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7251, proiswin="t");

 CREATE FUNCTION lag_bit(internal, "bit", int8) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7253, proiswin="t");

 CREATE FUNCTION lag_bit(internal, "bit", int8, "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7255, proiswin="t");

 CREATE FUNCTION lag_box(internal, box) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7267, proiswin="t");

 CREATE FUNCTION lag_box(internal, box, int8) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7269, proiswin="t");

 CREATE FUNCTION lag_box(internal, box, int8, box) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'lag_generic' WITH (OID=7271, proiswin="t");

 CREATE FUNCTION lead_int(internal, int4, int8, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7106, proiswin="t");

 CREATE FUNCTION lead_int(internal, int4, int8) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7104, proiswin="t");

 CREATE FUNCTION lead_int(internal, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7105, proiswin="t");

 CREATE FUNCTION lead_bool(internal, bool, int8, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7311, proiswin="t");

 CREATE FUNCTION lead_bool(internal, bool, int8) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7313, proiswin="t");

 CREATE FUNCTION lead_bool(internal, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7315, proiswin="t");

 CREATE FUNCTION lead_char(internal, "char", int8, "char") RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7317, proiswin="t");

 CREATE FUNCTION lead_char(internal, "char", int8) RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7319, proiswin="t");

 CREATE FUNCTION lead_char(internal, "char") RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7321, proiswin="t");

 CREATE FUNCTION lead_cidr(internal, cidr, int8, cidr) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7323, proiswin="t");

 CREATE FUNCTION lead_cidr(internal, cidr, int8) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7325, proiswin="t");

 CREATE FUNCTION lead_cidr(internal, cidr) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7327, proiswin="t");

 CREATE FUNCTION lead_circle(internal, circle, int8, circle) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7329, proiswin="t");

 CREATE FUNCTION lead_circle(internal, circle, int8) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7331, proiswin="t");

 CREATE FUNCTION lead_circle(internal, circle) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7333, proiswin="t");

 CREATE FUNCTION lead_float4(internal, float4, int8, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7335, proiswin="t");

 CREATE FUNCTION lead_float4(internal, float4, int8) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7337, proiswin="t");

 CREATE FUNCTION lead_float4(internal, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7339, proiswin="t");

 CREATE FUNCTION lead_float8(internal, float8, int8, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7341, proiswin="t");

 CREATE FUNCTION lead_float8(internal, float8, int8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7343, proiswin="t");

 CREATE FUNCTION lead_float8(internal, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7345, proiswin="t");

 CREATE FUNCTION lead_inet(internal, inet, int8, inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7347, proiswin="t");

 CREATE FUNCTION lead_inet(internal, inet, int8) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7349, proiswin="t");

 CREATE FUNCTION lead_inet(internal, inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7351, proiswin="t");

 CREATE FUNCTION lead_interval(internal, "interval", int8, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7353, proiswin="t");

 CREATE FUNCTION lead_interval(internal, "interval", int8) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7355, proiswin="t");

 CREATE FUNCTION lead_interval(internal, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7357, proiswin="t");

 CREATE FUNCTION lead_line(internal, line, int8, line) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7359, proiswin="t");

 CREATE FUNCTION lead_line(internal, line, int8) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7361, proiswin="t");

 CREATE FUNCTION lead_line(internal, line) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7363, proiswin="t");

 CREATE FUNCTION lead_lseg(internal, lseg, int8, lseg) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7365, proiswin="t");

 CREATE FUNCTION lead_lseg(internal, lseg, int8) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7367, proiswin="t");

 CREATE FUNCTION lead_lseg(internal, lseg) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7369, proiswin="t");

 CREATE FUNCTION lead_macaddr(internal, macaddr, int8, macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7371, proiswin="t");

 CREATE FUNCTION lead_macaddr(internal, macaddr, int8) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7373, proiswin="t");

 CREATE FUNCTION lead_macaddr(internal, macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7375, proiswin="t");

 CREATE FUNCTION lead_smallint(internal, int2, int8, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7377, proiswin="t");

 CREATE FUNCTION lead_smallint(internal, int2, int8) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7379, proiswin="t");

 CREATE FUNCTION lead_smallint(internal, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7381, proiswin="t");

 CREATE FUNCTION lead_int8(internal, int8, int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7383, proiswin="t");

 CREATE FUNCTION lead_int8(internal, int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7385, proiswin="t");

 CREATE FUNCTION lead_int8(internal, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7387, proiswin="t");

 CREATE FUNCTION lead_money(internal, money, int8, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7389, proiswin="t");

 CREATE FUNCTION lead_money(internal, money, int8) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7391, proiswin="t");

 CREATE FUNCTION lead_money(internal, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7393, proiswin="t");

 CREATE FUNCTION lead_name(internal, name, int8, name) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7395, proiswin="t");

 CREATE FUNCTION lead_name(internal, name, int8) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7397, proiswin="t");

 CREATE FUNCTION lead_name(internal, name) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7399, proiswin="t");

 CREATE FUNCTION lead_numeric(internal, "numeric", int8, "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7401, proiswin="t");

 CREATE FUNCTION lead_numeric(internal, "numeric", int8) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7403, proiswin="t");

 CREATE FUNCTION lead_numeric(internal, "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7405, proiswin="t");

 CREATE FUNCTION lead_oid(internal, oid, int8, oid) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7407, proiswin="t");

 CREATE FUNCTION lead_oid(internal, oid, int8) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7409, proiswin="t");

 CREATE FUNCTION lead_oid(internal, oid) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7411, proiswin="t");

 CREATE FUNCTION lead_path(internal, path, int8, path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7413, proiswin="t");

 CREATE FUNCTION lead_path(internal, path, int8) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7415, proiswin="t");

 CREATE FUNCTION lead_path(internal, path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7417, proiswin="t");

 CREATE FUNCTION lead_point(internal, point, int8, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7419, proiswin="t");

 CREATE FUNCTION lead_point(internal, point, int8) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7421, proiswin="t");

 CREATE FUNCTION lead_point(internal, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7423, proiswin="t");

 CREATE FUNCTION lead_polygon(internal, polygon, int8, polygon) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7425, proiswin="t");

 CREATE FUNCTION lead_polygon(internal, polygon, int8) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7427, proiswin="t");

 CREATE FUNCTION lead_polygon(internal, polygon) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7429, proiswin="t");

 CREATE FUNCTION lead_reltime(internal, reltime, int8, reltime) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7431, proiswin="t");

 CREATE FUNCTION lead_reltime(internal, reltime, int8) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7433, proiswin="t");

 CREATE FUNCTION lead_reltime(internal, reltime) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7435, proiswin="t");

 CREATE FUNCTION lead_text(internal, text, int8, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7437, proiswin="t");

 CREATE FUNCTION lead_text(internal, text, int8) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7439, proiswin="t");

 CREATE FUNCTION lead_text(internal, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7441, proiswin="t");

 CREATE FUNCTION lead_tid(internal, tid, int8, tid) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7443, proiswin="t");

 CREATE FUNCTION lead_tid(internal, tid, int8) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7445, proiswin="t");

 CREATE FUNCTION lead_tid(internal, tid) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7447, proiswin="t");

 CREATE FUNCTION lead_time(internal, "time", int8, "time") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7449, proiswin="t");

 CREATE FUNCTION lead_time(internal, "time", int8) RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7451, proiswin="t");

 CREATE FUNCTION lead_time(internal, "time") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7453, proiswin="t");

 CREATE FUNCTION lead_timestamp(internal, "timestamp", int8, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7455, proiswin="t");

 CREATE FUNCTION lead_timestamp(internal, "timestamp", int8) RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7457, proiswin="t");

 CREATE FUNCTION lead_timestamp(internal, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7459, proiswin="t");

 CREATE FUNCTION lead_timestamptz(internal, timestamptz, int8, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7461, proiswin="t");

 CREATE FUNCTION lead_timestamptz(internal, timestamptz, int8) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7463, proiswin="t");

 CREATE FUNCTION lead_timestamptz(internal, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7465, proiswin="t");

 CREATE FUNCTION lead_timetz(internal, timetz, int8, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7467, proiswin="t");

 CREATE FUNCTION lead_timetz(internal, timetz, int8) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7469, proiswin="t");

 CREATE FUNCTION lead_timetz(internal, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7471, proiswin="t");

 CREATE FUNCTION lead_varbit(internal, varbit, int8, varbit) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7473, proiswin="t");

 CREATE FUNCTION lead_varbit(internal, varbit, int8) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7475, proiswin="t");

 CREATE FUNCTION lead_varbit(internal, varbit) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7477, proiswin="t");

 CREATE FUNCTION lead_varchar(internal, "varchar", int8, "varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7479, proiswin="t");

 CREATE FUNCTION lead_varchar(internal, "varchar", int8) RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7481, proiswin="t");

 CREATE FUNCTION lead_varchar(internal, "varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7483, proiswin="t");

 CREATE FUNCTION lead_xid(internal, xid, int8, xid) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7485, proiswin="t");

 CREATE FUNCTION lead_xid(internal, xid, int8) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7487, proiswin="t");

 CREATE FUNCTION lead_xid(internal, xid) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7489, proiswin="t");

 CREATE FUNCTION lead_any(internal, anyarray, int8, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7217, proiswin="t");

 CREATE FUNCTION lead_any(internal, anyarray, int8) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7218, proiswin="t");

 CREATE FUNCTION lead_any(internal, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7219, proiswin="t");

 CREATE FUNCTION lead_bytea(internal, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7221, proiswin="t");

 CREATE FUNCTION lead_bytea(internal, bytea, int8) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7223, proiswin="t");

 CREATE FUNCTION lead_bytea(internal, bytea, int8, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7225, proiswin="t");

 CREATE FUNCTION lead_bit(internal, "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7245, proiswin="t");

 CREATE FUNCTION lead_bit(internal, "bit", int8) RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7247, proiswin="t");

 CREATE FUNCTION lead_bit(internal, "bit", int8, "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7249, proiswin="t");

 CREATE FUNCTION lead_box(internal, box) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7261, proiswin="t");

 CREATE FUNCTION lead_box(internal, box, int8) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7263, proiswin="t");

 CREATE FUNCTION lead_box(internal, box, int8, box) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'lead_generic' WITH (OID=7265, proiswin="t");

 CREATE FUNCTION first_value_bool(internal, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7111, proiswin="t");

 CREATE FUNCTION first_value_char(internal, "char") RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7112, proiswin="t");

 CREATE FUNCTION first_value_cidr(internal, cidr) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7113, proiswin="t");

 CREATE FUNCTION first_value_circle(internal, circle) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7114, proiswin="t");

 CREATE FUNCTION first_value_float4(internal, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7115, proiswin="t");

 CREATE FUNCTION first_value_float8(internal, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7116, proiswin="t");

 CREATE FUNCTION first_value_inet(internal, inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7117, proiswin="t");

 CREATE FUNCTION first_value_interval(internal, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7118, proiswin="t");

 CREATE FUNCTION first_value_line(internal, line) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7119, proiswin="t");

 CREATE FUNCTION first_value_lseg(internal, lseg) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7120, proiswin="t");

 CREATE FUNCTION first_value_macaddr(internal, macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7121, proiswin="t");

 CREATE FUNCTION first_value_smallint(internal, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7122, proiswin="t");

 CREATE FUNCTION first_value_int4(internal, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7123, proiswin="t");

 CREATE FUNCTION first_value_int8(internal, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7124, proiswin="t");

 CREATE FUNCTION first_value_money(internal, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7125, proiswin="t");

 CREATE FUNCTION first_value_name(internal, name) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7126, proiswin="t");

 CREATE FUNCTION first_value_numeric(internal, "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7127, proiswin="t");

 CREATE FUNCTION first_value_oid(internal, oid) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7128, proiswin="t");

 CREATE FUNCTION first_value_path(internal, path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7129, proiswin="t");

 CREATE FUNCTION first_value_point(internal, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7130, proiswin="t");

 CREATE FUNCTION first_value_polygon(internal, polygon) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7131, proiswin="t");

 CREATE FUNCTION first_value_reltime(internal, reltime) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7132, proiswin="t");

 CREATE FUNCTION first_value_text(internal, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7133, proiswin="t");

 CREATE FUNCTION first_value_tid(internal, tid) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7134, proiswin="t");

 CREATE FUNCTION first_value_time(internal, "time") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7135, proiswin="t");

 CREATE FUNCTION first_value_timestamp(internal, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7136, proiswin="t");

 CREATE FUNCTION first_value_timestamptz(internal, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7137, proiswin="t");

 CREATE FUNCTION first_value_timetz(internal, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7138, proiswin="t");

 CREATE FUNCTION first_value_varbit(internal, varbit) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7139, proiswin="t");

 CREATE FUNCTION first_value_varchar(internal, "varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7140, proiswin="t");

 CREATE FUNCTION first_value_xid(internal, xid) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7141, proiswin="t");

 CREATE FUNCTION first_value_bytea(internal, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7233, proiswin="t");

 CREATE FUNCTION first_value_bit(internal, "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7257, proiswin="t");

 CREATE FUNCTION first_value_box(internal, box) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7273, proiswin="t");

 CREATE FUNCTION first_value_any(internal, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'first_value_generic' WITH (OID=7289, proiswin="t");

 CREATE FUNCTION last_value_int(internal, int4) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7103, proiswin="t");

 CREATE FUNCTION last_value_smallint(internal, int2) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7107, proiswin="t");

 CREATE FUNCTION last_value_bigint(internal, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7108, proiswin="t");

 CREATE FUNCTION last_value_numeric(internal, "numeric") RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7109, proiswin="t");

 CREATE FUNCTION last_value_text(internal, text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7110, proiswin="t");

 CREATE FUNCTION last_value_bool(internal, bool) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7165, proiswin="t");

 CREATE FUNCTION last_value_char(internal, "char") RETURNS "char" LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7166, proiswin="t");

 CREATE FUNCTION last_value_cidr(internal, cidr) RETURNS cidr LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7167, proiswin="t");

 CREATE FUNCTION last_value_circle(internal, circle) RETURNS circle LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7168, proiswin="t");

 CREATE FUNCTION last_value_float4(internal, float4) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7142, proiswin="t");

 CREATE FUNCTION last_value_float8(internal, float8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7143, proiswin="t");

 CREATE FUNCTION last_value_inet(internal, inet) RETURNS inet LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7144, proiswin="t");

 CREATE FUNCTION last_value_interval(internal, "interval") RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7145, proiswin="t");

 CREATE FUNCTION last_value_line(internal, line) RETURNS line LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7146, proiswin="t");

 CREATE FUNCTION last_value_lseg(internal, lseg) RETURNS lseg LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7147, proiswin="t");

 CREATE FUNCTION last_value_macaddr(internal, macaddr) RETURNS macaddr LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7148, proiswin="t");

 CREATE FUNCTION last_value_money(internal, money) RETURNS money LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7149, proiswin="t");

 CREATE FUNCTION last_value_name(internal, name) RETURNS name LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7150, proiswin="t");

 CREATE FUNCTION last_value_oid(internal, oid) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7151, proiswin="t");

 CREATE FUNCTION last_value_path(internal, path) RETURNS path LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7152, proiswin="t");

 CREATE FUNCTION last_value_point(internal, point) RETURNS point LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7153, proiswin="t");

 CREATE FUNCTION last_value_polygon(internal, polygon) RETURNS polygon LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7154, proiswin="t");

 CREATE FUNCTION last_value_reltime(internal, reltime) RETURNS reltime LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7155, proiswin="t");

 CREATE FUNCTION last_value_tid(internal, tid) RETURNS tid LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7157, proiswin="t");

 CREATE FUNCTION last_value_time(internal, "time") RETURNS "time" LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7158, proiswin="t");

 CREATE FUNCTION last_value_timestamp(internal, "timestamp") RETURNS "timestamp" LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7159, proiswin="t");

 CREATE FUNCTION last_value_timestamptz(internal, timestamptz) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7160, proiswin="t");

 CREATE FUNCTION last_value_timetz(internal, timetz) RETURNS timetz LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7161, proiswin="t");

 CREATE FUNCTION last_value_varbit(internal, varbit) RETURNS varbit LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7162, proiswin="t");

 CREATE FUNCTION last_value_varchar(internal, "varchar") RETURNS "varchar" LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7163, proiswin="t");

 CREATE FUNCTION last_value_xid(internal, xid) RETURNS xid LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7164, proiswin="t");

 CREATE FUNCTION last_value_bytea(internal, bytea) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7239, proiswin="t");

 CREATE FUNCTION last_value_bit(internal, "bit") RETURNS "bit" LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7259, proiswin="t");

 CREATE FUNCTION last_value_box(internal, box) RETURNS box LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7275, proiswin="t");

 CREATE FUNCTION last_value_any(internal, anyarray) RETURNS anyarray LANGUAGE internal IMMUTABLE STRICT AS 'last_value_generic' WITH (OID=7291, proiswin="t");

 CREATE FUNCTION cume_dist_prelim(internal) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'cume_dist_prelim' WITH (OID=7204, DESCRIPTION="window preliminary function");
-- #define CUME_DIST_PRELIM_OID 7204
-- #define CUME_DIST_PRELIM_TYPE 20

 CREATE FUNCTION ntile_prelim_int(internal, int4) RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'ntile_prelim_int' WITH (OID=7205, DESCRIPTION="window preliminary function");

 CREATE FUNCTION ntile_prelim_bigint(internal, int8) RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'ntile_prelim_bigint' WITH (OID=7206, DESCRIPTION="window preliminary function");

 CREATE FUNCTION ntile_prelim_numeric(internal, "numeric") RETURNS _int8 LANGUAGE internal IMMUTABLE STRICT AS 'ntile_prelim_numeric' WITH (OID=7207, DESCRIPTION="window preliminary function");

 CREATE FUNCTION percent_rank_final(int8, int8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'percent_rank_final' WITH (OID=7303, DESCRIPTION="window final function");

 CREATE FUNCTION cume_dist_final(int8, int8) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'cume_dist_final' WITH (OID=7304, DESCRIPTION="window final function");

 CREATE FUNCTION ntile_final(_int8, int8) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'ntile_final' WITH (OID=7305, DESCRIPTION="window final function");

 CREATE FUNCTION get_ao_distribution(IN reloid oid, OUT segmentid int4, OUT tupcount float8) RETURNS SETOF pg_catalog.record LANGUAGE internal VOLATILE READS SQL DATA AS 'get_ao_distribution_oid' WITH (OID=7169, DESCRIPTION="show append only table tuple distribution across segment databases");

 CREATE FUNCTION get_ao_distribution(IN relname text, OUT segmentid int4, OUT tupcount float8) RETURNS SETOF pg_catalog.record LANGUAGE internal VOLATILE READS SQL DATA AS 'get_ao_distribution_name' WITH (OID=7170, DESCRIPTION="show append only table tuple distribution across segment databases");

 CREATE FUNCTION get_ao_compression_ratio(oid) RETURNS float8 LANGUAGE internal VOLATILE READS SQL DATA AS 'get_ao_compression_ratio_oid' WITH (OID=7171, DESCRIPTION="show append only table compression ratio");

 CREATE FUNCTION get_ao_compression_ratio(text) RETURNS float8 LANGUAGE internal VOLATILE READS SQL DATA AS 'get_ao_compression_ratio_name' WITH (OID=7172, DESCRIPTION="show append only table compression ratio");

 CREATE FUNCTION gp_update_ao_master_stats(oid) RETURNS float8 LANGUAGE internal VOLATILE MODIFIES SQL DATA AS 'gp_update_ao_master_stats_oid' WITH (OID=7173, DESCRIPTION="append only tables utility function");

 CREATE FUNCTION gp_update_ao_master_stats(text) RETURNS float8 LANGUAGE internal VOLATILE MODIFIES SQL DATA AS 'gp_update_ao_master_stats_name' WITH (OID=7174, DESCRIPTION="append only tables utility function");

 CREATE FUNCTION gp_persistent_build_db(bool) RETURNS int4 LANGUAGE internal VOLATILE AS 'gp_persistent_build_db' WITH (OID=7178, DESCRIPTION="populate the persistent tables and gp_relation_node for the current database");

 CREATE FUNCTION gp_persistent_build_all(bool) RETURNS int4 LANGUAGE internal VOLATILE AS 'gp_persistent_build_all' WITH (OID=7179, DESCRIPTION="populate the persistent tables and gp_relation_node for the whole database instance");

 CREATE FUNCTION gp_persistent_reset_all() RETURNS int4 LANGUAGE internal VOLATILE AS 'gp_persistent_reset_all' WITH (OID=7180, DESCRIPTION="Remove the persistent tables and gp_relation_node for the whole database instance");

 CREATE FUNCTION gp_persistent_repair_delete(int4, tid) RETURNS int4 LANGUAGE internal VOLATILE AS 'gp_persistent_repair_delete' WITH (OID=7181, DESCRIPTION="Remove an entry specified by TID from a persistent table for the current database instance");

 CREATE FUNCTION gp_persistent_set_relation_bufpool_kind_all() RETURNS int4 LANGUAGE internal VOLATILE AS 'gp_persistent_set_relation_bufpool_kind_all' WITH (OID=7182, DESCRIPTION="Populate the gp_persistent_relation_node table's relation_bufpool_kind column for the whole database instance for upgrade from 4.0 to 4.1");

-- GIN array support
 CREATE FUNCTION ginarrayextract(anyarray, internal) RETURNS internal LANGUAGE internal IMMUTABLE STRICT AS 'ginarrayextract' WITH (OID=2743, DESCRIPTION="GIN array support");

 CREATE FUNCTION ginarrayconsistent(internal, int2, internal) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'ginarrayconsistent' WITH (OID=2744, DESCRIPTION="GIN array support");

-- overlap/contains/contained
 CREATE FUNCTION arrayoverlap(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'arrayoverlap' WITH (OID=2747, DESCRIPTION="overlaps");

 CREATE FUNCTION arraycontains(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'arraycontains' WITH (OID=2748, DESCRIPTION="contains");

 CREATE FUNCTION arraycontained(anyarray, anyarray) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'arraycontained' WITH (OID=2749, DESCRIPTION="is contained by");

-- userlock replacements
 CREATE FUNCTION pg_advisory_lock(int8) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_lock_int8' WITH (OID=2880, DESCRIPTION="obtain exclusive advisory lock");

 CREATE FUNCTION pg_advisory_lock_shared(int8) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_lock_shared_int8' WITH (OID=2881, DESCRIPTION="obtain shared advisory lock");

 CREATE FUNCTION pg_try_advisory_lock(int8) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_try_advisory_lock_int8' WITH (OID=2882, DESCRIPTION="obtain exclusive advisory lock if available");

 CREATE FUNCTION pg_try_advisory_lock_shared(int8) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_try_advisory_lock_shared_int8' WITH (OID=2883, DESCRIPTION="obtain shared advisory lock if available");

 CREATE FUNCTION pg_advisory_unlock(int8) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_unlock_int8' WITH (OID=2884, DESCRIPTION="release exclusive advisory lock");

 CREATE FUNCTION pg_advisory_unlock_shared(int8) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_unlock_shared_int8' WITH (OID=2885, DESCRIPTION="release shared advisory lock");

 CREATE FUNCTION pg_advisory_lock(int4, int4) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_lock_int4' WITH (OID=2886, DESCRIPTION="obtain exclusive advisory lock");

 CREATE FUNCTION pg_advisory_lock_shared(int4, int4) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_lock_shared_int4' WITH (OID=2887, DESCRIPTION="obtain shared advisory lock");

 CREATE FUNCTION pg_try_advisory_lock(int4, int4) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_try_advisory_lock_int4' WITH (OID=2888, DESCRIPTION="obtain exclusive advisory lock if available");

 CREATE FUNCTION pg_try_advisory_lock_shared(int4, int4) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_try_advisory_lock_shared_int4' WITH (OID=2889, DESCRIPTION="obtain shared advisory lock if available");

 CREATE FUNCTION pg_advisory_unlock(int4, int4) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_unlock_int4' WITH (OID=2890, DESCRIPTION="release exclusive advisory lock");

 CREATE FUNCTION pg_advisory_unlock_shared(int4, int4) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_unlock_shared_int4' WITH (OID=2891, DESCRIPTION="release shared advisory lock");

 CREATE FUNCTION pg_advisory_unlock_all() RETURNS void LANGUAGE internal VOLATILE STRICT AS 'pg_advisory_unlock_all' WITH (OID=2892, DESCRIPTION="release all advisory locks");


-- xml functions
 CREATE FUNCTION xml_in(cstring) RETURNS xml LANGUAGE internal IMMUTABLE STRICT AS 'xml_in' WITH (OID=2973, DESCRIPTION="I/O");

 CREATE FUNCTION xml_out(xml) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'xml_out' WITH (OID=2974, DESCRIPTION="I/O");

 CREATE FUNCTION xmlcomment(text) RETURNS xml LANGUAGE internal IMMUTABLE STRICT AS 'xmlcomment' WITH (OID=2975, DESCRIPTION="generate XML comment");

 CREATE FUNCTION xml(text) RETURNS xml LANGUAGE internal IMMUTABLE STRICT AS 'texttoxml' WITH (OID=2976, DESCRIPTION="perform a non-validating parse of a character string to produce an XML value");

 CREATE FUNCTION xmlvalidate(xml, text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'xmlvalidate' WITH (OID=2977, DESCRIPTION="validate an XML value");

 CREATE FUNCTION xml_recv(internal) RETURNS xml LANGUAGE internal IMMUTABLE STRICT AS 'xml_recv' WITH (OID=2978, DESCRIPTION="I/O");

 CREATE FUNCTION xml_send(xml) RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'xml_send' WITH (OID=2979, DESCRIPTION="I/O");

 CREATE FUNCTION xmlconcat2(xml, xml) RETURNS xml LANGUAGE internal IMMUTABLE STRICT AS 'xmlconcat2' WITH (OID=2980, DESCRIPTION="aggregate transition function");

 CREATE FUNCTION xmlagg(xml) RETURNS xml LANGUAGE internal IMMUTABLE AS 'aggregate_dummy' WITH (OID=2981, DESCRIPTION="concatenate XML values", proisagg="t");

 CREATE FUNCTION text(xml) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'xmltotext' WITH (OID=2982, DESCRIPTION="serialize an XML value to a character string");

 CREATE FUNCTION xpath(text, xml, _text) RETURNS _xml LANGUAGE internal IMMUTABLE STRICT AS 'xpath' WITH (OID=2983, DESCRIPTION="evaluate XPath expression, with namespaces support");

 CREATE FUNCTION xpath(text, xml) RETURNS _xml LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.xpath($1, $2, '{}'::pg_catalog.text[])$$ WITH (OID=2984, DESCRIPTION="evaluate XPath expression");

 CREATE FUNCTION xmlexists(text, xml) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'xmlexists' WITH (OID=2985, DESCRIPTION="test XML value against XPath expression");

 CREATE FUNCTION xpath_exists(text, xml, _text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'xpath_exists' WITH (OID=2986, DESCRIPTION="test XML value against XPath expression, with namespace support");

 CREATE FUNCTION xpath_exists(text, xml) RETURNS bool LANGUAGE sql IMMUTABLE STRICT AS $$select pg_catalog.xpath_exists($1, $2, '{}'::pg_catalog.text[])$$ WITH (OID=2987, DESCRIPTION="test XML value against XPath expression");

 CREATE FUNCTION xml_is_well_formed(text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'xml_is_well_formed' WITH (OID=2988, DESCRIPTION="determine if a string is well formed XML");

 CREATE FUNCTION xml_is_well_formed_document(text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'xml_is_well_formed_document' WITH (OID=2989, DESCRIPTION="determine if a string is well formed XML document");

 CREATE FUNCTION xml_is_well_formed_content(text) RETURNS bool LANGUAGE internal IMMUTABLE STRICT AS 'xml_is_well_formed_content' WITH (OID=2990, DESCRIPTION="determine if a string is well formed XML content");


-- the bitmap index access method routines
 CREATE FUNCTION bmgettuple(internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'bmgettuple' WITH (OID=3050, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmgetmulti(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmgetmulti' WITH (OID=3051, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bminsert(internal, internal, internal, internal, internal, internal) RETURNS bool LANGUAGE internal VOLATILE STRICT AS 'bminsert' WITH (OID=3001, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmbeginscan(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmbeginscan' WITH (OID=3002, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmrescan(internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmrescan' WITH (OID=3003, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmendscan(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmendscan' WITH (OID=3004, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmmarkpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmmarkpos' WITH (OID=3005, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmrestrpos(internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmrestrpos' WITH (OID=3006, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmbuild(internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmbuild' WITH (OID=3007, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmbulkdelete(internal, internal, internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmbulkdelete' WITH (OID=3008, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmvacuumcleanup(internal, internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'bmvacuumcleanup' WITH (OID=3009);

 CREATE FUNCTION bmcostestimate(internal, internal, internal, internal, internal, internal, internal, internal) RETURNS void LANGUAGE internal VOLATILE STRICT AS 'bmcostestimate' WITH (OID=3010, DESCRIPTION="bitmap(internal)");

 CREATE FUNCTION bmoptions(_text, bool) RETURNS bytea LANGUAGE internal STABLE STRICT AS 'bmoptions' WITH (OID=3011, DESCRIPTION="btree(internal)");

-- AOCS functions.

 CREATE FUNCTION aocsvpinfo_decode(varbit, int4, int4) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'aocsvpinfo_decode' WITH (OID=9900);

-- raises deprecation error
 CREATE FUNCTION gp_deprecated() RETURNS void LANGUAGE internal IMMUTABLE AS 'gp_deprecated' WITH (OID=9997, DESCRIPTION="raises function deprecation error");

-- A convenient utility
 CREATE FUNCTION pg_objname_to_oid(text) RETURNS oid LANGUAGE internal IMMUTABLE STRICT AS 'pg_objname_to_oid' WITH (OID=9998, DESCRIPTION="convert an object name to oid");

-- Fault injection
 CREATE FUNCTION gp_fault_inject(int4, int8) RETURNS int8 LANGUAGE internal VOLATILE STRICT AS 'gp_fault_inject' WITH (OID=9999, DESCRIPTION="Greenplum fault testing only");

-- Analyze related
 CREATE FUNCTION gp_statistics_estimate_reltuples_relpages_oid(oid) RETURNS _float4 LANGUAGE internal VOLATILE STRICT AS 'gp_statistics_estimate_reltuples_relpages_oid' WITH (OID=5032, DESCRIPTION="Return reltuples/relpages information for relation.");

-- Backoff related
 CREATE FUNCTION gp_adjust_priority(int4, int4, int4) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'gp_adjust_priority_int' WITH (OID=5040, DESCRIPTION="change weight of all the backends for a given session id");

 CREATE FUNCTION gp_adjust_priority(int4, int4, text) RETURNS int4 LANGUAGE internal VOLATILE STRICT AS 'gp_adjust_priority_value' WITH (OID=5041, DESCRIPTION="change weight of all the backends for a given session id");

 CREATE FUNCTION gp_list_backend_priorities() RETURNS SETOF record LANGUAGE internal VOLATILE AS 'gp_list_backend_priorities' WITH (OID=5042, DESCRIPTION="list priorities of backends");


-- elog related
 CREATE FUNCTION gp_elog(text) RETURNS void LANGUAGE internal IMMUTABLE STRICT AS 'gp_elog' WITH (OID=5044, DESCRIPTION="Insert text into the error log");

 CREATE FUNCTION gp_elog(text, bool) RETURNS void LANGUAGE internal IMMUTABLE STRICT AS 'gp_elog' WITH (OID=5045, DESCRIPTION="Insert text into the error log");

-- Segment and master administration functions, see utils/gp/segadmin.c
 CREATE FUNCTION gp_add_master_standby(text, text, _text) RETURNS int2 LANGUAGE internal VOLATILE AS 'gp_add_master_standby' WITH (OID=5046, DESCRIPTION="Perform the catalog operations necessary for adding a new standby");

 CREATE FUNCTION gp_add_master_standby(text, text, _text, int4) RETURNS int2 LANGUAGE internal VOLATILE AS 'gp_add_master_standby_port' WITH (OID=5038, DESCRIPTION="Perform the catalog operations necessary for adding a new standby");

 CREATE FUNCTION gp_remove_master_standby() RETURNS bool LANGUAGE internal VOLATILE AS 'gp_remove_master_standby' WITH (OID=5047, DESCRIPTION="Remove a master standby from the system catalog");

 CREATE FUNCTION gp_add_segment_mirror(int2, text, text, int4, int4, _text) RETURNS int2 LANGUAGE internal VOLATILE AS 'gp_add_segment_mirror' WITH (OID=5048, DESCRIPTION="Perform the catalog operations necessary for adding a new segment mirror");

 CREATE FUNCTION gp_remove_segment_mirror(int2) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_remove_segment_mirror' WITH (OID=5049, DESCRIPTION="Remove a segment mirror from the system catalog");

 CREATE FUNCTION gp_add_segment(text, text, int4, _text) RETURNS int2 LANGUAGE internal VOLATILE AS 'gp_add_segment' WITH (OID=5050, DESCRIPTION="Perform the catalog operations necessary for adding a new primary segment");

 CREATE FUNCTION gp_remove_segment(int2) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_remove_segment' WITH (OID=5051, DESCRIPTION="Remove a primary segment from the system catalog");

 CREATE FUNCTION gp_prep_new_segment(_text) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_prep_new_segment' WITH (OID=5052, DESCRIPTION="Convert a cloned master catalog for use as a segment");

 CREATE FUNCTION gp_activate_standby() RETURNS bool LANGUAGE internal VOLATILE AS 'gp_activate_standby' WITH (OID=5053, DESCRIPTION="Activate a standby");

-- We cheat in the following two functions: they are technically volatile but
-- we can only dispatch them if they're immutable :(.


 CREATE FUNCTION gp_add_segment_persistent_entries(int2, int2, _text) RETURNS bool LANGUAGE internal IMMUTABLE AS 'gp_add_segment_persistent_entries' WITH (OID=5054, DESCRIPTION="Persist object nodes on a segment");

 CREATE FUNCTION gp_remove_segment_persistent_entries(int2, int2) RETURNS bool LANGUAGE internal IMMUTABLE AS 'gp_remove_segment_persistent_entries' WITH (OID=5055, DESCRIPTION="Remove persistent object node references at a segment");

-- persistent table repair functions

 CREATE FUNCTION gp_add_persistent_filespace_node_entry(tid, oid, int2, text, int2, text, int2, int8, int2, int4, int4, int8, tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_add_persistent_filespace_node_entry' WITH (OID=5056, DESCRIPTION="Add a new entry to gp_persistent_filespace_node");

 CREATE FUNCTION gp_add_persistent_tablespace_node_entry(tid, oid, oid, int2, int8, int2, int4, int4, int8, tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_add_persistent_tablespace_node_entry' WITH (OID=5057, DESCRIPTION="Add a new entry to gp_persistent_tablespace_node");

 CREATE FUNCTION gp_add_persistent_database_node_entry(tid, oid, oid, int2, int8, int2, int4, int4, int8, tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_add_persistent_database_node_entry' WITH (OID=5058, DESCRIPTION="Add a new entry to gp_persistent_database_node");

 CREATE FUNCTION gp_add_persistent_relation_node_entry(tid, oid, oid, oid, int4, int2, int2, int8, int2, int2, bool, int8, gpxlogloc, int4, int8, int8, int4, int4, int8, tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_add_persistent_relation_node_entry' WITH (OID=5059, DESCRIPTION="Add a new entry to gp_persistent_relation_node");

 CREATE FUNCTION gp_add_global_sequence_entry(tid, int8) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_add_global_sequence_entry' WITH (OID=5060, DESCRIPTION="Add a new entry to gp_global_sequence");

 CREATE FUNCTION gp_add_relation_node_entry(tid, oid, int4, int8, tid, int8) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_add_relation_node_entry' WITH (OID=5061, DESCRIPTION="Add a new entry to gp_relation_node");

 CREATE FUNCTION gp_update_persistent_filespace_node_entry(tid, oid, int2, text, int2, text, int2, int8, int2, int4, int4, int8, tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_update_persistent_filespace_node_entry' WITH (OID=5062, DESCRIPTION="Update an entry in gp_persistent_filespace_node");

 CREATE FUNCTION gp_update_persistent_tablespace_node_entry(tid, oid, oid, int2, int8, int2, int4, int4, int8, tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_update_persistent_tablespace_node_entry' WITH (OID=5063, DESCRIPTION="Update an entry in gp_persistent_tablespace_node");

 CREATE FUNCTION gp_update_persistent_database_node_entry(tid, oid, oid, int2, int8, int2, int4, int4, int8, tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_update_persistent_database_node_entry' WITH (OID=5064, DESCRIPTION="Update an entry in gp_persistent_database_node");

 CREATE FUNCTION gp_update_persistent_relation_node_entry(tid, oid, oid, oid, int4, int2, int2, int8, int2, int2, bool, int8, gpxlogloc, int4, int8, int8, int4, int4, int8, tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_update_persistent_relation_node_entry' WITH (OID=5065, DESCRIPTION="Update an entry in gp_persistent_relation_node");

 CREATE FUNCTION gp_update_global_sequence_entry(tid, int8) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_update_global_sequence_entry' WITH (OID=5066, DESCRIPTION="Update an entry in gp_global_sequence");

 CREATE FUNCTION gp_update_relation_node_entry(tid, oid, int4, int8, tid, int8) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_update_relation_node_entry' WITH (OID=5067, DESCRIPTION="Update an entry in gp_relation_node");

 CREATE FUNCTION gp_delete_persistent_filespace_node_entry(tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_delete_persistent_filespace_node_entry' WITH (OID=5068, DESCRIPTION="Remove an entry from gp_persistent_filespace_node");

 CREATE FUNCTION gp_delete_persistent_tablespace_node_entry(tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_delete_persistent_tablespace_node_entry' WITH (OID=5069, DESCRIPTION="Remove an entry from gp_persistent_tablespace_node");

 CREATE FUNCTION gp_delete_persistent_database_node_entry(tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_delete_persistent_database_node_entry' WITH (OID=5070, DESCRIPTION="Remove an entry from gp_persistent_database_node");

 CREATE FUNCTION gp_delete_persistent_relation_node_entry(tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_delete_persistent_relation_node_entry' WITH (OID=5071, DESCRIPTION="Remove an entry from gp_persistent_relation_node");

 CREATE FUNCTION gp_delete_global_sequence_entry(tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_delete_global_sequence_entry' WITH (OID=5072, DESCRIPTION="Remove an entry from gp_global_sequence");

 CREATE FUNCTION gp_delete_relation_node_entry(tid) RETURNS bool LANGUAGE internal VOLATILE AS 'gp_delete_relation_node_entry' WITH (OID=5073, DESCRIPTION="Remove an entry from gp_relation_node");

 CREATE FUNCTION gp_persistent_relation_node_check() RETURNS SETOF gp_persistent_relation_node LANGUAGE internal VOLATILE AS 'gp_persistent_relation_node_check' WITH (OID=5074, DESCRIPTION="physical filesystem information");

CREATE FUNCTION gp_dbspecific_ptcat_verification() RETURNS bool LANGUAGE internal VOLATILE AS 'gp_dbspecific_ptcat_verification' WITH (OID=5075, DESCRIPTION="perform database specific PersistentTables-Catalog verification");

CREATE FUNCTION gp_nondbspecific_ptcat_verification() RETURNS bool LANGUAGE internal VOLATILE AS 'gp_nondbspecific_ptcat_verification' WITH (OID=5080, DESCRIPTION="perform non-database specific PersistentTables-Catalog verification");

 CREATE FUNCTION cosh(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'dcosh' WITH (OID=3539, DESCRIPTION="Hyperbolic cosine function");

 CREATE FUNCTION sinh(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'dsinh' WITH (OID=3540, DESCRIPTION="Hyperbolic sine function");

 CREATE FUNCTION tanh(float8) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'dtanh' WITH (OID=3541, DESCRIPTION="Hyperbolic tangent function");

 CREATE FUNCTION anytable_in(cstring) RETURNS anytable LANGUAGE internal IMMUTABLE STRICT AS 'anytable_in' WITH (OID=3054, DESCRIPTION="anytable type serialization input function");

 CREATE FUNCTION anytable_out(anytable) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'anytable_out' WITH (OID=3055, DESCRIPTION="anytable type serialization output function");

 CREATE FUNCTION gp_quicklz_constructor(internal, internal, bool) RETURNS internal LANGUAGE internal VOLATILE AS 'quicklz_constructor' WITH (OID=5076, DESCRIPTION="quicklz constructor");

 CREATE FUNCTION gp_quicklz_destructor(internal) RETURNS void LANGUAGE internal VOLATILE AS 'quicklz_destructor' WITH(OID=5077, DESCRIPTION="quicklz destructor");

 CREATE FUNCTION gp_quicklz_compress(internal, int4, internal, int4, internal, internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'quicklz_compress' WITH(OID=5078, DESCRIPTION="quicklz compressor");

 CREATE FUNCTION gp_quicklz_decompress(internal, int4, internal, int4, internal, internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'quicklz_decompress' WITH(OID=5079, DESCRIPTION="quicklz decompressor");

 CREATE FUNCTION gp_quicklz_validator(internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'quicklz_validator' WITH(OID=9925, DESCRIPTION="quicklz compression validator");

 CREATE FUNCTION gp_zlib_constructor(internal, internal, bool) RETURNS internal LANGUAGE internal VOLATILE AS 'zlib_constructor' WITH (OID=9910, DESCRIPTION="zlib constructor");

 CREATE FUNCTION gp_zlib_destructor(internal) RETURNS void LANGUAGE internal VOLATILE AS 'zlib_destructor' WITH(OID=9911, DESCRIPTION="zlib destructor");

 CREATE FUNCTION gp_zlib_compress(internal, int4, internal, int4, internal, internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'zlib_compress' WITH(OID=9912, DESCRIPTION="zlib compressor");

 CREATE FUNCTION gp_zlib_decompress(internal, int4, internal, int4, internal, internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'zlib_decompress' WITH(OID=9913, DESCRIPTION="zlib decompressor");

 CREATE FUNCTION gp_zlib_validator(internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'zlib_validator' WITH(OID=9924, DESCRIPTION="zlib compression validator");

 CREATE FUNCTION gp_rle_type_constructor(internal, internal, bool) RETURNS internal LANGUAGE internal VOLATILE AS 'rle_type_constructor' WITH (OID=9914, DESCRIPTION="Type specific RLE constructor");

 CREATE FUNCTION gp_rle_type_destructor(internal) RETURNS void LANGUAGE internal VOLATILE AS 'rle_type_destructor' WITH(OID=9915, DESCRIPTION="Type specific RLE destructor");

 CREATE FUNCTION gp_rle_type_compress(internal, int4, internal, int4, internal, internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'rle_type_compress' WITH(OID=9916, DESCRIPTION="Type specific RLE compressor");

 CREATE FUNCTION gp_rle_type_decompress(internal, int4, internal, int4, internal, internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'rle_type_decompress' WITH(OID=9917, DESCRIPTION="Type specific RLE decompressor");

 CREATE FUNCTION gp_rle_type_validator(internal) RETURNS void LANGUAGE internal IMMUTABLE AS 'rle_type_validator' WITH(OID=9923, DESCRIPTION="Type speific RLE compression validator");

 CREATE FUNCTION gp_dummy_compression_constructor(internal, internal, bool) RETURNS internal LANGUAGE internal VOLATILE AS 'dummy_compression_constructor' WITH (OID=3064, DESCRIPTION="Dummy compression destructor");

 CREATE FUNCTION gp_dummy_compression_destructor(internal) RETURNS internal LANGUAGE internal VOLATILE AS 'dummy_compression_destructor' WITH (OID=3065, DESCRIPTION="Dummy compression destructor");

 CREATE FUNCTION gp_dummy_compression_compress(internal, int4, internal, int4, internal, internal) RETURNS internal LANGUAGE internal VOLATILE AS 'dummy_compression_compress' WITH (OID=3066, DESCRIPTION="Dummy compression compressor");

 CREATE FUNCTION gp_dummy_compression_decompress(internal, int4, internal, int4, internal, internal) RETURNS internal LANGUAGE internal VOLATILE AS 'dummy_compression_decompress' WITH (OID=3067, DESCRIPTION="Dummy compression decompressor");

 CREATE FUNCTION gp_dummy_compression_validator(internal) RETURNS internal LANGUAGE internal VOLATILE AS 'dummy_compression_validator' WITH (OID=3068, DESCRIPTION="Dummy compression validator");

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, int8, anyelement, int8 ) RETURNS int8 LANGUAGE internal IMMUTABLE STRICT AS 'linterp_int64' WITH (OID=6072, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, int4, anyelement, int4 ) RETURNS int4 LANGUAGE internal IMMUTABLE STRICT AS 'linterp_int32' WITH (OID=6073, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, int2, anyelement, int2 ) RETURNS int2 LANGUAGE internal IMMUTABLE STRICT AS 'linterp_int16' WITH (OID=6074, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, float8, anyelement, float8 ) RETURNS float8 LANGUAGE internal IMMUTABLE STRICT AS 'linterp_float8' WITH (OID=6075, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, float4, anyelement, float4 ) RETURNS float4 LANGUAGE internal IMMUTABLE STRICT AS 'linterp_float4' WITH (OID=6076, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, date, anyelement, date ) RETURNS date LANGUAGE internal IMMUTABLE STRICT AS 'linterp_DateADT' WITH (OID=6077, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, time, anyelement, time ) RETURNS time LANGUAGE internal IMMUTABLE STRICT AS 'linterp_TimeADT' WITH (OID=6078, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, timestamp, anyelement, timestamp ) RETURNS timestamp LANGUAGE internal IMMUTABLE STRICT AS 'linterp_Timestamp' WITH (OID=6079, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, timestamptz, anyelement, timestamptz ) RETURNS timestamptz LANGUAGE internal IMMUTABLE STRICT AS 'linterp_TimestampTz' WITH (OID=6080, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, "interval", anyelement, "interval" ) RETURNS "interval" LANGUAGE internal IMMUTABLE STRICT AS 'linterp_Interval' WITH (OID=6081, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION linear_interpolate( anyelement, anyelement, "numeric", anyelement, "numeric" ) RETURNS "numeric" LANGUAGE internal IMMUTABLE STRICT AS 'linterp_Numeric' WITH (OID=6082, DESCRIPTION="linear interpolation: x, x0,y0, x1,y1"); 

 CREATE FUNCTION lead_lag_frame_maker(internal) RETURNS internal LANGUAGE internal VOLATILE STRICT AS 'lead_lag_frame_maker' WITH (OID=5081);

 CREATE FUNCTION gp_partition_propagation(int4, oid) RETURNS void  LANGUAGE internal VOLATILE STRICT AS 'gp_partition_propagation' WITH (OID=6083, DESCRIPTION="inserts a partition oid into specified pid-index");

 CREATE FUNCTION gp_partition_selection(oid, anyelement) RETURNS oid LANGUAGE internal STABLE STRICT AS 'gp_partition_selection' WITH (OID=6084, DESCRIPTION="selects the child partition oid which satisfies a given partition key value");

 CREATE FUNCTION gp_partition_expansion(oid) RETURNS setof oid LANGUAGE internal STABLE STRICT AS 'gp_partition_expansion' WITH (OID=6085, DESCRIPTION="finds all child partition oids for the given parent oid");

 CREATE FUNCTION gp_partition_inverse(oid) RETURNS setof record LANGUAGE internal STABLE STRICT AS 'gp_partition_inverse' WITH (OID=6086, DESCRIPTION="returns all child partitition oids with their constraints for a given parent oid");

 CREATE FUNCTION disable_xform(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'disable_xform' WITH (OID=6087, DESCRIPTION="disables transformations in the optimizer");

 CREATE FUNCTION enable_xform(text) RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'enable_xform' WITH (OID=6088, DESCRIPTION="enables transformations in the optimizer");

 CREATE FUNCTION gp_opt_version() RETURNS text LANGUAGE internal IMMUTABLE STRICT AS 'gp_opt_version' WITH (OID=6089, DESCRIPTION="Returns the optimizer and gpos library versions");
