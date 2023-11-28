\echo Use "CREATE EXTENSION unionstore" to load this file. \quit

CREATE FUNCTION pg_cluster_size()
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_cluster_size'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION backpressure_lsns(
    OUT received_lsn pg_lsn,
    OUT disk_consistent_lsn pg_lsn,
    OUT remote_consistent_lsn pg_lsn
)
RETURNS record
AS 'MODULE_PATHNAME', 'backpressure_lsns'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION backpressure_throttling_time()
RETURNS bigint
AS 'MODULE_PATHNAME', 'backpressure_throttling_time'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION local_cache_pages()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'local_cache_pages'
LANGUAGE C PARALLEL SAFE;

-- Create a view for convenient access.
CREATE VIEW local_cache AS
	SELECT P.* FROM local_cache_pages() AS P
	(pageoffs int8, relfilenode int8, reltablespace oid, reldatabase oid,
	 relforknumber int2, relblocknumber int8, accesscount int4);

-- Create union storage engine's access method
CREATE ACCESS METHOD union_store
TYPE table
HANDLER heap_tableam_handler;

CREATE FUNCTION ushashhandler(internal)
    RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'ushashhandler'
LANGUAGE C;

CREATE FUNCTION usbthandler(internal)
    RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'usbthandler'
LANGUAGE C;

CREATE FUNCTION usgisthandler(internal)
    RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'usgisthandler'
LANGUAGE C;

CREATE FUNCTION usginhandler(internal)
    RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'usginhandler'
LANGUAGE C;

CREATE FUNCTION usspghandler(internal)
    RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'usspghandler'
LANGUAGE C;

CREATE FUNCTION usbrinhandler(internal)
    RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'usbrinhandler'
LANGUAGE C;

CREATE FUNCTION usbmhandler(internal)
    RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'usbmhandler'
LANGUAGE C;

CREATE ACCESS METHOD ushash TYPE INDEX HANDLER ushashhandler;
CREATE ACCESS METHOD usbtree TYPE INDEX HANDLER usbthandler;
CREATE ACCESS METHOD usgist TYPE INDEX HANDLER usgisthandler;
CREATE ACCESS METHOD usgin TYPE INDEX HANDLER usginhandler;
CREATE ACCESS METHOD usspgist TYPE INDEX HANDLER usspghandler;
CREATE ACCESS METHOD usbrin TYPE INDEX HANDLER usbrinhandler;
CREATE ACCESS METHOD usbitmap TYPE INDEX HANDLER usbmhandler;

CREATE OPERATOR FAMILY date_ops USING ushash;
CREATE OPERATOR CLASS date_ops
DEFAULT FOR TYPE date USING ushash FAMILY date_ops AS
OPERATOR 1 =(date,date),
FUNCTION 1(date,date) hashint4(int4),
FUNCTION 2(date,date) hashint4extended(int4,int8);
CREATE OPERATOR FAMILY numeric_ops USING ushash;
CREATE OPERATOR CLASS numeric_ops
DEFAULT FOR TYPE numeric USING ushash FAMILY numeric_ops AS
OPERATOR 1 =(numeric,numeric),
FUNCTION 1(numeric,numeric) hash_numeric(numeric),
FUNCTION 2(numeric,numeric) hash_numeric_extended(numeric,int8);
CREATE OPERATOR FAMILY cdbhash_oid_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_oid_ops
FOR TYPE oid USING ushash FAMILY cdbhash_oid_ops AS
OPERATOR 1 =(oid,oid),
FUNCTION 1(oid,oid) cdblegacyhash_oid(oid);
CREATE OPERATOR FAMILY char_bloom_ops USING usbrin;
CREATE OPERATOR CLASS char_bloom_ops
FOR TYPE "char" USING usbrin FAMILY char_bloom_ops AS
OPERATOR 1 =("char","char"),
FUNCTION 1("char","char") brin_bloom_opcinfo(internal),
FUNCTION 2("char","char") brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3("char","char") brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4("char","char") brin_bloom_union(internal,internal,internal),
FUNCTION 5("char","char") brin_bloom_options(internal),
FUNCTION 11("char","char") hashchar("char"),
STORAGE "char";
CREATE OPERATOR FAMILY network_ops USING usbtree;
CREATE OPERATOR CLASS cidr_ops
FOR TYPE inet USING usbtree FAMILY network_ops AS
OPERATOR 1 <(inet,inet),
OPERATOR 2 <=(inet,inet),
OPERATOR 3 =(inet,inet),
OPERATOR 4 >=(inet,inet),
OPERATOR 5 >(inet,inet),
FUNCTION 1(inet,inet) network_cmp(inet,inet),
FUNCTION 2(inet,inet) network_sortsupport(internal),
FUNCTION 4(inet,inet) btequalimage(oid);
CREATE OPERATOR FAMILY network_ops USING usbitmap;
CREATE OPERATOR CLASS cidr_ops
FOR TYPE inet USING usbitmap FAMILY network_ops AS
OPERATOR 1 <(inet,inet),
OPERATOR 2 <=(inet,inet),
OPERATOR 3 =(inet,inet),
OPERATOR 4 >=(inet,inet),
OPERATOR 5 >(inet,inet),
FUNCTION 1(inet,inet) network_cmp(inet,inet),
FUNCTION 2(inet,inet) network_sortsupport(internal),
FUNCTION 4(inet,inet) btequalimage(oid);
CREATE OPERATOR FAMILY bit_ops USING ushash;
CREATE OPERATOR CLASS bit_ops
DEFAULT FOR TYPE bit USING ushash FAMILY bit_ops AS
OPERATOR 1 =(bit,bit),
FUNCTION 1(bit,bit) bithash(bit);
CREATE OPERATOR FAMILY float_ops USING ushash;
CREATE OPERATOR CLASS float8_ops
DEFAULT FOR TYPE float8 USING ushash FAMILY float_ops AS
OPERATOR 1 =(float8,float8),
FUNCTION 1(float8,float8) hashfloat8(float8),
FUNCTION 2(float8,float8) hashfloat8extended(float8,int8);
CREATE OPERATOR CLASS float4_ops
DEFAULT FOR TYPE float4 USING ushash FAMILY float_ops AS
OPERATOR 1 =(float4,float4),
FUNCTION 1(float4,float4) hashfloat4(float4),
FUNCTION 2(float4,float4) hashfloat4extended(float4,int8);
CREATE OPERATOR FAMILY tid_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS tid_minmax_multi_ops
FOR TYPE tid USING usbrin FAMILY tid_minmax_multi_ops AS
OPERATOR 1 <(tid,tid),
OPERATOR 2 <=(tid,tid),
OPERATOR 3 =(tid,tid),
OPERATOR 4 >=(tid,tid),
OPERATOR 5 >(tid,tid),
FUNCTION 1(tid,tid) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(tid,tid) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(tid,tid) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(tid,tid) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(tid,tid) brin_minmax_multi_options(internal),
FUNCTION 11(tid,tid) brin_minmax_multi_distance_tid(internal,internal),
STORAGE tid;
CREATE OPERATOR FAMILY name_minmax_ops USING usbrin;
CREATE OPERATOR CLASS name_minmax_ops
DEFAULT FOR TYPE name USING usbrin FAMILY name_minmax_ops AS
OPERATOR 1 <(name,name),
OPERATOR 2 <=(name,name),
OPERATOR 3 =(name,name),
OPERATOR 4 >=(name,name),
OPERATOR 5 >(name,name),
FUNCTION 1(name,name) brin_minmax_opcinfo(internal),
FUNCTION 2(name,name) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(name,name) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(name,name) brin_minmax_union(internal,internal,internal),
STORAGE name;
CREATE OPERATOR FAMILY timetz_ops USING ushash;
CREATE OPERATOR CLASS timetz_ops
DEFAULT FOR TYPE timetz USING ushash FAMILY timetz_ops AS
OPERATOR 1 =(timetz,timetz),
FUNCTION 1(timetz,timetz) timetz_hash(timetz),
FUNCTION 2(timetz,timetz) timetz_hash_extended(timetz,int8);
CREATE OPERATOR FAMILY quad_point_ops USING usspgist;
CREATE OPERATOR CLASS quad_point_ops
DEFAULT FOR TYPE point USING usspgist FAMILY quad_point_ops AS
OPERATOR 11 |>>(point,point),
OPERATOR 30 >^(point,point),
OPERATOR 1 <<(point,point),
OPERATOR 5 >>(point,point),
OPERATOR 10 <<|(point,point),
OPERATOR 29 <^(point,point),
OPERATOR 6 ~=(point,point),
OPERATOR 15 <->(point,point) FOR ORDER BY float_ops,
FUNCTION 1(point,point) spg_quad_config(internal,internal),
FUNCTION 2(point,point) spg_quad_choose(internal,internal),
FUNCTION 3(point,point) spg_quad_picksplit(internal,internal),
FUNCTION 4(point,point) spg_quad_inner_consistent(internal,internal),
FUNCTION 5(point,point) spg_quad_leaf_consistent(internal,internal);
CREATE OPERATOR FAMILY tsvector_ops USING usgist;
CREATE OPERATOR CLASS tsvector_ops
DEFAULT FOR TYPE tsvector USING usgist FAMILY tsvector_ops AS
FUNCTION 1(tsvector,tsvector) gtsvector_consistent(internal,tsvector,int2,oid,internal),
FUNCTION 2(tsvector,tsvector) gtsvector_union(internal,internal),
FUNCTION 3(tsvector,tsvector) gtsvector_compress(internal),
FUNCTION 4(tsvector,tsvector) gtsvector_decompress(internal),
FUNCTION 5(tsvector,tsvector) gtsvector_penalty(internal,internal,internal),
FUNCTION 6(tsvector,tsvector) gtsvector_picksplit(internal,internal),
FUNCTION 7(tsvector,tsvector) gtsvector_same(gtsvector,gtsvector,internal),
FUNCTION 10(tsvector,tsvector) gtsvector_options(internal),
STORAGE gtsvector;
CREATE OPERATOR FAMILY bpchar_ops USING usbtree;
CREATE OPERATOR CLASS bpchar_ops
DEFAULT FOR TYPE bpchar USING usbtree FAMILY bpchar_ops AS
OPERATOR 1 <(bpchar,bpchar),
OPERATOR 2 <=(bpchar,bpchar),
OPERATOR 3 =(bpchar,bpchar),
OPERATOR 4 >=(bpchar,bpchar),
OPERATOR 5 >(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) bpcharcmp(bpchar,bpchar),
FUNCTION 2(bpchar,bpchar) bpchar_sortsupport(internal),
FUNCTION 4(bpchar,bpchar) btvarstrequalimage(oid);
CREATE OPERATOR FAMILY bpchar_ops USING usbitmap;
CREATE OPERATOR CLASS bpchar_ops
DEFAULT FOR TYPE bpchar USING usbitmap FAMILY bpchar_ops AS
OPERATOR 1 <(bpchar,bpchar),
OPERATOR 2 <=(bpchar,bpchar),
OPERATOR 3 =(bpchar,bpchar),
OPERATOR 4 >=(bpchar,bpchar),
OPERATOR 5 >(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) bpcharcmp(bpchar,bpchar),
FUNCTION 2(bpchar,bpchar) bpchar_sortsupport(internal),
FUNCTION 4(bpchar,bpchar) btvarstrequalimage(oid);
CREATE OPERATOR FAMILY uuid_ops USING ushash;
CREATE OPERATOR CLASS uuid_ops
DEFAULT FOR TYPE uuid USING ushash FAMILY uuid_ops AS
OPERATOR 1 =(uuid,uuid),
FUNCTION 1(uuid,uuid) uuid_hash(uuid),
FUNCTION 2(uuid,uuid) uuid_hash_extended(uuid,int8);
CREATE OPERATOR FAMILY float_ops USING usbtree;
CREATE OPERATOR CLASS float4_ops
DEFAULT FOR TYPE float4 USING usbtree FAMILY float_ops AS
OPERATOR 1 <(float4,float4),
OPERATOR 2 <=(float4,float4),
OPERATOR 3 =(float4,float4),
OPERATOR 4 >=(float4,float4),
OPERATOR 5 >(float4,float4),
FUNCTION 1(float4,float4) btfloat4cmp(float4,float4),
FUNCTION 2(float4,float4) btfloat4sortsupport(internal);
CREATE OPERATOR CLASS float8_ops
DEFAULT FOR TYPE float8 USING usbtree FAMILY float_ops AS
OPERATOR 1 <(float8,float8),
OPERATOR 2 <=(float8,float8),
OPERATOR 3 =(float8,float8),
OPERATOR 4 >=(float8,float8),
OPERATOR 5 >(float8,float8),
FUNCTION 1(float8,float8) btfloat8cmp(float8,float8),
FUNCTION 2(float8,float8) btfloat8sortsupport(internal),
FUNCTION 3(float8,float8) in_range(float8,float8,float8,bool,bool);
CREATE OPERATOR FAMILY float_ops USING usbitmap;
CREATE OPERATOR CLASS float4_ops
DEFAULT FOR TYPE float4 USING usbitmap FAMILY float_ops AS
OPERATOR 1 <(float4,float4),
OPERATOR 2 <=(float4,float4),
OPERATOR 3 =(float4,float4),
OPERATOR 4 >=(float4,float4),
OPERATOR 5 >(float4,float4),
FUNCTION 1(float4,float4) btfloat4cmp(float4,float4),
FUNCTION 2(float4,float4) btfloat4sortsupport(internal);
CREATE OPERATOR CLASS float8_ops
DEFAULT FOR TYPE float8 USING usbitmap FAMILY float_ops AS
OPERATOR 1 <(float8,float8),
OPERATOR 2 <=(float8,float8),
OPERATOR 3 =(float8,float8),
OPERATOR 4 >=(float8,float8),
OPERATOR 5 >(float8,float8),
FUNCTION 1(float8,float8) btfloat8cmp(float8,float8),
FUNCTION 2(float8,float8) btfloat8sortsupport(internal),
FUNCTION 3(float8,float8) in_range(float8,float8,float8,bool,bool);
CREATE OPERATOR FAMILY bytea_ops USING usbtree;
CREATE OPERATOR CLASS bytea_ops
DEFAULT FOR TYPE bytea USING usbtree FAMILY bytea_ops AS
OPERATOR 1 <(bytea,bytea),
OPERATOR 2 <=(bytea,bytea),
OPERATOR 3 =(bytea,bytea),
OPERATOR 4 >=(bytea,bytea),
OPERATOR 5 >(bytea,bytea),
FUNCTION 1(bytea,bytea) byteacmp(bytea,bytea),
FUNCTION 2(bytea,bytea) bytea_sortsupport(internal),
FUNCTION 4(bytea,bytea) btequalimage(oid);
CREATE OPERATOR FAMILY bytea_ops USING usbitmap;
CREATE OPERATOR CLASS bytea_ops
DEFAULT FOR TYPE bytea USING usbitmap FAMILY bytea_ops AS
OPERATOR 1 <(bytea,bytea),
OPERATOR 2 <=(bytea,bytea),
OPERATOR 3 =(bytea,bytea),
OPERATOR 4 >=(bytea,bytea),
OPERATOR 5 >(bytea,bytea),
FUNCTION 1(bytea,bytea) byteacmp(bytea,bytea),
FUNCTION 2(bytea,bytea) bytea_sortsupport(internal),
FUNCTION 4(bytea,bytea) btequalimage(oid);
CREATE OPERATOR FAMILY bit_ops USING usbtree;
CREATE OPERATOR CLASS bit_ops
DEFAULT FOR TYPE bit USING usbtree FAMILY bit_ops AS
OPERATOR 1 <(bit,bit),
OPERATOR 2 <=(bit,bit),
OPERATOR 3 =(bit,bit),
OPERATOR 4 >=(bit,bit),
OPERATOR 5 >(bit,bit),
FUNCTION 1(bit,bit) bitcmp(bit,bit),
FUNCTION 4(bit,bit) btequalimage(oid);
CREATE OPERATOR FAMILY bit_ops USING usbitmap;
CREATE OPERATOR CLASS bit_ops
DEFAULT FOR TYPE bit USING usbitmap FAMILY bit_ops AS
OPERATOR 1 <(bit,bit),
OPERATOR 2 <=(bit,bit),
OPERATOR 3 =(bit,bit),
OPERATOR 4 >=(bit,bit),
OPERATOR 5 >(bit,bit),
FUNCTION 1(bit,bit) bitcmp(bit,bit),
FUNCTION 4(bit,bit) btequalimage(oid);
CREATE OPERATOR FAMILY cdbhash_bit_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_bit_ops
FOR TYPE bit USING ushash FAMILY cdbhash_bit_ops AS
OPERATOR 1 =(bit,bit),
FUNCTION 1(bit,bit) cdblegacyhash_bit(bit);
CREATE OPERATOR FAMILY cdbhash_cash_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_cash_ops
FOR TYPE money USING ushash FAMILY cdbhash_cash_ops AS
OPERATOR 1 =(money,money),
FUNCTION 1(money,money) cdblegacyhash_cash(money);
CREATE OPERATOR FAMILY numeric_ops USING usbtree;
CREATE OPERATOR CLASS numeric_ops
DEFAULT FOR TYPE numeric USING usbtree FAMILY numeric_ops AS
OPERATOR 1 <(numeric,numeric),
OPERATOR 2 <=(numeric,numeric),
OPERATOR 3 =(numeric,numeric),
OPERATOR 4 >=(numeric,numeric),
OPERATOR 5 >(numeric,numeric),
FUNCTION 1(numeric,numeric) numeric_cmp(numeric,numeric),
FUNCTION 2(numeric,numeric) numeric_sortsupport(internal),
FUNCTION 3(numeric,numeric) in_range(numeric,numeric,numeric,bool,bool);
CREATE OPERATOR FAMILY numeric_ops USING usbitmap;
CREATE OPERATOR CLASS numeric_ops
DEFAULT FOR TYPE numeric USING usbitmap FAMILY numeric_ops AS
OPERATOR 1 <(numeric,numeric),
OPERATOR 2 <=(numeric,numeric),
OPERATOR 3 =(numeric,numeric),
OPERATOR 4 >=(numeric,numeric),
OPERATOR 5 >(numeric,numeric),
FUNCTION 1(numeric,numeric) numeric_cmp(numeric,numeric),
FUNCTION 2(numeric,numeric) numeric_sortsupport(internal),
FUNCTION 3(numeric,numeric) in_range(numeric,numeric,numeric,bool,bool);
CREATE OPERATOR FAMILY xid8_ops USING ushash;
CREATE OPERATOR CLASS xid8_ops
DEFAULT FOR TYPE xid8 USING ushash FAMILY xid8_ops AS
OPERATOR 1 =(xid8,xid8),
FUNCTION 1(xid8,xid8) hashint8(int8),
FUNCTION 2(xid8,xid8) hashint8extended(int8,int8);
CREATE OPERATOR FAMILY network_ops USING ushash;
CREATE OPERATOR CLASS inet_ops
DEFAULT FOR TYPE inet USING ushash FAMILY network_ops AS
OPERATOR 1 =(inet,inet),
FUNCTION 1(inet,inet) hashinet(inet),
FUNCTION 2(inet,inet) hashinetextended(inet,int8);
CREATE OPERATOR FAMILY box_inclusion_ops USING usbrin;
CREATE OPERATOR CLASS box_inclusion_ops
DEFAULT FOR TYPE box USING usbrin FAMILY box_inclusion_ops AS
OPERATOR 1 <<(box,box),
OPERATOR 2 &<(box,box),
OPERATOR 3 &&(box,box),
OPERATOR 4 &>(box,box),
OPERATOR 5 >>(box,box),
OPERATOR 6 ~=(box,box),
OPERATOR 7 @>(box,box),
OPERATOR 8 <@(box,box),
OPERATOR 9 &<|(box,box),
OPERATOR 10 <<|(box,box),
OPERATOR 11 |>>(box,box),
OPERATOR 12 |&>(box,box),
FUNCTION 1(box,box) brin_inclusion_opcinfo(internal),
FUNCTION 2(box,box) brin_inclusion_add_value(internal,internal,internal,internal),
FUNCTION 3(box,box) brin_inclusion_consistent(internal,internal,internal),
FUNCTION 4(box,box) brin_inclusion_union(internal,internal,internal),
FUNCTION 11(box,box) bound_box(box,box),
FUNCTION 13(box,box) box_contain(box,box),
STORAGE box;
CREATE OPERATOR FAMILY macaddr8_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS macaddr8_minmax_multi_ops
FOR TYPE macaddr8 USING usbrin FAMILY macaddr8_minmax_multi_ops AS
OPERATOR 1 <(macaddr8,macaddr8),
OPERATOR 2 <=(macaddr8,macaddr8),
OPERATOR 3 =(macaddr8,macaddr8),
OPERATOR 4 >=(macaddr8,macaddr8),
OPERATOR 5 >(macaddr8,macaddr8),
FUNCTION 1(macaddr8,macaddr8) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(macaddr8,macaddr8) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(macaddr8,macaddr8) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(macaddr8,macaddr8) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(macaddr8,macaddr8) brin_minmax_multi_options(internal),
FUNCTION 11(macaddr8,macaddr8) brin_minmax_multi_distance_macaddr8(internal,internal),
STORAGE macaddr8;
CREATE OPERATOR FAMILY interval_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS interval_minmax_multi_ops
FOR TYPE interval USING usbrin FAMILY interval_minmax_multi_ops AS
OPERATOR 1 <(interval,interval),
OPERATOR 2 <=(interval,interval),
OPERATOR 3 =(interval,interval),
OPERATOR 4 >=(interval,interval),
OPERATOR 5 >(interval,interval),
FUNCTION 1(interval,interval) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(interval,interval) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(interval,interval) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(interval,interval) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(interval,interval) brin_minmax_multi_options(internal),
FUNCTION 11(interval,interval) brin_minmax_multi_distance_interval(internal,internal),
STORAGE interval;
CREATE OPERATOR FAMILY jsonb_ops USING usbtree;
CREATE OPERATOR CLASS jsonb_ops
DEFAULT FOR TYPE jsonb USING usbtree FAMILY jsonb_ops AS
OPERATOR 1 <(jsonb,jsonb),
OPERATOR 2 <=(jsonb,jsonb),
OPERATOR 3 =(jsonb,jsonb),
OPERATOR 4 >=(jsonb,jsonb),
OPERATOR 5 >(jsonb,jsonb),
FUNCTION 1(jsonb,jsonb) jsonb_cmp(jsonb,jsonb);
CREATE OPERATOR FAMILY jsonb_ops USING usbitmap;
CREATE OPERATOR CLASS jsonb_ops
DEFAULT FOR TYPE jsonb USING usbitmap FAMILY jsonb_ops AS
OPERATOR 1 <(jsonb,jsonb),
OPERATOR 2 <=(jsonb,jsonb),
OPERATOR 3 =(jsonb,jsonb),
OPERATOR 4 >=(jsonb,jsonb),
OPERATOR 5 >(jsonb,jsonb),
FUNCTION 1(jsonb,jsonb) jsonb_cmp(jsonb,jsonb);
CREATE OPERATOR FAMILY jsonb_ops USING usgin;
CREATE OPERATOR CLASS jsonb_ops
DEFAULT FOR TYPE jsonb USING usgin FAMILY jsonb_ops AS
OPERATOR 7 @>(jsonb,jsonb),
FUNCTION 1(jsonb,jsonb) gin_compare_jsonb(text,text),
FUNCTION 2(jsonb,jsonb) gin_extract_jsonb(jsonb,internal,internal),
FUNCTION 3(jsonb,jsonb) gin_extract_jsonb_query(jsonb,internal,int2,internal,internal,internal,internal),
FUNCTION 4(jsonb,jsonb) gin_consistent_jsonb(internal,int2,jsonb,int4,internal,internal,internal,internal),
FUNCTION 6(jsonb,jsonb) gin_triconsistent_jsonb(internal,int2,jsonb,int4,internal,internal,internal),
STORAGE text;
CREATE OPERATOR FAMILY oid_ops USING ushash;
CREATE OPERATOR CLASS oid_ops
DEFAULT FOR TYPE oid USING ushash FAMILY oid_ops AS
OPERATOR 1 =(oid,oid),
FUNCTION 1(oid,oid) hashoid(oid),
FUNCTION 2(oid,oid) hashoidextended(oid,int8);
CREATE OPERATOR FAMILY cdbhash_tid_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_tid_ops
FOR TYPE tid USING ushash FAMILY cdbhash_tid_ops AS
OPERATOR 1 =(tid,tid),
FUNCTION 1(tid,tid) cdblegacyhash_tid(tid);
CREATE OPERATOR FAMILY float_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS float8_minmax_multi_ops
FOR TYPE float8 USING usbrin FAMILY float_minmax_multi_ops AS
OPERATOR 1 <(float8,float8),
OPERATOR 2 <=(float8,float8),
OPERATOR 3 =(float8,float8),
OPERATOR 4 >=(float8,float8),
OPERATOR 5 >(float8,float8),
FUNCTION 1(float8,float8) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(float8,float8) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(float8,float8) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(float8,float8) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(float8,float8) brin_minmax_multi_options(internal),
FUNCTION 11(float8,float8) brin_minmax_multi_distance_float8(internal,internal);
CREATE OPERATOR CLASS float4_minmax_multi_ops
FOR TYPE float4 USING usbrin FAMILY float_minmax_multi_ops AS
OPERATOR 1 <(float4,float4),
OPERATOR 2 <=(float4,float4),
OPERATOR 3 =(float4,float4),
OPERATOR 4 >=(float4,float4),
OPERATOR 5 >(float4,float4),
FUNCTION 1(float4,float4) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(float4,float4) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(float4,float4) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(float4,float4) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(float4,float4) brin_minmax_multi_options(internal),
FUNCTION 11(float4,float4) brin_minmax_multi_distance_float4(internal,internal),
STORAGE float4;
CREATE OPERATOR FAMILY jsonb_ops USING ushash;
CREATE OPERATOR CLASS jsonb_ops
DEFAULT FOR TYPE jsonb USING ushash FAMILY jsonb_ops AS
OPERATOR 1 =(jsonb,jsonb),
FUNCTION 1(jsonb,jsonb) jsonb_hash(jsonb),
FUNCTION 2(jsonb,jsonb) jsonb_hash_extended(jsonb,int8);
CREATE OPERATOR FAMILY time_ops USING usbtree;
CREATE OPERATOR CLASS time_ops
DEFAULT FOR TYPE time USING usbtree FAMILY time_ops AS
OPERATOR 1 <(time,time),
OPERATOR 2 <=(time,time),
OPERATOR 3 =(time,time),
OPERATOR 4 >=(time,time),
OPERATOR 5 >(time,time),
FUNCTION 1(time,time) time_cmp(time,time),
FUNCTION 4(time,time) btequalimage(oid);
CREATE OPERATOR FAMILY time_ops USING usbitmap;
CREATE OPERATOR CLASS time_ops
DEFAULT FOR TYPE time USING usbitmap FAMILY time_ops AS
OPERATOR 1 <(time,time),
OPERATOR 2 <=(time,time),
OPERATOR 3 =(time,time),
OPERATOR 4 >=(time,time),
OPERATOR 5 >(time,time),
FUNCTION 1(time,time) time_cmp(time,time),
FUNCTION 4(time,time) btequalimage(oid);
CREATE OPERATOR FAMILY timestamptz_ops USING ushash;
CREATE OPERATOR CLASS timestamptz_ops
DEFAULT FOR TYPE timestamptz USING ushash FAMILY timestamptz_ops AS
OPERATOR 1 =(timestamptz,timestamptz),
FUNCTION 1(timestamptz,timestamptz) timestamp_hash(timestamp),
FUNCTION 2(timestamptz,timestamptz) timestamp_hash_extended(timestamp,int8);
CREATE OPERATOR FAMILY timetz_minmax_ops USING usbrin;
CREATE OPERATOR CLASS timetz_minmax_ops
DEFAULT FOR TYPE timetz USING usbrin FAMILY timetz_minmax_ops AS
OPERATOR 1 <(timetz,timetz),
OPERATOR 2 <=(timetz,timetz),
OPERATOR 3 =(timetz,timetz),
OPERATOR 4 >=(timetz,timetz),
OPERATOR 5 >(timetz,timetz),
FUNCTION 1(timetz,timetz) brin_minmax_opcinfo(internal),
FUNCTION 2(timetz,timetz) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(timetz,timetz) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(timetz,timetz) brin_minmax_union(internal,internal,internal),
STORAGE timetz;
CREATE OPERATOR FAMILY aclitem_ops USING ushash;
CREATE OPERATOR CLASS aclitem_ops
DEFAULT FOR TYPE aclitem USING ushash FAMILY aclitem_ops AS
OPERATOR 1 =(aclitem,aclitem),
FUNCTION 1(aclitem,aclitem) hash_aclitem(aclitem),
FUNCTION 2(aclitem,aclitem) hash_aclitem_extended(aclitem,int8);
CREATE OPERATOR FAMILY time_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS time_minmax_multi_ops
FOR TYPE time USING usbrin FAMILY time_minmax_multi_ops AS
OPERATOR 1 <(time,time),
OPERATOR 2 <=(time,time),
OPERATOR 3 =(time,time),
OPERATOR 4 >=(time,time),
OPERATOR 5 >(time,time),
FUNCTION 1(time,time) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(time,time) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(time,time) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(time,time) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(time,time) brin_minmax_multi_options(internal),
FUNCTION 11(time,time) brin_minmax_multi_distance_time(internal,internal),
STORAGE time;
CREATE OPERATOR FAMILY point_ops USING usgist;
CREATE OPERATOR CLASS point_ops
DEFAULT FOR TYPE point USING usgist FAMILY point_ops AS
OPERATOR 11 |>>(point,point),
OPERATOR 30 >^(point,point),
OPERATOR 1 <<(point,point),
OPERATOR 5 >>(point,point),
OPERATOR 10 <<|(point,point),
OPERATOR 29 <^(point,point),
OPERATOR 6 ~=(point,point),
OPERATOR 15 <->(point,point) FOR ORDER BY float_ops,
FUNCTION 1(point,point) gist_point_consistent(internal,point,int2,oid,internal),
FUNCTION 2(point,point) gist_box_union(internal,internal),
FUNCTION 3(point,point) gist_point_compress(internal),
FUNCTION 5(point,point) gist_box_penalty(internal,internal,internal),
FUNCTION 6(point,point) gist_box_picksplit(internal,internal),
FUNCTION 7(point,point) gist_box_same(box,box,internal),
FUNCTION 8(point,point) gist_point_distance(internal,point,int2,oid,internal),
FUNCTION 9(point,point) gist_point_fetch(internal),
FUNCTION 11(point,point) gist_point_sortsupport(internal),
STORAGE box;
CREATE OPERATOR FAMILY datetime_ops USING usbtree;
CREATE OPERATOR CLASS timestamp_ops
DEFAULT FOR TYPE timestamp USING usbtree FAMILY datetime_ops AS
OPERATOR 1 <(timestamp,timestamp),
OPERATOR 2 <=(timestamp,timestamp),
OPERATOR 3 =(timestamp,timestamp),
OPERATOR 4 >=(timestamp,timestamp),
OPERATOR 5 >(timestamp,timestamp),
FUNCTION 1(timestamp,timestamp) timestamp_cmp(timestamp,timestamp),
FUNCTION 2(timestamp,timestamp) timestamp_sortsupport(internal),
FUNCTION 4(timestamp,timestamp) btequalimage(oid);
CREATE OPERATOR CLASS date_ops
DEFAULT FOR TYPE date USING usbtree FAMILY datetime_ops AS
OPERATOR 1 <(date,date),
OPERATOR 2 <=(date,date),
OPERATOR 3 =(date,date),
OPERATOR 4 >=(date,date),
OPERATOR 5 >(date,date),
FUNCTION 1(date,date) date_cmp(date,date),
FUNCTION 2(date,date) date_sortsupport(internal),
FUNCTION 4(date,date) btequalimage(oid);
CREATE OPERATOR CLASS timestamptz_ops
DEFAULT FOR TYPE timestamptz USING usbtree FAMILY datetime_ops AS
OPERATOR 1 <(timestamptz,timestamptz),
OPERATOR 2 <=(timestamptz,timestamptz),
OPERATOR 3 =(timestamptz,timestamptz),
OPERATOR 4 >=(timestamptz,timestamptz),
OPERATOR 5 >(timestamptz,timestamptz),
FUNCTION 1(timestamptz,timestamptz) timestamptz_cmp(timestamptz,timestamptz),
FUNCTION 2(timestamptz,timestamptz) timestamp_sortsupport(internal),
FUNCTION 4(timestamptz,timestamptz) btequalimage(oid);
CREATE OPERATOR FAMILY datetime_ops USING usbitmap;
CREATE OPERATOR CLASS timestamp_ops
DEFAULT FOR TYPE timestamp USING usbitmap FAMILY datetime_ops AS
OPERATOR 1 <(timestamp,timestamp),
OPERATOR 2 <=(timestamp,timestamp),
OPERATOR 3 =(timestamp,timestamp),
OPERATOR 4 >=(timestamp,timestamp),
OPERATOR 5 >(timestamp,timestamp),
FUNCTION 1(timestamp,timestamp) timestamp_cmp(timestamp,timestamp),
FUNCTION 2(timestamp,timestamp) timestamp_sortsupport(internal),
FUNCTION 4(timestamp,timestamp) btequalimage(oid);
CREATE OPERATOR CLASS date_ops
DEFAULT FOR TYPE date USING usbitmap FAMILY datetime_ops AS
OPERATOR 1 <(date,date),
OPERATOR 2 <=(date,date),
OPERATOR 3 =(date,date),
OPERATOR 4 >=(date,date),
OPERATOR 5 >(date,date),
FUNCTION 1(date,date) date_cmp(date,date),
FUNCTION 2(date,date) date_sortsupport(internal),
FUNCTION 4(date,date) btequalimage(oid);
CREATE OPERATOR CLASS timestamptz_ops
DEFAULT FOR TYPE timestamptz USING usbitmap FAMILY datetime_ops AS
OPERATOR 1 <(timestamptz,timestamptz),
OPERATOR 2 <=(timestamptz,timestamptz),
OPERATOR 3 =(timestamptz,timestamptz),
OPERATOR 4 >=(timestamptz,timestamptz),
OPERATOR 5 >(timestamptz,timestamptz),
FUNCTION 1(timestamptz,timestamptz) timestamptz_cmp(timestamptz,timestamptz),
FUNCTION 2(timestamptz,timestamptz) timestamp_sortsupport(internal),
FUNCTION 4(timestamptz,timestamptz) btequalimage(oid);
CREATE OPERATOR FAMILY circle_ops USING usgist;
CREATE OPERATOR CLASS circle_ops
DEFAULT FOR TYPE circle USING usgist FAMILY circle_ops AS
OPERATOR 1 <<(circle,circle),
OPERATOR 2 &<(circle,circle),
OPERATOR 3 &&(circle,circle),
OPERATOR 4 &>(circle,circle),
OPERATOR 5 >>(circle,circle),
OPERATOR 6 ~=(circle,circle),
OPERATOR 7 @>(circle,circle),
OPERATOR 8 <@(circle,circle),
OPERATOR 9 &<|(circle,circle),
OPERATOR 10 <<|(circle,circle),
OPERATOR 11 |>>(circle,circle),
OPERATOR 12 |&>(circle,circle),
FUNCTION 1(circle,circle) gist_circle_consistent(internal,circle,int2,oid,internal),
FUNCTION 2(circle,circle) gist_box_union(internal,internal),
FUNCTION 3(circle,circle) gist_circle_compress(internal),
FUNCTION 5(circle,circle) gist_box_penalty(internal,internal,internal),
FUNCTION 6(circle,circle) gist_box_picksplit(internal,internal),
FUNCTION 7(circle,circle) gist_box_same(box,box,internal),
FUNCTION 8(circle,circle) gist_circle_distance(internal,circle,int2,oid,internal),
STORAGE box;
CREATE OPERATOR FAMILY record_image_ops USING usbtree;
CREATE OPERATOR CLASS record_image_ops
FOR TYPE record USING usbtree FAMILY record_image_ops AS
OPERATOR 1 *<(record,record),
OPERATOR 2 *<=(record,record),
OPERATOR 3 *=(record,record),
OPERATOR 4 *>=(record,record),
OPERATOR 5 *>(record,record),
FUNCTION 1(record,record) btrecordimagecmp(record,record);
CREATE OPERATOR FAMILY record_image_ops USING usbitmap;
CREATE OPERATOR CLASS record_image_ops
FOR TYPE record USING usbitmap FAMILY record_image_ops AS
OPERATOR 1 *<(record,record),
OPERATOR 2 *<=(record,record),
OPERATOR 3 *=(record,record),
OPERATOR 4 *>=(record,record),
OPERATOR 5 *>(record,record),
FUNCTION 1(record,record) btrecordimagecmp(record,record);
CREATE OPERATOR FAMILY text_pattern_ops USING ushash;
CREATE OPERATOR CLASS varchar_pattern_ops
FOR TYPE text USING ushash FAMILY text_pattern_ops AS
OPERATOR 1 =(text,text),
FUNCTION 1(text,text) hashtext(text),
FUNCTION 2(text,text) hashtextextended(text,int8);
CREATE OPERATOR FAMILY cdbhash_char_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_char_ops
FOR TYPE "char" USING ushash FAMILY cdbhash_char_ops AS
OPERATOR 1 =("char","char"),
FUNCTION 1("char","char") cdblegacyhash_char("char");
CREATE OPERATOR FAMILY box_ops USING usgist;
CREATE OPERATOR CLASS box_ops
DEFAULT FOR TYPE box USING usgist FAMILY box_ops AS
OPERATOR 1 <<(box,box),
OPERATOR 2 &<(box,box),
OPERATOR 3 &&(box,box),
OPERATOR 4 &>(box,box),
OPERATOR 5 >>(box,box),
OPERATOR 6 ~=(box,box),
OPERATOR 7 @>(box,box),
OPERATOR 8 <@(box,box),
OPERATOR 9 &<|(box,box),
OPERATOR 10 <<|(box,box),
OPERATOR 11 |>>(box,box),
OPERATOR 12 |&>(box,box),
FUNCTION 1(box,box) gist_box_consistent(internal,box,int2,oid,internal),
FUNCTION 2(box,box) gist_box_union(internal,internal),
FUNCTION 5(box,box) gist_box_penalty(internal,internal,internal),
FUNCTION 6(box,box) gist_box_picksplit(internal,internal),
FUNCTION 7(box,box) gist_box_same(box,box,internal),
FUNCTION 8(box,box) gist_box_distance(internal,box,int2,oid,internal);
CREATE OPERATOR FAMILY oidvector_ops USING ushash;
CREATE OPERATOR CLASS oidvector_ops
DEFAULT FOR TYPE oidvector USING ushash FAMILY oidvector_ops AS
OPERATOR 1 =(oidvector,oidvector),
FUNCTION 1(oidvector,oidvector) hashoidvector(oidvector),
FUNCTION 2(oidvector,oidvector) hashoidvectorextended(oidvector,int8);
CREATE OPERATOR FAMILY bytea_ops USING ushash;
CREATE OPERATOR CLASS bytea_ops
DEFAULT FOR TYPE bytea USING ushash FAMILY bytea_ops AS
OPERATOR 1 =(bytea,bytea),
FUNCTION 1(bytea,bytea) hashvarlena(internal),
FUNCTION 2(bytea,bytea) hashvarlenaextended(internal,int8);
CREATE OPERATOR FAMILY varbit_ops USING usbtree;
CREATE OPERATOR CLASS varbit_ops
DEFAULT FOR TYPE varbit USING usbtree FAMILY varbit_ops AS
OPERATOR 1 <(varbit,varbit),
OPERATOR 2 <=(varbit,varbit),
OPERATOR 3 =(varbit,varbit),
OPERATOR 4 >=(varbit,varbit),
OPERATOR 5 >(varbit,varbit),
FUNCTION 1(varbit,varbit) varbitcmp(varbit,varbit),
FUNCTION 4(varbit,varbit) btequalimage(oid);
CREATE OPERATOR FAMILY varbit_ops USING usbitmap;
CREATE OPERATOR CLASS varbit_ops
DEFAULT FOR TYPE varbit USING usbitmap FAMILY varbit_ops AS
OPERATOR 1 <(varbit,varbit),
OPERATOR 2 <=(varbit,varbit),
OPERATOR 3 =(varbit,varbit),
OPERATOR 4 >=(varbit,varbit),
OPERATOR 5 >(varbit,varbit),
FUNCTION 1(varbit,varbit) varbitcmp(varbit,varbit),
FUNCTION 4(varbit,varbit) btequalimage(oid);
CREATE OPERATOR FAMILY poly_ops USING usspgist;
CREATE OPERATOR CLASS poly_ops
DEFAULT FOR TYPE polygon USING usspgist FAMILY poly_ops AS
OPERATOR 1 <<(polygon,polygon),
OPERATOR 2 &<(polygon,polygon),
OPERATOR 3 &&(polygon,polygon),
OPERATOR 4 &>(polygon,polygon),
OPERATOR 5 >>(polygon,polygon),
OPERATOR 6 ~=(polygon,polygon),
OPERATOR 7 @>(polygon,polygon),
OPERATOR 8 <@(polygon,polygon),
OPERATOR 9 &<|(polygon,polygon),
OPERATOR 10 <<|(polygon,polygon),
OPERATOR 11 |>>(polygon,polygon),
OPERATOR 12 |&>(polygon,polygon),
FUNCTION 1(polygon,polygon) spg_bbox_quad_config(internal,internal),
FUNCTION 2(polygon,polygon) spg_box_quad_choose(internal,internal),
FUNCTION 3(polygon,polygon) spg_box_quad_picksplit(internal,internal),
FUNCTION 4(polygon,polygon) spg_box_quad_inner_consistent(internal,internal),
FUNCTION 5(polygon,polygon) spg_box_quad_leaf_consistent(internal,internal),
FUNCTION 6(polygon,polygon) spg_poly_quad_compress(polygon),
STORAGE box;
CREATE OPERATOR FAMILY money_ops USING usbtree;
CREATE OPERATOR CLASS money_ops
DEFAULT FOR TYPE money USING usbtree FAMILY money_ops AS
OPERATOR 1 <(money,money),
OPERATOR 2 <=(money,money),
OPERATOR 3 =(money,money),
OPERATOR 4 >=(money,money),
OPERATOR 5 >(money,money),
FUNCTION 1(money,money) cash_cmp(money,money),
FUNCTION 4(money,money) btequalimage(oid);
CREATE OPERATOR FAMILY money_ops USING usbitmap;
CREATE OPERATOR CLASS money_ops
DEFAULT FOR TYPE money USING usbitmap FAMILY money_ops AS
OPERATOR 1 <(money,money),
OPERATOR 2 <=(money,money),
OPERATOR 3 =(money,money),
OPERATOR 4 >=(money,money),
OPERATOR 5 >(money,money),
FUNCTION 1(money,money) cash_cmp(money,money),
FUNCTION 4(money,money) btequalimage(oid);
CREATE OPERATOR FAMILY multirange_ops USING usgist;
CREATE OPERATOR CLASS multirange_ops
DEFAULT FOR TYPE anymultirange USING usgist FAMILY multirange_ops AS
OPERATOR 1 <<(anymultirange,anymultirange),
OPERATOR 2 &<(anymultirange,anymultirange),
OPERATOR 3 &&(anymultirange,anymultirange),
OPERATOR 4 &>(anymultirange,anymultirange),
OPERATOR 5 >>(anymultirange,anymultirange),
OPERATOR 6 -|-(anymultirange,anymultirange),
OPERATOR 7 @>(anymultirange,anymultirange),
OPERATOR 8 <@(anymultirange,anymultirange),
OPERATOR 18 =(anymultirange,anymultirange),
FUNCTION 1(anymultirange,anymultirange) multirange_gist_consistent(internal,anymultirange,int2,oid,internal),
FUNCTION 2(anymultirange,anymultirange) range_gist_union(internal,internal),
FUNCTION 3(anymultirange,anymultirange) multirange_gist_compress(internal),
FUNCTION 5(anymultirange,anymultirange) range_gist_penalty(internal,internal,internal),
FUNCTION 6(anymultirange,anymultirange) range_gist_picksplit(internal,internal),
FUNCTION 7(anymultirange,anymultirange) range_gist_same(anyrange,anyrange,internal),
STORAGE anyrange;
CREATE OPERATOR FAMILY name_bloom_ops USING usbrin;
CREATE OPERATOR CLASS name_bloom_ops
FOR TYPE name USING usbrin FAMILY name_bloom_ops AS
OPERATOR 1 =(name,name),
FUNCTION 1(name,name) brin_bloom_opcinfo(internal),
FUNCTION 2(name,name) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(name,name) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(name,name) brin_bloom_union(internal,internal,internal),
FUNCTION 5(name,name) brin_bloom_options(internal),
FUNCTION 11(name,name) hashname(name),
STORAGE name;
CREATE OPERATOR FAMILY text_ops USING usbtree;
CREATE OPERATOR CLASS text_ops
DEFAULT FOR TYPE text USING usbtree FAMILY text_ops AS
OPERATOR 1 <(text,text),
OPERATOR 2 <=(text,text),
OPERATOR 3 =(text,text),
OPERATOR 4 >=(text,text),
OPERATOR 5 >(text,text),
FUNCTION 1(text,text) bttextcmp(text,text),
FUNCTION 2(text,text) bttextsortsupport(internal),
FUNCTION 4(text,text) btvarstrequalimage(oid);
CREATE OPERATOR CLASS name_ops
DEFAULT FOR TYPE name USING usbtree FAMILY text_ops AS
OPERATOR 1 <(name,name),
OPERATOR 2 <=(name,name),
OPERATOR 3 =(name,name),
OPERATOR 4 >=(name,name),
OPERATOR 5 >(name,name),
FUNCTION 1(name,name) btnamecmp(name,name),
FUNCTION 2(name,name) btnamesortsupport(internal),
FUNCTION 4(name,name) btvarstrequalimage(oid);
CREATE OPERATOR FAMILY text_ops USING usbitmap;
CREATE OPERATOR CLASS text_ops
DEFAULT FOR TYPE text USING usbitmap FAMILY text_ops AS
OPERATOR 1 <(text,text),
OPERATOR 2 <=(text,text),
OPERATOR 3 =(text,text),
OPERATOR 4 >=(text,text),
OPERATOR 5 >(text,text),
FUNCTION 1(text,text) bttextcmp(text,text),
FUNCTION 2(text,text) bttextsortsupport(internal),
FUNCTION 4(text,text) btvarstrequalimage(oid);
CREATE OPERATOR CLASS name_ops
DEFAULT FOR TYPE name USING usbitmap FAMILY text_ops AS
OPERATOR 1 <(name,name),
OPERATOR 2 <=(name,name),
OPERATOR 3 =(name,name),
OPERATOR 4 >=(name,name),
OPERATOR 5 >(name,name),
FUNCTION 1(name,name) btnamecmp(name,name),
FUNCTION 2(name,name) btnamesortsupport(internal),
FUNCTION 4(name,name) btvarstrequalimage(oid);
CREATE OPERATOR FAMILY bpchar_bloom_ops USING usbrin;
CREATE OPERATOR CLASS bpchar_bloom_ops
FOR TYPE bpchar USING usbrin FAMILY bpchar_bloom_ops AS
OPERATOR 1 =(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) brin_bloom_opcinfo(internal),
FUNCTION 2(bpchar,bpchar) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(bpchar,bpchar) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(bpchar,bpchar) brin_bloom_union(internal,internal,internal),
FUNCTION 5(bpchar,bpchar) brin_bloom_options(internal),
FUNCTION 11(bpchar,bpchar) hashbpchar(bpchar),
STORAGE bpchar;
CREATE OPERATOR FAMILY kd_point_ops USING usspgist;
CREATE OPERATOR CLASS kd_point_ops
FOR TYPE point USING usspgist FAMILY kd_point_ops AS
OPERATOR 11 |>>(point,point),
OPERATOR 30 >^(point,point),
OPERATOR 1 <<(point,point),
OPERATOR 5 >>(point,point),
OPERATOR 10 <<|(point,point),
OPERATOR 29 <^(point,point),
OPERATOR 6 ~=(point,point),
OPERATOR 15 <->(point,point) FOR ORDER BY float_ops,
FUNCTION 1(point,point) spg_kd_config(internal,internal),
FUNCTION 2(point,point) spg_kd_choose(internal,internal),
FUNCTION 3(point,point) spg_kd_picksplit(internal,internal),
FUNCTION 4(point,point) spg_kd_inner_consistent(internal,internal),
FUNCTION 5(point,point) spg_quad_leaf_consistent(internal,internal);
CREATE OPERATOR FAMILY tsquery_ops USING usgist;
CREATE OPERATOR CLASS tsquery_ops
DEFAULT FOR TYPE tsquery USING usgist FAMILY tsquery_ops AS
OPERATOR 7 @>(tsquery,tsquery),
OPERATOR 8 <@(tsquery,tsquery),
FUNCTION 1(tsquery,tsquery) gtsquery_consistent(internal,tsquery,int2,oid,internal),
FUNCTION 2(tsquery,tsquery) gtsquery_union(internal,internal),
FUNCTION 3(tsquery,tsquery) gtsquery_compress(internal),
FUNCTION 5(tsquery,tsquery) gtsquery_penalty(internal,internal,internal),
FUNCTION 6(tsquery,tsquery) gtsquery_picksplit(internal,internal),
FUNCTION 7(tsquery,tsquery) gtsquery_same(int8,int8,internal),
STORAGE int8;
CREATE OPERATOR FAMILY char_minmax_ops USING usbrin;
CREATE OPERATOR CLASS char_minmax_ops
DEFAULT FOR TYPE "char" USING usbrin FAMILY char_minmax_ops AS
OPERATOR 1 <("char","char"),
OPERATOR 2 <=("char","char"),
OPERATOR 3 =("char","char"),
OPERATOR 4 >=("char","char"),
OPERATOR 5 >("char","char"),
FUNCTION 1("char","char") brin_minmax_opcinfo(internal),
FUNCTION 2("char","char") brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3("char","char") brin_minmax_consistent(internal,internal,internal),
FUNCTION 4("char","char") brin_minmax_union(internal,internal,internal),
STORAGE "char";
CREATE OPERATOR FAMILY tsquery_ops USING usbtree;
CREATE OPERATOR CLASS tsquery_ops
DEFAULT FOR TYPE tsquery USING usbtree FAMILY tsquery_ops AS
OPERATOR 1 <(tsquery,tsquery),
OPERATOR 2 <=(tsquery,tsquery),
OPERATOR 3 =(tsquery,tsquery),
OPERATOR 4 >=(tsquery,tsquery),
OPERATOR 5 >(tsquery,tsquery),
FUNCTION 1(tsquery,tsquery) tsquery_cmp(tsquery,tsquery);
CREATE OPERATOR FAMILY tsquery_ops USING usbitmap;
CREATE OPERATOR CLASS tsquery_ops
DEFAULT FOR TYPE tsquery USING usbitmap FAMILY tsquery_ops AS
OPERATOR 1 <(tsquery,tsquery),
OPERATOR 2 <=(tsquery,tsquery),
OPERATOR 3 =(tsquery,tsquery),
OPERATOR 4 >=(tsquery,tsquery),
OPERATOR 5 >(tsquery,tsquery),
FUNCTION 1(tsquery,tsquery) tsquery_cmp(tsquery,tsquery);
CREATE OPERATOR FAMILY tid_ops USING ushash;
CREATE OPERATOR CLASS tid_ops
DEFAULT FOR TYPE tid USING ushash FAMILY tid_ops AS
OPERATOR 1 =(tid,tid),
FUNCTION 1(tid,tid) hashtid(tid),
FUNCTION 2(tid,tid) hashtidextended(tid,int8);
CREATE OPERATOR FAMILY macaddr_ops USING ushash;
CREATE OPERATOR CLASS macaddr_ops
DEFAULT FOR TYPE macaddr USING ushash FAMILY macaddr_ops AS
OPERATOR 1 =(macaddr,macaddr),
FUNCTION 1(macaddr,macaddr) hashmacaddr(macaddr),
FUNCTION 2(macaddr,macaddr) hashmacaddrextended(macaddr,int8);
CREATE OPERATOR FAMILY cdbhash_varbit_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_varbit_ops
FOR TYPE varbit USING ushash FAMILY cdbhash_varbit_ops AS
OPERATOR 1 =(varbit,varbit),
FUNCTION 1(varbit,varbit) cdblegacyhash_bit(bit);
CREATE OPERATOR FAMILY multirange_ops USING usbtree;
CREATE OPERATOR CLASS multirange_ops
DEFAULT FOR TYPE anymultirange USING usbtree FAMILY multirange_ops AS
OPERATOR 1 <(anymultirange,anymultirange),
OPERATOR 2 <=(anymultirange,anymultirange),
OPERATOR 3 =(anymultirange,anymultirange),
OPERATOR 4 >=(anymultirange,anymultirange),
OPERATOR 5 >(anymultirange,anymultirange),
FUNCTION 1(anymultirange,anymultirange) multirange_cmp(anymultirange,anymultirange);
CREATE OPERATOR FAMILY multirange_ops USING usbitmap;
CREATE OPERATOR CLASS multirange_ops
DEFAULT FOR TYPE anymultirange USING usbitmap FAMILY multirange_ops AS
OPERATOR 1 <(anymultirange,anymultirange),
OPERATOR 2 <=(anymultirange,anymultirange),
OPERATOR 3 =(anymultirange,anymultirange),
OPERATOR 4 >=(anymultirange,anymultirange),
OPERATOR 5 >(anymultirange,anymultirange),
FUNCTION 1(anymultirange,anymultirange) multirange_cmp(anymultirange,anymultirange);
CREATE OPERATOR FAMILY macaddr_ops USING usbtree;
CREATE OPERATOR CLASS macaddr_ops
DEFAULT FOR TYPE macaddr USING usbtree FAMILY macaddr_ops AS
OPERATOR 1 <(macaddr,macaddr),
OPERATOR 2 <=(macaddr,macaddr),
OPERATOR 3 =(macaddr,macaddr),
OPERATOR 4 >=(macaddr,macaddr),
OPERATOR 5 >(macaddr,macaddr),
FUNCTION 1(macaddr,macaddr) macaddr_cmp(macaddr,macaddr),
FUNCTION 2(macaddr,macaddr) macaddr_sortsupport(internal),
FUNCTION 4(macaddr,macaddr) btequalimage(oid);
CREATE OPERATOR FAMILY macaddr_ops USING usbitmap;
CREATE OPERATOR CLASS macaddr_ops
DEFAULT FOR TYPE macaddr USING usbitmap FAMILY macaddr_ops AS
OPERATOR 1 <(macaddr,macaddr),
OPERATOR 2 <=(macaddr,macaddr),
OPERATOR 3 =(macaddr,macaddr),
OPERATOR 4 >=(macaddr,macaddr),
OPERATOR 5 >(macaddr,macaddr),
FUNCTION 1(macaddr,macaddr) macaddr_cmp(macaddr,macaddr),
FUNCTION 2(macaddr,macaddr) macaddr_sortsupport(internal),
FUNCTION 4(macaddr,macaddr) btequalimage(oid);
CREATE OPERATOR FAMILY record_ops USING ushash;
CREATE OPERATOR CLASS record_ops
DEFAULT FOR TYPE record USING ushash FAMILY record_ops AS
OPERATOR 1 =(record,record),
FUNCTION 1(record,record) hash_record(record),
FUNCTION 2(record,record) hash_record_extended(record,int8);
CREATE OPERATOR FAMILY cdbhash_integer_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_int4_ops
FOR TYPE int4 USING ushash FAMILY cdbhash_integer_ops AS
OPERATOR 1 =(int4,int4),
FUNCTION 1(int4,int4) cdblegacyhash_int4(int4);
CREATE OPERATOR CLASS cdbhash_int8_ops
FOR TYPE int8 USING ushash FAMILY cdbhash_integer_ops AS
OPERATOR 1 =(int8,int8),
FUNCTION 1(int8,int8) cdblegacyhash_int8(int8);
CREATE OPERATOR CLASS cdbhash_int2_ops
FOR TYPE int2 USING ushash FAMILY cdbhash_integer_ops AS
OPERATOR 1 =(int2,int2),
FUNCTION 1(int2,int2) cdblegacyhash_int2(int2);
CREATE OPERATOR FAMILY bpchar_pattern_ops USING usbtree;
CREATE OPERATOR CLASS bpchar_pattern_ops
FOR TYPE bpchar USING usbtree FAMILY bpchar_pattern_ops AS
OPERATOR 1 ~<~(bpchar,bpchar),
OPERATOR 2 ~<=~(bpchar,bpchar),
OPERATOR 3 =(bpchar,bpchar),
OPERATOR 4 ~>=~(bpchar,bpchar),
OPERATOR 5 ~>~(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) btbpchar_pattern_cmp(bpchar,bpchar),
FUNCTION 2(bpchar,bpchar) btbpchar_pattern_sortsupport(internal),
FUNCTION 4(bpchar,bpchar) btequalimage(oid);
CREATE OPERATOR FAMILY bpchar_pattern_ops USING usbitmap;
CREATE OPERATOR CLASS bpchar_pattern_ops
FOR TYPE bpchar USING usbitmap FAMILY bpchar_pattern_ops AS
OPERATOR 1 ~<~(bpchar,bpchar),
OPERATOR 2 ~<=~(bpchar,bpchar),
OPERATOR 3 =(bpchar,bpchar),
OPERATOR 4 ~>=~(bpchar,bpchar),
OPERATOR 5 ~>~(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) btbpchar_pattern_cmp(bpchar,bpchar),
FUNCTION 2(bpchar,bpchar) btbpchar_pattern_sortsupport(internal),
FUNCTION 4(bpchar,bpchar) btequalimage(oid);
CREATE OPERATOR FAMILY jsonb_path_ops USING usgin;
CREATE OPERATOR CLASS jsonb_path_ops
FOR TYPE jsonb USING usgin FAMILY jsonb_path_ops AS
OPERATOR 7 @>(jsonb,jsonb),
FUNCTION 1(jsonb,jsonb) btint4cmp(int4,int4),
FUNCTION 2(jsonb,jsonb) gin_extract_jsonb_path(jsonb,internal,internal),
FUNCTION 3(jsonb,jsonb) gin_extract_jsonb_query_path(jsonb,internal,int2,internal,internal,internal,internal),
FUNCTION 4(jsonb,jsonb) gin_consistent_jsonb_path(internal,int2,jsonb,int4,internal,internal,internal,internal),
FUNCTION 6(jsonb,jsonb) gin_triconsistent_jsonb_path(internal,int2,jsonb,int4,internal,internal,internal),
STORAGE int4;
CREATE OPERATOR FAMILY enum_ops USING usbtree;
CREATE OPERATOR CLASS enum_ops
DEFAULT FOR TYPE anyenum USING usbtree FAMILY enum_ops AS
OPERATOR 1 <(anyenum,anyenum),
OPERATOR 2 <=(anyenum,anyenum),
OPERATOR 3 =(anyenum,anyenum),
OPERATOR 4 >=(anyenum,anyenum),
OPERATOR 5 >(anyenum,anyenum),
FUNCTION 1(anyenum,anyenum) enum_cmp(anyenum,anyenum),
FUNCTION 4(anyenum,anyenum) btequalimage(oid);
CREATE OPERATOR FAMILY enum_ops USING usbitmap;
CREATE OPERATOR CLASS enum_ops
DEFAULT FOR TYPE anyenum USING usbitmap FAMILY enum_ops AS
OPERATOR 1 <(anyenum,anyenum),
OPERATOR 2 <=(anyenum,anyenum),
OPERATOR 3 =(anyenum,anyenum),
OPERATOR 4 >=(anyenum,anyenum),
OPERATOR 5 >(anyenum,anyenum),
FUNCTION 1(anyenum,anyenum) enum_cmp(anyenum,anyenum),
FUNCTION 4(anyenum,anyenum) btequalimage(oid);
CREATE OPERATOR FAMILY poly_ops USING usgist;
CREATE OPERATOR CLASS poly_ops
DEFAULT FOR TYPE polygon USING usgist FAMILY poly_ops AS
OPERATOR 1 <<(polygon,polygon),
OPERATOR 2 &<(polygon,polygon),
OPERATOR 3 &&(polygon,polygon),
OPERATOR 4 &>(polygon,polygon),
OPERATOR 5 >>(polygon,polygon),
OPERATOR 6 ~=(polygon,polygon),
OPERATOR 7 @>(polygon,polygon),
OPERATOR 8 <@(polygon,polygon),
OPERATOR 9 &<|(polygon,polygon),
OPERATOR 10 <<|(polygon,polygon),
OPERATOR 11 |>>(polygon,polygon),
OPERATOR 12 |&>(polygon,polygon),
FUNCTION 1(polygon,polygon) gist_poly_consistent(internal,polygon,int2,oid,internal),
FUNCTION 2(polygon,polygon) gist_box_union(internal,internal),
FUNCTION 3(polygon,polygon) gist_poly_compress(internal),
FUNCTION 5(polygon,polygon) gist_box_penalty(internal,internal,internal),
FUNCTION 6(polygon,polygon) gist_box_picksplit(internal,internal),
FUNCTION 7(polygon,polygon) gist_box_same(box,box,internal),
FUNCTION 8(polygon,polygon) gist_poly_distance(internal,polygon,int2,oid,internal),
STORAGE box;
CREATE OPERATOR FAMILY float_minmax_ops USING usbrin;
CREATE OPERATOR CLASS float8_minmax_ops
DEFAULT FOR TYPE float8 USING usbrin FAMILY float_minmax_ops AS
OPERATOR 1 <(float8,float8),
OPERATOR 2 <=(float8,float8),
OPERATOR 3 =(float8,float8),
OPERATOR 4 >=(float8,float8),
OPERATOR 5 >(float8,float8),
FUNCTION 1(float8,float8) brin_minmax_opcinfo(internal),
FUNCTION 2(float8,float8) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(float8,float8) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(float8,float8) brin_minmax_union(internal,internal,internal);
CREATE OPERATOR CLASS float4_minmax_ops
DEFAULT FOR TYPE float4 USING usbrin FAMILY float_minmax_ops AS
OPERATOR 1 <(float4,float4),
OPERATOR 2 <=(float4,float4),
OPERATOR 3 =(float4,float4),
OPERATOR 4 >=(float4,float4),
OPERATOR 5 >(float4,float4),
FUNCTION 1(float4,float4) brin_minmax_opcinfo(internal),
FUNCTION 2(float4,float4) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(float4,float4) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(float4,float4) brin_minmax_union(internal,internal,internal),
STORAGE float4;
CREATE OPERATOR FAMILY multirange_ops USING ushash;
CREATE OPERATOR CLASS multirange_ops
DEFAULT FOR TYPE anymultirange USING ushash FAMILY multirange_ops AS
OPERATOR 1 =(anymultirange,anymultirange),
FUNCTION 1(anymultirange,anymultirange) hash_multirange(anymultirange),
FUNCTION 2(anymultirange,anymultirange) hash_multirange_extended(anymultirange,int8);
CREATE OPERATOR FAMILY bytea_bloom_ops USING usbrin;
CREATE OPERATOR CLASS bytea_bloom_ops
FOR TYPE bytea USING usbrin FAMILY bytea_bloom_ops AS
OPERATOR 1 =(bytea,bytea),
FUNCTION 1(bytea,bytea) brin_bloom_opcinfo(internal),
FUNCTION 2(bytea,bytea) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(bytea,bytea) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(bytea,bytea) brin_bloom_union(internal,internal,internal),
FUNCTION 5(bytea,bytea) brin_bloom_options(internal),
FUNCTION 11(bytea,bytea) hashvarlena(internal),
STORAGE bytea;
CREATE OPERATOR FAMILY timetz_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS timetz_minmax_multi_ops
FOR TYPE timetz USING usbrin FAMILY timetz_minmax_multi_ops AS
OPERATOR 1 <(timetz,timetz),
OPERATOR 2 <=(timetz,timetz),
OPERATOR 3 =(timetz,timetz),
OPERATOR 4 >=(timetz,timetz),
OPERATOR 5 >(timetz,timetz),
FUNCTION 1(timetz,timetz) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(timetz,timetz) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(timetz,timetz) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(timetz,timetz) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(timetz,timetz) brin_minmax_multi_options(internal),
FUNCTION 11(timetz,timetz) brin_minmax_multi_distance_timetz(internal,internal),
STORAGE timetz;
CREATE OPERATOR FAMILY time_minmax_ops USING usbrin;
CREATE OPERATOR CLASS time_minmax_ops
DEFAULT FOR TYPE time USING usbrin FAMILY time_minmax_ops AS
OPERATOR 1 <(time,time),
OPERATOR 2 <=(time,time),
OPERATOR 3 =(time,time),
OPERATOR 4 >=(time,time),
OPERATOR 5 >(time,time),
FUNCTION 1(time,time) brin_minmax_opcinfo(internal),
FUNCTION 2(time,time) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(time,time) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(time,time) brin_minmax_union(internal,internal,internal),
STORAGE time;
CREATE OPERATOR FAMILY cdbhash_time_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_time_ops
FOR TYPE time USING ushash FAMILY cdbhash_time_ops AS
OPERATOR 1 =(time,time),
FUNCTION 1(time,time) cdblegacyhash_time(time);
CREATE OPERATOR FAMILY bool_ops USING ushash;
CREATE OPERATOR CLASS bool_ops
DEFAULT FOR TYPE bool USING ushash FAMILY bool_ops AS
OPERATOR 1 =(bool,bool),
FUNCTION 1(bool,bool) hashchar("char"),
FUNCTION 2(bool,bool) hashcharextended("char",int8);
CREATE OPERATOR FAMILY pg_lsn_minmax_ops USING usbrin;
CREATE OPERATOR CLASS pg_lsn_minmax_ops
DEFAULT FOR TYPE pg_lsn USING usbrin FAMILY pg_lsn_minmax_ops AS
OPERATOR 1 <(pg_lsn,pg_lsn),
OPERATOR 2 <=(pg_lsn,pg_lsn),
OPERATOR 3 =(pg_lsn,pg_lsn),
OPERATOR 4 >=(pg_lsn,pg_lsn),
OPERATOR 5 >(pg_lsn,pg_lsn),
FUNCTION 1(pg_lsn,pg_lsn) brin_minmax_opcinfo(internal),
FUNCTION 2(pg_lsn,pg_lsn) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(pg_lsn,pg_lsn) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(pg_lsn,pg_lsn) brin_minmax_union(internal,internal,internal),
STORAGE pg_lsn;
CREATE OPERATOR FAMILY bytea_minmax_ops USING usbrin;
CREATE OPERATOR CLASS bytea_minmax_ops
DEFAULT FOR TYPE bytea USING usbrin FAMILY bytea_minmax_ops AS
OPERATOR 1 <(bytea,bytea),
OPERATOR 2 <=(bytea,bytea),
OPERATOR 3 =(bytea,bytea),
OPERATOR 4 >=(bytea,bytea),
OPERATOR 5 >(bytea,bytea),
FUNCTION 1(bytea,bytea) brin_minmax_opcinfo(internal),
FUNCTION 2(bytea,bytea) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(bytea,bytea) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(bytea,bytea) brin_minmax_union(internal,internal,internal),
STORAGE bytea;
CREATE OPERATOR FAMILY integer_minmax_ops USING usbrin;
CREATE OPERATOR CLASS int8_minmax_ops
DEFAULT FOR TYPE int8 USING usbrin FAMILY integer_minmax_ops AS
OPERATOR 1 <(int8,int8),
OPERATOR 2 <=(int8,int8),
OPERATOR 3 =(int8,int8),
OPERATOR 4 >=(int8,int8),
OPERATOR 5 >(int8,int8),
FUNCTION 1(int8,int8) brin_minmax_opcinfo(internal),
FUNCTION 2(int8,int8) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(int8,int8) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(int8,int8) brin_minmax_union(internal,internal,internal);
CREATE OPERATOR CLASS int4_minmax_ops
DEFAULT FOR TYPE int4 USING usbrin FAMILY integer_minmax_ops AS
OPERATOR 1 <(int4,int4),
OPERATOR 2 <=(int4,int4),
OPERATOR 3 =(int4,int4),
OPERATOR 4 >=(int4,int4),
OPERATOR 5 >(int4,int4),
FUNCTION 1(int4,int4) brin_minmax_opcinfo(internal),
FUNCTION 2(int4,int4) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(int4,int4) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(int4,int4) brin_minmax_union(internal,internal,internal);
CREATE OPERATOR CLASS int2_minmax_ops
DEFAULT FOR TYPE int2 USING usbrin FAMILY integer_minmax_ops AS
OPERATOR 1 <(int2,int2),
OPERATOR 2 <=(int2,int2),
OPERATOR 3 =(int2,int2),
OPERATOR 4 >=(int2,int2),
OPERATOR 5 >(int2,int2),
FUNCTION 1(int2,int2) brin_minmax_opcinfo(internal),
FUNCTION 2(int2,int2) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(int2,int2) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(int2,int2) brin_minmax_union(internal,internal,internal),
STORAGE int2;
CREATE OPERATOR FAMILY enum_ops USING ushash;
CREATE OPERATOR CLASS enum_ops
DEFAULT FOR TYPE anyenum USING ushash FAMILY enum_ops AS
OPERATOR 1 =(anyenum,anyenum),
FUNCTION 1(anyenum,anyenum) hashenum(anyenum),
FUNCTION 2(anyenum,anyenum) hashenumextended(anyenum,int8);
CREATE OPERATOR FAMILY float_bloom_ops USING usbrin;
CREATE OPERATOR CLASS float4_bloom_ops
FOR TYPE float4 USING usbrin FAMILY float_bloom_ops AS
OPERATOR 1 =(float4,float4),
FUNCTION 1(float4,float4) brin_bloom_opcinfo(internal),
FUNCTION 2(float4,float4) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(float4,float4) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(float4,float4) brin_bloom_union(internal,internal,internal),
FUNCTION 5(float4,float4) brin_bloom_options(internal),
FUNCTION 11(float4,float4) hashfloat4(float4);
CREATE OPERATOR CLASS float8_bloom_ops
FOR TYPE float8 USING usbrin FAMILY float_bloom_ops AS
OPERATOR 1 =(float8,float8),
FUNCTION 1(float8,float8) brin_bloom_opcinfo(internal),
FUNCTION 2(float8,float8) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(float8,float8) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(float8,float8) brin_bloom_union(internal,internal,internal),
FUNCTION 5(float8,float8) brin_bloom_options(internal),
FUNCTION 11(float8,float8) hashfloat8(float8),
STORAGE float8;
CREATE OPERATOR FAMILY time_bloom_ops USING usbrin;
CREATE OPERATOR CLASS time_bloom_ops
FOR TYPE time USING usbrin FAMILY time_bloom_ops AS
OPERATOR 1 =(time,time),
FUNCTION 1(time,time) brin_bloom_opcinfo(internal),
FUNCTION 2(time,time) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(time,time) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(time,time) brin_bloom_union(internal,internal,internal),
FUNCTION 5(time,time) brin_bloom_options(internal),
FUNCTION 11(time,time) time_hash(time),
STORAGE time;
CREATE OPERATOR FAMILY text_ops USING usspgist;
CREATE OPERATOR CLASS text_ops
DEFAULT FOR TYPE text USING usspgist FAMILY text_ops AS
OPERATOR 1 ~<~(text,text),
OPERATOR 2 ~<=~(text,text),
OPERATOR 3 =(text,text),
OPERATOR 4 ~>=~(text,text),
OPERATOR 5 ~>~(text,text),
OPERATOR 11 <(text,text),
OPERATOR 12 <=(text,text),
OPERATOR 14 >=(text,text),
OPERATOR 15 >(text,text),
OPERATOR 28 ^@(text,text),
FUNCTION 1(text,text) spg_text_config(internal,internal),
FUNCTION 2(text,text) spg_text_choose(internal,internal),
FUNCTION 3(text,text) spg_text_picksplit(internal,internal),
FUNCTION 4(text,text) spg_text_inner_consistent(internal,internal),
FUNCTION 5(text,text) spg_text_leaf_consistent(internal,internal);
CREATE OPERATOR FAMILY text_ops USING ushash;
CREATE OPERATOR CLASS varchar_ops
FOR TYPE text USING ushash FAMILY text_ops AS
OPERATOR 1 =(text,text),
FUNCTION 1(text,text) hashtext(text),
FUNCTION 2(text,text) hashtextextended(text,int8);
CREATE OPERATOR CLASS name_ops
DEFAULT FOR TYPE name USING ushash FAMILY text_ops AS
OPERATOR 1 =(name,name),
FUNCTION 1(name,name) hashname(name),
FUNCTION 2(name,name) hashnameextended(name,int8);
CREATE OPERATOR FAMILY xid8_ops USING usbtree;
CREATE OPERATOR CLASS xid8_ops
DEFAULT FOR TYPE xid8 USING usbtree FAMILY xid8_ops AS
OPERATOR 1 <(xid8,xid8),
OPERATOR 2 <=(xid8,xid8),
OPERATOR 3 =(xid8,xid8),
OPERATOR 4 >=(xid8,xid8),
OPERATOR 5 >(xid8,xid8),
FUNCTION 1(xid8,xid8) xid8cmp(xid8,xid8),
FUNCTION 4(xid8,xid8) btequalimage(oid);
CREATE OPERATOR FAMILY xid8_ops USING usbitmap;
CREATE OPERATOR CLASS xid8_ops
DEFAULT FOR TYPE xid8 USING usbitmap FAMILY xid8_ops AS
OPERATOR 1 <(xid8,xid8),
OPERATOR 2 <=(xid8,xid8),
OPERATOR 3 =(xid8,xid8),
OPERATOR 4 >=(xid8,xid8),
OPERATOR 5 >(xid8,xid8),
FUNCTION 1(xid8,xid8) xid8cmp(xid8,xid8),
FUNCTION 4(xid8,xid8) btequalimage(oid);
CREATE OPERATOR FAMILY range_ops USING usbtree;
CREATE OPERATOR CLASS range_ops
DEFAULT FOR TYPE anyrange USING usbtree FAMILY range_ops AS
OPERATOR 1 <(anyrange,anyrange),
OPERATOR 2 <=(anyrange,anyrange),
OPERATOR 3 =(anyrange,anyrange),
OPERATOR 4 >=(anyrange,anyrange),
OPERATOR 5 >(anyrange,anyrange),
FUNCTION 1(anyrange,anyrange) range_cmp(anyrange,anyrange);
CREATE OPERATOR FAMILY range_ops USING usbitmap;
CREATE OPERATOR CLASS range_ops
DEFAULT FOR TYPE anyrange USING usbitmap FAMILY range_ops AS
OPERATOR 1 <(anyrange,anyrange),
OPERATOR 2 <=(anyrange,anyrange),
OPERATOR 3 =(anyrange,anyrange),
OPERATOR 4 >=(anyrange,anyrange),
OPERATOR 5 >(anyrange,anyrange),
FUNCTION 1(anyrange,anyrange) range_cmp(anyrange,anyrange);
CREATE OPERATOR FAMILY oid_ops USING usbtree;
CREATE OPERATOR CLASS oid_ops
DEFAULT FOR TYPE oid USING usbtree FAMILY oid_ops AS
OPERATOR 1 <(oid,oid),
OPERATOR 2 <=(oid,oid),
OPERATOR 3 =(oid,oid),
OPERATOR 4 >=(oid,oid),
OPERATOR 5 >(oid,oid),
FUNCTION 1(oid,oid) btoidcmp(oid,oid),
FUNCTION 2(oid,oid) btoidsortsupport(internal),
FUNCTION 4(oid,oid) btequalimage(oid);
CREATE OPERATOR FAMILY oid_ops USING usbitmap;
CREATE OPERATOR CLASS oid_ops
DEFAULT FOR TYPE oid USING usbitmap FAMILY oid_ops AS
OPERATOR 1 <(oid,oid),
OPERATOR 2 <=(oid,oid),
OPERATOR 3 =(oid,oid),
OPERATOR 4 >=(oid,oid),
OPERATOR 5 >(oid,oid),
FUNCTION 1(oid,oid) btoidcmp(oid,oid),
FUNCTION 2(oid,oid) btoidsortsupport(internal),
FUNCTION 4(oid,oid) btequalimage(oid);
CREATE OPERATOR FAMILY cdbhash_timetz_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_timetz_ops
FOR TYPE timetz USING ushash FAMILY cdbhash_timetz_ops AS
OPERATOR 1 =(timetz,timetz),
FUNCTION 1(timetz,timetz) cdblegacyhash_timetz(timetz);
CREATE OPERATOR FAMILY datetime_minmax_ops USING usbrin;
CREATE OPERATOR CLASS timestamp_minmax_ops
DEFAULT FOR TYPE timestamp USING usbrin FAMILY datetime_minmax_ops AS
OPERATOR 1 <(timestamp,timestamp),
OPERATOR 2 <=(timestamp,timestamp),
OPERATOR 3 =(timestamp,timestamp),
OPERATOR 4 >=(timestamp,timestamp),
OPERATOR 5 >(timestamp,timestamp),
FUNCTION 1(timestamp,timestamp) brin_minmax_opcinfo(internal),
FUNCTION 2(timestamp,timestamp) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(timestamp,timestamp) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(timestamp,timestamp) brin_minmax_union(internal,internal,internal);
CREATE OPERATOR CLASS timestamptz_minmax_ops
DEFAULT FOR TYPE timestamptz USING usbrin FAMILY datetime_minmax_ops AS
OPERATOR 1 <(timestamptz,timestamptz),
OPERATOR 2 <=(timestamptz,timestamptz),
OPERATOR 3 =(timestamptz,timestamptz),
OPERATOR 4 >=(timestamptz,timestamptz),
OPERATOR 5 >(timestamptz,timestamptz),
FUNCTION 1(timestamptz,timestamptz) brin_minmax_opcinfo(internal),
FUNCTION 2(timestamptz,timestamptz) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(timestamptz,timestamptz) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(timestamptz,timestamptz) brin_minmax_union(internal,internal,internal);
CREATE OPERATOR CLASS date_minmax_ops
DEFAULT FOR TYPE date USING usbrin FAMILY datetime_minmax_ops AS
OPERATOR 1 <(date,date),
OPERATOR 2 <=(date,date),
OPERATOR 3 =(date,date),
OPERATOR 4 >=(date,date),
OPERATOR 5 >(date,date),
FUNCTION 1(date,date) brin_minmax_opcinfo(internal),
FUNCTION 2(date,date) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(date,date) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(date,date) brin_minmax_union(internal,internal,internal),
STORAGE date;
CREATE OPERATOR FAMILY network_ops USING usgist;
CREATE OPERATOR CLASS inet_ops
FOR TYPE inet USING usgist FAMILY network_ops AS
OPERATOR 3 &&(inet,inet),
OPERATOR 18 =(inet,inet),
OPERATOR 19 <>(inet,inet),
OPERATOR 20 <(inet,inet),
OPERATOR 21 <=(inet,inet),
OPERATOR 22 >(inet,inet),
OPERATOR 23 >=(inet,inet),
OPERATOR 24 <<(inet,inet),
OPERATOR 25 <<=(inet,inet),
OPERATOR 26 >>(inet,inet),
OPERATOR 27 >>=(inet,inet),
FUNCTION 1(inet,inet) inet_gist_consistent(internal,inet,int2,oid,internal),
FUNCTION 2(inet,inet) inet_gist_union(internal,internal),
FUNCTION 3(inet,inet) inet_gist_compress(internal),
FUNCTION 5(inet,inet) inet_gist_penalty(internal,internal,internal),
FUNCTION 6(inet,inet) inet_gist_picksplit(internal,internal),
FUNCTION 7(inet,inet) inet_gist_same(inet,inet,internal),
FUNCTION 9(inet,inet) inet_gist_fetch(internal);
CREATE OPERATOR FAMILY datetime_bloom_ops USING usbrin;
CREATE OPERATOR CLASS timestamp_bloom_ops
FOR TYPE timestamp USING usbrin FAMILY datetime_bloom_ops AS
OPERATOR 1 =(timestamp,timestamp),
FUNCTION 1(timestamp,timestamp) brin_bloom_opcinfo(internal),
FUNCTION 2(timestamp,timestamp) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(timestamp,timestamp) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(timestamp,timestamp) brin_bloom_union(internal,internal,internal),
FUNCTION 5(timestamp,timestamp) brin_bloom_options(internal),
FUNCTION 11(timestamp,timestamp) timestamp_hash(timestamp);
CREATE OPERATOR CLASS timestamptz_bloom_ops
FOR TYPE timestamptz USING usbrin FAMILY datetime_bloom_ops AS
OPERATOR 1 =(timestamptz,timestamptz),
FUNCTION 1(timestamptz,timestamptz) brin_bloom_opcinfo(internal),
FUNCTION 2(timestamptz,timestamptz) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(timestamptz,timestamptz) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(timestamptz,timestamptz) brin_bloom_union(internal,internal,internal),
FUNCTION 5(timestamptz,timestamptz) brin_bloom_options(internal),
FUNCTION 11(timestamptz,timestamptz) timestamp_hash(timestamp);
CREATE OPERATOR CLASS date_bloom_ops
FOR TYPE date USING usbrin FAMILY datetime_bloom_ops AS
OPERATOR 1 =(date,date),
FUNCTION 1(date,date) brin_bloom_opcinfo(internal),
FUNCTION 2(date,date) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(date,date) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(date,date) brin_bloom_union(internal,internal,internal),
FUNCTION 5(date,date) brin_bloom_options(internal),
FUNCTION 11(date,date) hashint4(int4),
STORAGE date;
CREATE OPERATOR FAMILY array_ops USING usbtree;
CREATE OPERATOR CLASS array_ops
DEFAULT FOR TYPE anyarray USING usbtree FAMILY array_ops AS
OPERATOR 1 <(anyarray,anyarray),
OPERATOR 2 <=(anyarray,anyarray),
OPERATOR 3 =(anyarray,anyarray),
OPERATOR 4 >=(anyarray,anyarray),
OPERATOR 5 >(anyarray,anyarray),
FUNCTION 1(anyarray,anyarray) btarraycmp(anyarray,anyarray);
CREATE OPERATOR FAMILY array_ops USING usbitmap;
CREATE OPERATOR CLASS array_ops
DEFAULT FOR TYPE anyarray USING usbitmap FAMILY array_ops AS
OPERATOR 1 <(anyarray,anyarray),
OPERATOR 2 <=(anyarray,anyarray),
OPERATOR 3 =(anyarray,anyarray),
OPERATOR 4 >=(anyarray,anyarray),
OPERATOR 5 >(anyarray,anyarray),
FUNCTION 1(anyarray,anyarray) btarraycmp(anyarray,anyarray);
CREATE OPERATOR FAMILY cid_ops USING ushash;
CREATE OPERATOR CLASS cid_ops
DEFAULT FOR TYPE cid USING ushash FAMILY cid_ops AS
OPERATOR 1 =(cid,cid),
FUNCTION 1(cid,cid) hashint4(int4),
FUNCTION 2(cid,cid) hashint4extended(int4,int8);
CREATE OPERATOR FAMILY integer_bloom_ops USING usbrin;
CREATE OPERATOR CLASS int2_bloom_ops
FOR TYPE int2 USING usbrin FAMILY integer_bloom_ops AS
OPERATOR 1 =(int2,int2),
FUNCTION 1(int2,int2) brin_bloom_opcinfo(internal),
FUNCTION 2(int2,int2) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(int2,int2) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(int2,int2) brin_bloom_union(internal,internal,internal),
FUNCTION 5(int2,int2) brin_bloom_options(internal),
FUNCTION 11(int2,int2) hashint2(int2);
CREATE OPERATOR CLASS int4_bloom_ops
FOR TYPE int4 USING usbrin FAMILY integer_bloom_ops AS
OPERATOR 1 =(int4,int4),
FUNCTION 1(int4,int4) brin_bloom_opcinfo(internal),
FUNCTION 2(int4,int4) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(int4,int4) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(int4,int4) brin_bloom_union(internal,internal,internal),
FUNCTION 5(int4,int4) brin_bloom_options(internal),
FUNCTION 11(int4,int4) hashint4(int4);
CREATE OPERATOR CLASS int8_bloom_ops
FOR TYPE int8 USING usbrin FAMILY integer_bloom_ops AS
OPERATOR 1 =(int8,int8),
FUNCTION 1(int8,int8) brin_bloom_opcinfo(internal),
FUNCTION 2(int8,int8) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(int8,int8) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(int8,int8) brin_bloom_union(internal,internal,internal),
FUNCTION 5(int8,int8) brin_bloom_options(internal),
FUNCTION 11(int8,int8) hashint8(int8),
STORAGE int8;
CREATE OPERATOR FAMILY cdbhash_uuid_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_uuid_ops
FOR TYPE uuid USING ushash FAMILY cdbhash_uuid_ops AS
OPERATOR 1 =(uuid,uuid),
FUNCTION 1(uuid,uuid) cdblegacyhash_uuid(uuid);
CREATE OPERATOR FAMILY pg_lsn_ops USING ushash;
CREATE OPERATOR CLASS pg_lsn_ops
DEFAULT FOR TYPE pg_lsn USING ushash FAMILY pg_lsn_ops AS
OPERATOR 1 =(pg_lsn,pg_lsn),
FUNCTION 1(pg_lsn,pg_lsn) pg_lsn_hash(pg_lsn),
FUNCTION 2(pg_lsn,pg_lsn) pg_lsn_hash_extended(pg_lsn,int8);
CREATE OPERATOR FAMILY cdbhash_float8_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_float8_ops
FOR TYPE float8 USING ushash FAMILY cdbhash_float8_ops AS
OPERATOR 1 =(float8,float8),
FUNCTION 1(float8,float8) cdblegacyhash_float8(float8);
CREATE OPERATOR FAMILY char_ops USING usbtree;
CREATE OPERATOR CLASS char_ops
DEFAULT FOR TYPE "char" USING usbtree FAMILY char_ops AS
OPERATOR 1 <("char","char"),
OPERATOR 2 <=("char","char"),
OPERATOR 3 =("char","char"),
OPERATOR 4 >=("char","char"),
OPERATOR 5 >("char","char"),
FUNCTION 1("char","char") btcharcmp("char","char"),
FUNCTION 4("char","char") btequalimage(oid);
CREATE OPERATOR FAMILY char_ops USING usbitmap;
CREATE OPERATOR CLASS char_ops
DEFAULT FOR TYPE "char" USING usbitmap FAMILY char_ops AS
OPERATOR 1 <("char","char"),
OPERATOR 2 <=("char","char"),
OPERATOR 3 =("char","char"),
OPERATOR 4 >=("char","char"),
OPERATOR 5 >("char","char"),
FUNCTION 1("char","char") btcharcmp("char","char"),
FUNCTION 4("char","char") btequalimage(oid);
CREATE OPERATOR FAMILY datetime_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS timestamptz_minmax_multi_ops
FOR TYPE timestamptz USING usbrin FAMILY datetime_minmax_multi_ops AS
OPERATOR 1 <(timestamptz,timestamptz),
OPERATOR 2 <=(timestamptz,timestamptz),
OPERATOR 3 =(timestamptz,timestamptz),
OPERATOR 4 >=(timestamptz,timestamptz),
OPERATOR 5 >(timestamptz,timestamptz),
FUNCTION 1(timestamptz,timestamptz) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(timestamptz,timestamptz) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(timestamptz,timestamptz) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(timestamptz,timestamptz) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(timestamptz,timestamptz) brin_minmax_multi_options(internal),
FUNCTION 11(timestamptz,timestamptz) brin_minmax_multi_distance_timestamp(internal,internal);
CREATE OPERATOR CLASS date_minmax_multi_ops
FOR TYPE date USING usbrin FAMILY datetime_minmax_multi_ops AS
OPERATOR 1 <(date,date),
OPERATOR 2 <=(date,date),
OPERATOR 3 =(date,date),
OPERATOR 4 >=(date,date),
OPERATOR 5 >(date,date),
FUNCTION 1(date,date) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(date,date) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(date,date) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(date,date) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(date,date) brin_minmax_multi_options(internal),
FUNCTION 11(date,date) brin_minmax_multi_distance_date(internal,internal);
CREATE OPERATOR CLASS timestamp_minmax_multi_ops
FOR TYPE timestamp USING usbrin FAMILY datetime_minmax_multi_ops AS
OPERATOR 1 <(timestamp,timestamp),
OPERATOR 2 <=(timestamp,timestamp),
OPERATOR 3 =(timestamp,timestamp),
OPERATOR 4 >=(timestamp,timestamp),
OPERATOR 5 >(timestamp,timestamp),
FUNCTION 1(timestamp,timestamp) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(timestamp,timestamp) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(timestamp,timestamp) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(timestamp,timestamp) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(timestamp,timestamp) brin_minmax_multi_options(internal),
FUNCTION 11(timestamp,timestamp) brin_minmax_multi_distance_timestamp(internal,internal),
STORAGE timestamp;
CREATE OPERATOR FAMILY cdbhash_timestamptz_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_timestamptz_ops
FOR TYPE timestamptz USING ushash FAMILY cdbhash_timestamptz_ops AS
OPERATOR 1 =(timestamptz,timestamptz),
FUNCTION 1(timestamptz,timestamptz) cdblegacyhash_timestamptz(timestamptz);
CREATE OPERATOR FAMILY text_pattern_ops USING usbtree;
CREATE OPERATOR CLASS text_pattern_ops
FOR TYPE text USING usbtree FAMILY text_pattern_ops AS
OPERATOR 1 ~<~(text,text),
OPERATOR 2 ~<=~(text,text),
OPERATOR 3 =(text,text),
OPERATOR 4 ~>=~(text,text),
OPERATOR 5 ~>~(text,text),
FUNCTION 1(text,text) bttext_pattern_cmp(text,text),
FUNCTION 2(text,text) bttext_pattern_sortsupport(internal),
FUNCTION 4(text,text) btequalimage(oid);
CREATE OPERATOR FAMILY text_pattern_ops USING usbitmap;
CREATE OPERATOR CLASS text_pattern_ops
FOR TYPE text USING usbitmap FAMILY text_pattern_ops AS
OPERATOR 1 ~<~(text,text),
OPERATOR 2 ~<=~(text,text),
OPERATOR 3 =(text,text),
OPERATOR 4 ~>=~(text,text),
OPERATOR 5 ~>~(text,text),
FUNCTION 1(text,text) bttext_pattern_cmp(text,text),
FUNCTION 2(text,text) bttext_pattern_sortsupport(internal),
FUNCTION 4(text,text) btequalimage(oid);
CREATE OPERATOR FAMILY integer_ops USING usbtree;
CREATE OPERATOR CLASS int8_ops
DEFAULT FOR TYPE int8 USING usbtree FAMILY integer_ops AS
OPERATOR 1 <(int8,int8),
OPERATOR 2 <=(int8,int8),
OPERATOR 3 =(int8,int8),
OPERATOR 4 >=(int8,int8),
OPERATOR 5 >(int8,int8),
FUNCTION 1(int8,int8) btint8cmp(int8,int8),
FUNCTION 2(int8,int8) btint8sortsupport(internal),
FUNCTION 4(int8,int8) btequalimage(oid),
FUNCTION 3(int8,int8) in_range(int8,int8,int8,bool,bool);
CREATE OPERATOR CLASS int2_ops
DEFAULT FOR TYPE int2 USING usbtree FAMILY integer_ops AS
OPERATOR 1 <(int2,int2),
OPERATOR 2 <=(int2,int2),
OPERATOR 3 =(int2,int2),
OPERATOR 4 >=(int2,int2),
OPERATOR 5 >(int2,int2),
FUNCTION 1(int2,int2) btint2cmp(int2,int2),
FUNCTION 2(int2,int2) btint2sortsupport(internal),
FUNCTION 4(int2,int2) btequalimage(oid),
FUNCTION 3(int2,int2) in_range(int2,int2,int2,bool,bool);
CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING usbtree FAMILY integer_ops AS
OPERATOR 1 <(int4,int4),
OPERATOR 2 <=(int4,int4),
OPERATOR 3 =(int4,int4),
OPERATOR 4 >=(int4,int4),
OPERATOR 5 >(int4,int4),
FUNCTION 1(int4,int4) btint4cmp(int4,int4),
FUNCTION 2(int4,int4) btint4sortsupport(internal),
FUNCTION 4(int4,int4) btequalimage(oid),
FUNCTION 3(int4,int4) in_range(int4,int4,int4,bool,bool);
CREATE OPERATOR FAMILY integer_ops USING usbitmap;
CREATE OPERATOR CLASS int8_ops
DEFAULT FOR TYPE int8 USING usbitmap FAMILY integer_ops AS
OPERATOR 1 <(int8,int8),
OPERATOR 2 <=(int8,int8),
OPERATOR 3 =(int8,int8),
OPERATOR 4 >=(int8,int8),
OPERATOR 5 >(int8,int8),
FUNCTION 1(int8,int8) btint8cmp(int8,int8),
FUNCTION 2(int8,int8) btint8sortsupport(internal),
FUNCTION 4(int8,int8) btequalimage(oid),
FUNCTION 3(int8,int8) in_range(int8,int8,int8,bool,bool);
CREATE OPERATOR CLASS int2_ops
DEFAULT FOR TYPE int2 USING usbitmap FAMILY integer_ops AS
OPERATOR 1 <(int2,int2),
OPERATOR 2 <=(int2,int2),
OPERATOR 3 =(int2,int2),
OPERATOR 4 >=(int2,int2),
OPERATOR 5 >(int2,int2),
FUNCTION 1(int2,int2) btint2cmp(int2,int2),
FUNCTION 2(int2,int2) btint2sortsupport(internal),
FUNCTION 4(int2,int2) btequalimage(oid),
FUNCTION 3(int2,int2) in_range(int2,int2,int2,bool,bool);
CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING usbitmap FAMILY integer_ops AS
OPERATOR 1 <(int4,int4),
OPERATOR 2 <=(int4,int4),
OPERATOR 3 =(int4,int4),
OPERATOR 4 >=(int4,int4),
OPERATOR 5 >(int4,int4),
FUNCTION 1(int4,int4) btint4cmp(int4,int4),
FUNCTION 2(int4,int4) btint4sortsupport(internal),
FUNCTION 4(int4,int4) btequalimage(oid),
FUNCTION 3(int4,int4) in_range(int4,int4,int4,bool,bool);
CREATE OPERATOR FAMILY record_ops USING usbtree;
CREATE OPERATOR CLASS record_ops
DEFAULT FOR TYPE record USING usbtree FAMILY record_ops AS
OPERATOR 1 <(record,record),
OPERATOR 2 <=(record,record),
OPERATOR 3 =(record,record),
OPERATOR 4 >=(record,record),
OPERATOR 5 >(record,record),
FUNCTION 1(record,record) btrecordcmp(record,record);
CREATE OPERATOR FAMILY record_ops USING usbitmap;
CREATE OPERATOR CLASS record_ops
DEFAULT FOR TYPE record USING usbitmap FAMILY record_ops AS
OPERATOR 1 <(record,record),
OPERATOR 2 <=(record,record),
OPERATOR 3 =(record,record),
OPERATOR 4 >=(record,record),
OPERATOR 5 >(record,record),
FUNCTION 1(record,record) btrecordcmp(record,record);
CREATE OPERATOR FAMILY cdbhash_timestamp_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_timestamp_ops
FOR TYPE timestamp USING ushash FAMILY cdbhash_timestamp_ops AS
OPERATOR 1 =(timestamp,timestamp),
FUNCTION 1(timestamp,timestamp) cdblegacyhash_timestamp(timestamp);
CREATE OPERATOR FAMILY tid_ops USING usbtree;
CREATE OPERATOR CLASS tid_ops
DEFAULT FOR TYPE tid USING usbtree FAMILY tid_ops AS
OPERATOR 1 <(tid,tid),
OPERATOR 2 <=(tid,tid),
OPERATOR 3 =(tid,tid),
OPERATOR 4 >=(tid,tid),
OPERATOR 5 >(tid,tid),
FUNCTION 1(tid,tid) bttidcmp(tid,tid),
FUNCTION 4(tid,tid) btequalimage(oid);
CREATE OPERATOR FAMILY tid_ops USING usbitmap;
CREATE OPERATOR CLASS tid_ops
DEFAULT FOR TYPE tid USING usbitmap FAMILY tid_ops AS
OPERATOR 1 <(tid,tid),
OPERATOR 2 <=(tid,tid),
OPERATOR 3 =(tid,tid),
OPERATOR 4 >=(tid,tid),
OPERATOR 5 >(tid,tid),
FUNCTION 1(tid,tid) bttidcmp(tid,tid),
FUNCTION 4(tid,tid) btequalimage(oid);
CREATE OPERATOR FAMILY integer_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS int4_minmax_multi_ops
FOR TYPE int4 USING usbrin FAMILY integer_minmax_multi_ops AS
OPERATOR 1 <(int4,int4),
OPERATOR 2 <=(int4,int4),
OPERATOR 3 =(int4,int4),
OPERATOR 4 >=(int4,int4),
OPERATOR 5 >(int4,int4),
FUNCTION 1(int4,int4) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(int4,int4) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(int4,int4) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(int4,int4) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(int4,int4) brin_minmax_multi_options(internal),
FUNCTION 11(int4,int4) brin_minmax_multi_distance_int4(internal,internal);
CREATE OPERATOR CLASS int2_minmax_multi_ops
FOR TYPE int2 USING usbrin FAMILY integer_minmax_multi_ops AS
OPERATOR 1 <(int2,int2),
OPERATOR 2 <=(int2,int2),
OPERATOR 3 =(int2,int2),
OPERATOR 4 >=(int2,int2),
OPERATOR 5 >(int2,int2),
FUNCTION 1(int2,int2) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(int2,int2) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(int2,int2) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(int2,int2) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(int2,int2) brin_minmax_multi_options(internal),
FUNCTION 11(int2,int2) brin_minmax_multi_distance_int2(internal,internal);
CREATE OPERATOR CLASS int8_minmax_multi_ops
FOR TYPE int8 USING usbrin FAMILY integer_minmax_multi_ops AS
OPERATOR 1 <(int8,int8),
OPERATOR 2 <=(int8,int8),
OPERATOR 3 =(int8,int8),
OPERATOR 4 >=(int8,int8),
OPERATOR 5 >(int8,int8),
FUNCTION 1(int8,int8) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(int8,int8) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(int8,int8) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(int8,int8) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(int8,int8) brin_minmax_multi_options(internal),
FUNCTION 11(int8,int8) brin_minmax_multi_distance_int8(internal,internal),
STORAGE int8;
CREATE OPERATOR FAMILY cdbhash_bytea_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_bytea_ops
FOR TYPE bytea USING ushash FAMILY cdbhash_bytea_ops AS
OPERATOR 1 =(bytea,bytea),
FUNCTION 1(bytea,bytea) cdblegacyhash_bytea(bytea);
CREATE OPERATOR FAMILY xid_ops USING ushash;
CREATE OPERATOR CLASS xid_ops
DEFAULT FOR TYPE xid USING ushash FAMILY xid_ops AS
OPERATOR 1 =(xid,xid),
FUNCTION 1(xid,xid) hashint4(int4),
FUNCTION 2(xid,xid) hashint4extended(int4,int8);
CREATE OPERATOR FAMILY bpchar_ops USING ushash;
CREATE OPERATOR CLASS bpchar_ops
DEFAULT FOR TYPE bpchar USING ushash FAMILY bpchar_ops AS
OPERATOR 1 =(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) hashbpchar(bpchar),
FUNCTION 2(bpchar,bpchar) hashbpcharextended(bpchar,int8);
CREATE OPERATOR FAMILY cdbhash_float4_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_float4_ops
FOR TYPE float4 USING ushash FAMILY cdbhash_float4_ops AS
OPERATOR 1 =(float4,float4),
FUNCTION 1(float4,float4) cdblegacyhash_float4(float4);
CREATE OPERATOR FAMILY pg_lsn_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS pg_lsn_minmax_multi_ops
FOR TYPE pg_lsn USING usbrin FAMILY pg_lsn_minmax_multi_ops AS
OPERATOR 1 <(pg_lsn,pg_lsn),
OPERATOR 2 <=(pg_lsn,pg_lsn),
OPERATOR 3 =(pg_lsn,pg_lsn),
OPERATOR 4 >=(pg_lsn,pg_lsn),
OPERATOR 5 >(pg_lsn,pg_lsn),
FUNCTION 1(pg_lsn,pg_lsn) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(pg_lsn,pg_lsn) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(pg_lsn,pg_lsn) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(pg_lsn,pg_lsn) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(pg_lsn,pg_lsn) brin_minmax_multi_options(internal),
FUNCTION 11(pg_lsn,pg_lsn) brin_minmax_multi_distance_pg_lsn(internal,internal),
STORAGE pg_lsn;
CREATE OPERATOR FAMILY varbit_minmax_ops USING usbrin;
CREATE OPERATOR CLASS varbit_minmax_ops
DEFAULT FOR TYPE varbit USING usbrin FAMILY varbit_minmax_ops AS
OPERATOR 1 <(varbit,varbit),
OPERATOR 2 <=(varbit,varbit),
OPERATOR 3 =(varbit,varbit),
OPERATOR 4 >=(varbit,varbit),
OPERATOR 5 >(varbit,varbit),
FUNCTION 1(varbit,varbit) brin_minmax_opcinfo(internal),
FUNCTION 2(varbit,varbit) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(varbit,varbit) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(varbit,varbit) brin_minmax_union(internal,internal,internal),
STORAGE varbit;
CREATE OPERATOR FAMILY range_ops USING usgist;
CREATE OPERATOR CLASS range_ops
DEFAULT FOR TYPE anyrange USING usgist FAMILY range_ops AS
OPERATOR 1 <<(anyrange,anyrange),
OPERATOR 2 &<(anyrange,anyrange),
OPERATOR 3 &&(anyrange,anyrange),
OPERATOR 4 &>(anyrange,anyrange),
OPERATOR 5 >>(anyrange,anyrange),
OPERATOR 6 -|-(anyrange,anyrange),
OPERATOR 7 @>(anyrange,anyrange),
OPERATOR 8 <@(anyrange,anyrange),
OPERATOR 18 =(anyrange,anyrange),
FUNCTION 1(anyrange,anyrange) range_gist_consistent(internal,anyrange,int2,oid,internal),
FUNCTION 2(anyrange,anyrange) range_gist_union(internal,internal),
FUNCTION 5(anyrange,anyrange) range_gist_penalty(internal,internal,internal),
FUNCTION 6(anyrange,anyrange) range_gist_picksplit(internal,internal),
FUNCTION 7(anyrange,anyrange) range_gist_same(anyrange,anyrange,internal);
CREATE OPERATOR FAMILY integer_ops USING ushash;
CREATE OPERATOR CLASS int8_ops
DEFAULT FOR TYPE int8 USING ushash FAMILY integer_ops AS
OPERATOR 1 =(int8,int8),
FUNCTION 1(int8,int8) hashint8(int8),
FUNCTION 2(int8,int8) hashint8extended(int8,int8);
CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING ushash FAMILY integer_ops AS
OPERATOR 1 =(int4,int4),
FUNCTION 1(int4,int4) hashint4(int4),
FUNCTION 2(int4,int4) hashint4extended(int4,int8);
CREATE OPERATOR CLASS int2_ops
DEFAULT FOR TYPE int2 USING ushash FAMILY integer_ops AS
OPERATOR 1 =(int2,int2),
FUNCTION 1(int2,int2) hashint2(int2),
FUNCTION 2(int2,int2) hashint2extended(int2,int8);
CREATE OPERATOR FAMILY text_minmax_ops USING usbrin;
CREATE OPERATOR CLASS text_minmax_ops
DEFAULT FOR TYPE text USING usbrin FAMILY text_minmax_ops AS
OPERATOR 1 <(text,text),
OPERATOR 2 <=(text,text),
OPERATOR 3 =(text,text),
OPERATOR 4 >=(text,text),
OPERATOR 5 >(text,text),
FUNCTION 1(text,text) brin_minmax_opcinfo(internal),
FUNCTION 2(text,text) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(text,text) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(text,text) brin_minmax_union(internal,internal,internal),
STORAGE text;
CREATE OPERATOR FAMILY text_bloom_ops USING usbrin;
CREATE OPERATOR CLASS text_bloom_ops
FOR TYPE text USING usbrin FAMILY text_bloom_ops AS
OPERATOR 1 =(text,text),
FUNCTION 1(text,text) brin_bloom_opcinfo(internal),
FUNCTION 2(text,text) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(text,text) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(text,text) brin_bloom_union(internal,internal,internal),
FUNCTION 5(text,text) brin_bloom_options(internal),
FUNCTION 11(text,text) hashtext(text),
STORAGE text;
CREATE OPERATOR FAMILY cdbhash_date_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_date_ops
FOR TYPE date USING ushash FAMILY cdbhash_date_ops AS
OPERATOR 1 =(date,date),
FUNCTION 1(date,date) cdblegacyhash_date(date);
CREATE OPERATOR FAMILY uuid_ops USING usbtree;
CREATE OPERATOR CLASS uuid_ops
DEFAULT FOR TYPE uuid USING usbtree FAMILY uuid_ops AS
OPERATOR 1 <(uuid,uuid),
OPERATOR 2 <=(uuid,uuid),
OPERATOR 3 =(uuid,uuid),
OPERATOR 4 >=(uuid,uuid),
OPERATOR 5 >(uuid,uuid),
FUNCTION 1(uuid,uuid) uuid_cmp(uuid,uuid),
FUNCTION 2(uuid,uuid) uuid_sortsupport(internal),
FUNCTION 4(uuid,uuid) btequalimage(oid);
CREATE OPERATOR FAMILY uuid_ops USING usbitmap;
CREATE OPERATOR CLASS uuid_ops
DEFAULT FOR TYPE uuid USING usbitmap FAMILY uuid_ops AS
OPERATOR 1 <(uuid,uuid),
OPERATOR 2 <=(uuid,uuid),
OPERATOR 3 =(uuid,uuid),
OPERATOR 4 >=(uuid,uuid),
OPERATOR 5 >(uuid,uuid),
FUNCTION 1(uuid,uuid) uuid_cmp(uuid,uuid),
FUNCTION 2(uuid,uuid) uuid_sortsupport(internal),
FUNCTION 4(uuid,uuid) btequalimage(oid);
CREATE OPERATOR FAMILY timetz_bloom_ops USING usbrin;
CREATE OPERATOR CLASS timetz_bloom_ops
FOR TYPE timetz USING usbrin FAMILY timetz_bloom_ops AS
OPERATOR 1 =(timetz,timetz),
FUNCTION 1(timetz,timetz) brin_bloom_opcinfo(internal),
FUNCTION 2(timetz,timetz) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(timetz,timetz) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(timetz,timetz) brin_bloom_union(internal,internal,internal),
FUNCTION 5(timetz,timetz) brin_bloom_options(internal),
FUNCTION 11(timetz,timetz) timetz_hash(timetz),
STORAGE timetz;
CREATE OPERATOR FAMILY range_inclusion_ops USING usbrin;
CREATE OPERATOR CLASS range_inclusion_ops
DEFAULT FOR TYPE anyrange USING usbrin FAMILY range_inclusion_ops AS
OPERATOR 1 <<(anyrange,anyrange),
OPERATOR 2 &<(anyrange,anyrange),
OPERATOR 3 &&(anyrange,anyrange),
OPERATOR 4 &>(anyrange,anyrange),
OPERATOR 5 >>(anyrange,anyrange),
OPERATOR 7 @>(anyrange,anyrange),
OPERATOR 8 <@(anyrange,anyrange),
OPERATOR 17 -|-(anyrange,anyrange),
OPERATOR 18 =(anyrange,anyrange),
OPERATOR 20 <(anyrange,anyrange),
OPERATOR 21 <=(anyrange,anyrange),
OPERATOR 22 >(anyrange,anyrange),
OPERATOR 23 >=(anyrange,anyrange),
FUNCTION 1(anyrange,anyrange) brin_inclusion_opcinfo(internal),
FUNCTION 2(anyrange,anyrange) brin_inclusion_add_value(internal,internal,internal,internal),
FUNCTION 3(anyrange,anyrange) brin_inclusion_consistent(internal,internal,internal),
FUNCTION 4(anyrange,anyrange) brin_inclusion_union(internal,internal,internal),
FUNCTION 11(anyrange,anyrange) range_merge(anyrange,anyrange),
FUNCTION 13(anyrange,anyrange) range_contains(anyrange,anyrange),
FUNCTION 14(anyrange,anyrange) isempty(anyrange),
STORAGE anyrange;
CREATE OPERATOR FAMILY network_inclusion_ops USING usbrin;
CREATE OPERATOR CLASS inet_inclusion_ops
DEFAULT FOR TYPE inet USING usbrin FAMILY network_inclusion_ops AS
OPERATOR 3 &&(inet,inet),
OPERATOR 7 >>=(inet,inet),
OPERATOR 8 <<=(inet,inet),
OPERATOR 18 =(inet,inet),
OPERATOR 24 >>(inet,inet),
OPERATOR 26 <<(inet,inet),
FUNCTION 1(inet,inet) brin_inclusion_opcinfo(internal),
FUNCTION 2(inet,inet) brin_inclusion_add_value(internal,internal,internal,internal),
FUNCTION 3(inet,inet) brin_inclusion_consistent(internal,internal,internal),
FUNCTION 4(inet,inet) brin_inclusion_union(internal,internal,internal),
FUNCTION 11(inet,inet) inet_merge(inet,inet),
FUNCTION 12(inet,inet) inet_same_family(inet,inet),
FUNCTION 13(inet,inet) network_supeq(inet,inet),
STORAGE inet;
CREATE OPERATOR FAMILY cdbhash_interval_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_interval_ops
FOR TYPE interval USING ushash FAMILY cdbhash_interval_ops AS
OPERATOR 1 =(interval,interval),
FUNCTION 1(interval,interval) cdblegacyhash_interval(interval);
CREATE OPERATOR FAMILY macaddr_bloom_ops USING usbrin;
CREATE OPERATOR CLASS macaddr_bloom_ops
FOR TYPE macaddr USING usbrin FAMILY macaddr_bloom_ops AS
OPERATOR 1 =(macaddr,macaddr),
FUNCTION 1(macaddr,macaddr) brin_bloom_opcinfo(internal),
FUNCTION 2(macaddr,macaddr) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(macaddr,macaddr) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(macaddr,macaddr) brin_bloom_union(internal,internal,internal),
FUNCTION 5(macaddr,macaddr) brin_bloom_options(internal),
FUNCTION 11(macaddr,macaddr) hashmacaddr(macaddr),
STORAGE macaddr;
CREATE OPERATOR FAMILY time_ops USING ushash;
CREATE OPERATOR CLASS time_ops
DEFAULT FOR TYPE time USING ushash FAMILY time_ops AS
OPERATOR 1 =(time,time),
FUNCTION 1(time,time) time_hash(time),
FUNCTION 2(time,time) time_hash_extended(time,int8);
CREATE OPERATOR FAMILY cdbhash_bpchar_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_bpchar_ops
FOR TYPE bpchar USING ushash FAMILY cdbhash_bpchar_ops AS
OPERATOR 1 =(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) cdblegacyhash_bpchar(bpchar);
CREATE OPERATOR FAMILY interval_ops USING usbtree;
CREATE OPERATOR CLASS interval_ops
DEFAULT FOR TYPE interval USING usbtree FAMILY interval_ops AS
OPERATOR 1 <(interval,interval),
OPERATOR 2 <=(interval,interval),
OPERATOR 3 =(interval,interval),
OPERATOR 4 >=(interval,interval),
OPERATOR 5 >(interval,interval),
FUNCTION 1(interval,interval) interval_cmp(interval,interval),
FUNCTION 3(interval,interval) in_range(interval,interval,interval,bool,bool),
FUNCTION 4(interval,interval) btequalimage(oid);
CREATE OPERATOR FAMILY interval_ops USING usbitmap;
CREATE OPERATOR CLASS interval_ops
DEFAULT FOR TYPE interval USING usbitmap FAMILY interval_ops AS
OPERATOR 1 <(interval,interval),
OPERATOR 2 <=(interval,interval),
OPERATOR 3 =(interval,interval),
OPERATOR 4 >=(interval,interval),
OPERATOR 5 >(interval,interval),
FUNCTION 1(interval,interval) interval_cmp(interval,interval),
FUNCTION 3(interval,interval) in_range(interval,interval,interval,bool,bool),
FUNCTION 4(interval,interval) btequalimage(oid);
CREATE OPERATOR FAMILY bpchar_minmax_ops USING usbrin;
CREATE OPERATOR CLASS bpchar_minmax_ops
DEFAULT FOR TYPE bpchar USING usbrin FAMILY bpchar_minmax_ops AS
OPERATOR 1 <(bpchar,bpchar),
OPERATOR 2 <=(bpchar,bpchar),
OPERATOR 3 =(bpchar,bpchar),
OPERATOR 4 >=(bpchar,bpchar),
OPERATOR 5 >(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) brin_minmax_opcinfo(internal),
FUNCTION 2(bpchar,bpchar) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(bpchar,bpchar) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(bpchar,bpchar) brin_minmax_union(internal,internal,internal),
STORAGE bpchar;
CREATE OPERATOR FAMILY cdbhash_array_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_array_ops
FOR TYPE anyarray USING ushash FAMILY cdbhash_array_ops AS
OPERATOR 1 =(anyarray,anyarray),
FUNCTION 1(anyarray,anyarray) cdblegacyhash_array(anyarray);
CREATE OPERATOR FAMILY macaddr_minmax_ops USING usbrin;
CREATE OPERATOR CLASS macaddr_minmax_ops
DEFAULT FOR TYPE macaddr USING usbrin FAMILY macaddr_minmax_ops AS
OPERATOR 1 <(macaddr,macaddr),
OPERATOR 2 <=(macaddr,macaddr),
OPERATOR 3 =(macaddr,macaddr),
OPERATOR 4 >=(macaddr,macaddr),
OPERATOR 5 >(macaddr,macaddr),
FUNCTION 1(macaddr,macaddr) brin_minmax_opcinfo(internal),
FUNCTION 2(macaddr,macaddr) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(macaddr,macaddr) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(macaddr,macaddr) brin_minmax_union(internal,internal,internal),
STORAGE macaddr;
CREATE OPERATOR FAMILY cdbhash_enum_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_enum_ops
FOR TYPE anyenum USING ushash FAMILY cdbhash_enum_ops AS
OPERATOR 1 =(anyenum,anyenum),
FUNCTION 1(anyenum,anyenum) cdblegacyhash_anyenum(anyenum);
CREATE OPERATOR FAMILY tid_minmax_ops USING usbrin;
CREATE OPERATOR CLASS tid_minmax_ops
DEFAULT FOR TYPE tid USING usbrin FAMILY tid_minmax_ops AS
OPERATOR 1 <(tid,tid),
OPERATOR 2 <=(tid,tid),
OPERATOR 3 =(tid,tid),
OPERATOR 4 >=(tid,tid),
OPERATOR 5 >(tid,tid),
FUNCTION 1(tid,tid) brin_minmax_opcinfo(internal),
FUNCTION 2(tid,tid) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(tid,tid) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(tid,tid) brin_minmax_union(internal,internal,internal),
STORAGE tid;
CREATE OPERATOR FAMILY cdbhash_macaddr_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_macaddr_ops
FOR TYPE macaddr USING ushash FAMILY cdbhash_macaddr_ops AS
OPERATOR 1 =(macaddr,macaddr),
FUNCTION 1(macaddr,macaddr) cdblegacyhash_macaddr(macaddr);
CREATE OPERATOR FAMILY varbit_ops USING ushash;
CREATE OPERATOR CLASS varbit_ops
DEFAULT FOR TYPE varbit USING ushash FAMILY varbit_ops AS
OPERATOR 1 =(varbit,varbit),
FUNCTION 1(varbit,varbit) bithash(varbit);
CREATE OPERATOR FAMILY interval_ops USING ushash;
CREATE OPERATOR CLASS interval_ops
DEFAULT FOR TYPE interval USING ushash FAMILY interval_ops AS
OPERATOR 1 =(interval,interval),
FUNCTION 1(interval,interval) interval_hash(interval),
FUNCTION 2(interval,interval) interval_hash_extended(interval,int8);
CREATE OPERATOR FAMILY macaddr8_ops USING ushash;
CREATE OPERATOR CLASS macaddr8_ops
DEFAULT FOR TYPE macaddr8 USING ushash FAMILY macaddr8_ops AS
OPERATOR 1 =(macaddr8,macaddr8),
FUNCTION 1(macaddr8,macaddr8) hashmacaddr8(macaddr8),
FUNCTION 2(macaddr8,macaddr8) hashmacaddr8extended(macaddr8,int8);
CREATE OPERATOR FAMILY oid_bloom_ops USING usbrin;
CREATE OPERATOR CLASS oid_bloom_ops
FOR TYPE oid USING usbrin FAMILY oid_bloom_ops AS
OPERATOR 1 =(oid,oid),
FUNCTION 1(oid,oid) brin_bloom_opcinfo(internal),
FUNCTION 2(oid,oid) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(oid,oid) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(oid,oid) brin_bloom_union(internal,internal,internal),
FUNCTION 5(oid,oid) brin_bloom_options(internal),
FUNCTION 11(oid,oid) hashoid(oid),
STORAGE oid;
CREATE OPERATOR FAMILY pg_lsn_ops USING usbtree;
CREATE OPERATOR CLASS pg_lsn_ops
DEFAULT FOR TYPE pg_lsn USING usbtree FAMILY pg_lsn_ops AS
OPERATOR 1 <(pg_lsn,pg_lsn),
OPERATOR 2 <=(pg_lsn,pg_lsn),
OPERATOR 3 =(pg_lsn,pg_lsn),
OPERATOR 4 >=(pg_lsn,pg_lsn),
OPERATOR 5 >(pg_lsn,pg_lsn),
FUNCTION 1(pg_lsn,pg_lsn) pg_lsn_cmp(pg_lsn,pg_lsn),
FUNCTION 4(pg_lsn,pg_lsn) btequalimage(oid);
CREATE OPERATOR FAMILY pg_lsn_ops USING usbitmap;
CREATE OPERATOR CLASS pg_lsn_ops
DEFAULT FOR TYPE pg_lsn USING usbitmap FAMILY pg_lsn_ops AS
OPERATOR 1 <(pg_lsn,pg_lsn),
OPERATOR 2 <=(pg_lsn,pg_lsn),
OPERATOR 3 =(pg_lsn,pg_lsn),
OPERATOR 4 >=(pg_lsn,pg_lsn),
OPERATOR 5 >(pg_lsn,pg_lsn),
FUNCTION 1(pg_lsn,pg_lsn) pg_lsn_cmp(pg_lsn,pg_lsn),
FUNCTION 4(pg_lsn,pg_lsn) btequalimage(oid);
CREATE OPERATOR FAMILY box_ops USING usspgist;
CREATE OPERATOR CLASS box_ops
DEFAULT FOR TYPE box USING usspgist FAMILY box_ops AS
OPERATOR 1 <<(box,box),
OPERATOR 2 &<(box,box),
OPERATOR 3 &&(box,box),
OPERATOR 4 &>(box,box),
OPERATOR 5 >>(box,box),
OPERATOR 6 ~=(box,box),
OPERATOR 7 @>(box,box),
OPERATOR 8 <@(box,box),
OPERATOR 9 &<|(box,box),
OPERATOR 10 <<|(box,box),
OPERATOR 11 |>>(box,box),
OPERATOR 12 |&>(box,box),
FUNCTION 1(box,box) spg_box_quad_config(internal,internal),
FUNCTION 2(box,box) spg_box_quad_choose(internal,internal),
FUNCTION 3(box,box) spg_box_quad_picksplit(internal,internal),
FUNCTION 4(box,box) spg_box_quad_inner_consistent(internal,internal),
FUNCTION 5(box,box) spg_box_quad_leaf_consistent(internal,internal);
CREATE OPERATOR FAMILY numeric_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS numeric_minmax_multi_ops
FOR TYPE numeric USING usbrin FAMILY numeric_minmax_multi_ops AS
OPERATOR 1 <(numeric,numeric),
OPERATOR 2 <=(numeric,numeric),
OPERATOR 3 =(numeric,numeric),
OPERATOR 4 >=(numeric,numeric),
OPERATOR 5 >(numeric,numeric),
FUNCTION 1(numeric,numeric) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(numeric,numeric) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(numeric,numeric) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(numeric,numeric) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(numeric,numeric) brin_minmax_multi_options(internal),
FUNCTION 11(numeric,numeric) brin_minmax_multi_distance_numeric(internal,internal),
STORAGE numeric;
CREATE OPERATOR FAMILY char_ops USING ushash;
CREATE OPERATOR CLASS char_ops
DEFAULT FOR TYPE "char" USING ushash FAMILY char_ops AS
OPERATOR 1 =("char","char"),
FUNCTION 1("char","char") hashchar("char"),
FUNCTION 2("char","char") hashcharextended("char",int8);
CREATE OPERATOR FAMILY cdbhash_bool_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_bool_ops
FOR TYPE bool USING ushash FAMILY cdbhash_bool_ops AS
OPERATOR 1 =(bool,bool),
FUNCTION 1(bool,bool) cdblegacyhash_bool(bool);
CREATE OPERATOR FAMILY macaddr8_ops USING usbtree;
CREATE OPERATOR CLASS macaddr8_ops
DEFAULT FOR TYPE macaddr8 USING usbtree FAMILY macaddr8_ops AS
OPERATOR 1 <(macaddr8,macaddr8),
OPERATOR 2 <=(macaddr8,macaddr8),
OPERATOR 3 =(macaddr8,macaddr8),
OPERATOR 4 >=(macaddr8,macaddr8),
OPERATOR 5 >(macaddr8,macaddr8),
FUNCTION 1(macaddr8,macaddr8) macaddr8_cmp(macaddr8,macaddr8),
FUNCTION 4(macaddr8,macaddr8) btequalimage(oid);
CREATE OPERATOR FAMILY macaddr8_ops USING usbitmap;
CREATE OPERATOR CLASS macaddr8_ops
DEFAULT FOR TYPE macaddr8 USING usbitmap FAMILY macaddr8_ops AS
OPERATOR 1 <(macaddr8,macaddr8),
OPERATOR 2 <=(macaddr8,macaddr8),
OPERATOR 3 =(macaddr8,macaddr8),
OPERATOR 4 >=(macaddr8,macaddr8),
OPERATOR 5 >(macaddr8,macaddr8),
FUNCTION 1(macaddr8,macaddr8) macaddr8_cmp(macaddr8,macaddr8),
FUNCTION 4(macaddr8,macaddr8) btequalimage(oid);
CREATE OPERATOR FAMILY macaddr_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS macaddr_minmax_multi_ops
FOR TYPE macaddr USING usbrin FAMILY macaddr_minmax_multi_ops AS
OPERATOR 1 <(macaddr,macaddr),
OPERATOR 2 <=(macaddr,macaddr),
OPERATOR 3 =(macaddr,macaddr),
OPERATOR 4 >=(macaddr,macaddr),
OPERATOR 5 >(macaddr,macaddr),
FUNCTION 1(macaddr,macaddr) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(macaddr,macaddr) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(macaddr,macaddr) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(macaddr,macaddr) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(macaddr,macaddr) brin_minmax_multi_options(internal),
FUNCTION 11(macaddr,macaddr) brin_minmax_multi_distance_macaddr(internal,internal),
STORAGE macaddr;
CREATE OPERATOR FAMILY pg_lsn_bloom_ops USING usbrin;
CREATE OPERATOR CLASS pg_lsn_bloom_ops
FOR TYPE pg_lsn USING usbrin FAMILY pg_lsn_bloom_ops AS
OPERATOR 1 =(pg_lsn,pg_lsn),
FUNCTION 1(pg_lsn,pg_lsn) brin_bloom_opcinfo(internal),
FUNCTION 2(pg_lsn,pg_lsn) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(pg_lsn,pg_lsn) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(pg_lsn,pg_lsn) brin_bloom_union(internal,internal,internal),
FUNCTION 5(pg_lsn,pg_lsn) brin_bloom_options(internal),
FUNCTION 11(pg_lsn,pg_lsn) pg_lsn_hash(pg_lsn),
STORAGE pg_lsn;
CREATE OPERATOR FAMILY tsvector_ops USING usbtree;
CREATE OPERATOR CLASS tsvector_ops
DEFAULT FOR TYPE tsvector USING usbtree FAMILY tsvector_ops AS
OPERATOR 1 <(tsvector,tsvector),
OPERATOR 2 <=(tsvector,tsvector),
OPERATOR 3 =(tsvector,tsvector),
OPERATOR 4 >=(tsvector,tsvector),
OPERATOR 5 >(tsvector,tsvector),
FUNCTION 1(tsvector,tsvector) tsvector_cmp(tsvector,tsvector);
CREATE OPERATOR FAMILY tsvector_ops USING usbitmap;
CREATE OPERATOR CLASS tsvector_ops
DEFAULT FOR TYPE tsvector USING usbitmap FAMILY tsvector_ops AS
OPERATOR 1 <(tsvector,tsvector),
OPERATOR 2 <=(tsvector,tsvector),
OPERATOR 3 =(tsvector,tsvector),
OPERATOR 4 >=(tsvector,tsvector),
OPERATOR 5 >(tsvector,tsvector),
FUNCTION 1(tsvector,tsvector) tsvector_cmp(tsvector,tsvector);
CREATE OPERATOR FAMILY cdbhash_text_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_text_ops
FOR TYPE text USING ushash FAMILY cdbhash_text_ops AS
OPERATOR 1 =(text,text),
FUNCTION 1(text,text) cdblegacyhash_text(text);
CREATE OPERATOR FAMILY oid_minmax_ops USING usbrin;
CREATE OPERATOR CLASS oid_minmax_ops
DEFAULT FOR TYPE oid USING usbrin FAMILY oid_minmax_ops AS
OPERATOR 1 <(oid,oid),
OPERATOR 2 <=(oid,oid),
OPERATOR 3 =(oid,oid),
OPERATOR 4 >=(oid,oid),
OPERATOR 5 >(oid,oid),
FUNCTION 1(oid,oid) brin_minmax_opcinfo(internal),
FUNCTION 2(oid,oid) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(oid,oid) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(oid,oid) brin_minmax_union(internal,internal,internal),
STORAGE oid;
CREATE OPERATOR FAMILY tsvector_ops USING usgin;
CREATE OPERATOR CLASS tsvector_ops
DEFAULT FOR TYPE tsvector USING usgin FAMILY tsvector_ops AS
FUNCTION 1(tsvector,tsvector) gin_cmp_tslexeme(text,text),
FUNCTION 2(tsvector,tsvector) gin_extract_tsvector(tsvector,internal,internal),
FUNCTION 3(tsvector,tsvector) gin_extract_tsquery(tsvector,internal,int2,internal,internal,internal,internal),
FUNCTION 4(tsvector,tsvector) gin_tsquery_consistent(internal,int2,tsvector,int4,internal,internal,internal,internal),
FUNCTION 5(tsvector,tsvector) gin_cmp_prefix(text,text,int2,internal),
FUNCTION 6(tsvector,tsvector) gin_tsquery_triconsistent(internal,int2,tsvector,int4,internal,internal,internal),
STORAGE text;
CREATE OPERATOR FAMILY array_ops USING ushash;
CREATE OPERATOR CLASS array_ops
DEFAULT FOR TYPE anyarray USING ushash FAMILY array_ops AS
OPERATOR 1 =(anyarray,anyarray),
FUNCTION 1(anyarray,anyarray) hash_array(anyarray),
FUNCTION 2(anyarray,anyarray) hash_array_extended(anyarray,int8);
CREATE OPERATOR FAMILY complex_ops USING ushash;
CREATE OPERATOR CLASS complex_ops
DEFAULT FOR TYPE complex USING ushash FAMILY complex_ops AS
OPERATOR 1 =(complex,complex),
FUNCTION 1(complex,complex) hashcomplex(complex);
CREATE OPERATOR FAMILY tid_bloom_ops USING usbrin;
CREATE OPERATOR CLASS tid_bloom_ops
FOR TYPE tid USING usbrin FAMILY tid_bloom_ops AS
OPERATOR 1 =(tid,tid),
FUNCTION 1(tid,tid) brin_bloom_opcinfo(internal),
FUNCTION 2(tid,tid) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(tid,tid) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(tid,tid) brin_bloom_union(internal,internal,internal),
FUNCTION 5(tid,tid) brin_bloom_options(internal),
FUNCTION 11(tid,tid) hashtid(tid),
STORAGE tid;
CREATE OPERATOR FAMILY oid_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS oid_minmax_multi_ops
FOR TYPE oid USING usbrin FAMILY oid_minmax_multi_ops AS
OPERATOR 1 <(oid,oid),
OPERATOR 2 <=(oid,oid),
OPERATOR 3 =(oid,oid),
OPERATOR 4 >=(oid,oid),
OPERATOR 5 >(oid,oid),
FUNCTION 1(oid,oid) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(oid,oid) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(oid,oid) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(oid,oid) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(oid,oid) brin_minmax_multi_options(internal),
FUNCTION 11(oid,oid) brin_minmax_multi_distance_int4(internal,internal),
STORAGE oid;
CREATE OPERATOR FAMILY range_ops USING usspgist;
CREATE OPERATOR CLASS range_ops
DEFAULT FOR TYPE anyrange USING usspgist FAMILY range_ops AS
OPERATOR 1 <<(anyrange,anyrange),
OPERATOR 2 &<(anyrange,anyrange),
OPERATOR 3 &&(anyrange,anyrange),
OPERATOR 4 &>(anyrange,anyrange),
OPERATOR 5 >>(anyrange,anyrange),
OPERATOR 6 -|-(anyrange,anyrange),
OPERATOR 7 @>(anyrange,anyrange),
OPERATOR 8 <@(anyrange,anyrange),
OPERATOR 18 =(anyrange,anyrange),
FUNCTION 1(anyrange,anyrange) spg_range_quad_config(internal,internal),
FUNCTION 2(anyrange,anyrange) spg_range_quad_choose(internal,internal),
FUNCTION 3(anyrange,anyrange) spg_range_quad_picksplit(internal,internal),
FUNCTION 4(anyrange,anyrange) spg_range_quad_inner_consistent(internal,internal),
FUNCTION 5(anyrange,anyrange) spg_range_quad_leaf_consistent(internal,internal);
CREATE OPERATOR FAMILY timestamp_ops USING ushash;
CREATE OPERATOR CLASS timestamp_ops
DEFAULT FOR TYPE timestamp USING ushash FAMILY timestamp_ops AS
OPERATOR 1 =(timestamp,timestamp),
FUNCTION 1(timestamp,timestamp) timestamp_hash(timestamp),
FUNCTION 2(timestamp,timestamp) timestamp_hash_extended(timestamp,int8);
CREATE OPERATOR FAMILY cdbhash_complex_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_complex_ops
FOR TYPE complex USING ushash FAMILY cdbhash_complex_ops AS
OPERATOR 1 =(complex,complex),
FUNCTION 1(complex,complex) cdblegacyhash_complex(complex);
CREATE OPERATOR FAMILY uuid_bloom_ops USING usbrin;
CREATE OPERATOR CLASS uuid_bloom_ops
FOR TYPE uuid USING usbrin FAMILY uuid_bloom_ops AS
OPERATOR 1 =(uuid,uuid),
FUNCTION 1(uuid,uuid) brin_bloom_opcinfo(internal),
FUNCTION 2(uuid,uuid) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(uuid,uuid) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(uuid,uuid) brin_bloom_union(internal,internal,internal),
FUNCTION 5(uuid,uuid) brin_bloom_options(internal),
FUNCTION 11(uuid,uuid) uuid_hash(uuid),
STORAGE uuid;
CREATE OPERATOR FAMILY cdbhash_oidvector_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_oidvector_ops
FOR TYPE oidvector USING ushash FAMILY cdbhash_oidvector_ops AS
OPERATOR 1 =(oidvector,oidvector),
FUNCTION 1(oidvector,oidvector) cdblegacyhash_oidvector(oidvector);
CREATE OPERATOR FAMILY network_minmax_ops USING usbrin;
CREATE OPERATOR CLASS inet_minmax_ops
FOR TYPE inet USING usbrin FAMILY network_minmax_ops AS
OPERATOR 1 <(inet,inet),
OPERATOR 2 <=(inet,inet),
OPERATOR 3 =(inet,inet),
OPERATOR 4 >=(inet,inet),
OPERATOR 5 >(inet,inet),
FUNCTION 1(inet,inet) brin_minmax_opcinfo(internal),
FUNCTION 2(inet,inet) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(inet,inet) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(inet,inet) brin_minmax_union(internal,internal,internal),
STORAGE inet;
CREATE OPERATOR FAMILY network_ops USING usspgist;
CREATE OPERATOR CLASS inet_ops
DEFAULT FOR TYPE inet USING usspgist FAMILY network_ops AS
OPERATOR 3 &&(inet,inet),
OPERATOR 18 =(inet,inet),
OPERATOR 19 <>(inet,inet),
OPERATOR 20 <(inet,inet),
OPERATOR 21 <=(inet,inet),
OPERATOR 22 >(inet,inet),
OPERATOR 23 >=(inet,inet),
OPERATOR 24 <<(inet,inet),
OPERATOR 25 <<=(inet,inet),
OPERATOR 26 >>(inet,inet),
OPERATOR 27 >>=(inet,inet),
FUNCTION 1(inet,inet) inet_spg_config(internal,internal),
FUNCTION 2(inet,inet) inet_spg_choose(internal,internal),
FUNCTION 3(inet,inet) inet_spg_picksplit(internal,internal),
FUNCTION 4(inet,inet) inet_spg_inner_consistent(internal,internal),
FUNCTION 5(inet,inet) inet_spg_leaf_consistent(internal,internal);
CREATE OPERATOR FAMILY uuid_minmax_ops USING usbrin;
CREATE OPERATOR CLASS uuid_minmax_ops
DEFAULT FOR TYPE uuid USING usbrin FAMILY uuid_minmax_ops AS
OPERATOR 1 <(uuid,uuid),
OPERATOR 2 <=(uuid,uuid),
OPERATOR 3 =(uuid,uuid),
OPERATOR 4 >=(uuid,uuid),
OPERATOR 5 >(uuid,uuid),
FUNCTION 1(uuid,uuid) brin_minmax_opcinfo(internal),
FUNCTION 2(uuid,uuid) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(uuid,uuid) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(uuid,uuid) brin_minmax_union(internal,internal,internal),
STORAGE uuid;
CREATE OPERATOR FAMILY cdbhash_numeric_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_numeric_ops
FOR TYPE numeric USING ushash FAMILY cdbhash_numeric_ops AS
OPERATOR 1 =(numeric,numeric),
FUNCTION 1(numeric,numeric) cdblegacyhash_numeric(numeric);
CREATE OPERATOR FAMILY array_ops USING usgin;
CREATE OPERATOR CLASS array_ops
DEFAULT FOR TYPE anyarray USING usgin FAMILY array_ops AS
OPERATOR 1 &&(anyarray,anyarray),
OPERATOR 2 @>(anyarray,anyarray),
OPERATOR 3 <@(anyarray,anyarray),
OPERATOR 4 =(anyarray,anyarray),
FUNCTION 2(anyarray,anyarray) ginarrayextract(anyarray,internal,internal),
FUNCTION 3(anyarray,anyarray) ginqueryarrayextract(anyarray,internal,int2,internal,internal,internal,internal),
FUNCTION 4(anyarray,anyarray) ginarrayconsistent(internal,int2,anyarray,int4,internal,internal,internal,internal),
FUNCTION 6(anyarray,anyarray) ginarraytriconsistent(internal,int2,anyarray,int4,internal,internal,internal),
STORAGE anyelement;
CREATE OPERATOR FAMILY numeric_bloom_ops USING usbrin;
CREATE OPERATOR CLASS numeric_bloom_ops
FOR TYPE numeric USING usbrin FAMILY numeric_bloom_ops AS
OPERATOR 1 =(numeric,numeric),
FUNCTION 1(numeric,numeric) brin_bloom_opcinfo(internal),
FUNCTION 2(numeric,numeric) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(numeric,numeric) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(numeric,numeric) brin_bloom_union(internal,internal,internal),
FUNCTION 5(numeric,numeric) brin_bloom_options(internal),
FUNCTION 11(numeric,numeric) hash_numeric(numeric),
STORAGE numeric;
CREATE OPERATOR FAMILY complex_ops USING usbtree;
CREATE OPERATOR CLASS complex_ops
DEFAULT FOR TYPE complex USING usbtree FAMILY complex_ops AS
OPERATOR 1 <<(complex,complex),
OPERATOR 2 <<=(complex,complex),
OPERATOR 3 =(complex,complex),
OPERATOR 4 >>=(complex,complex),
OPERATOR 5 >>(complex,complex),
FUNCTION 1(complex,complex) complex_cmp(complex,complex);
CREATE OPERATOR FAMILY complex_ops USING usbitmap;
CREATE OPERATOR CLASS complex_ops
DEFAULT FOR TYPE complex USING usbitmap FAMILY complex_ops AS
OPERATOR 1 <<(complex,complex),
OPERATOR 2 <<=(complex,complex),
OPERATOR 3 =(complex,complex),
OPERATOR 4 >>=(complex,complex),
OPERATOR 5 >>(complex,complex),
FUNCTION 1(complex,complex) complex_cmp(complex,complex);
CREATE OPERATOR FAMILY timetz_ops USING usbtree;
CREATE OPERATOR CLASS timetz_ops
DEFAULT FOR TYPE timetz USING usbtree FAMILY timetz_ops AS
OPERATOR 1 <(timetz,timetz),
OPERATOR 2 <=(timetz,timetz),
OPERATOR 3 =(timetz,timetz),
OPERATOR 4 >=(timetz,timetz),
OPERATOR 5 >(timetz,timetz),
FUNCTION 1(timetz,timetz) timetz_cmp(timetz,timetz),
FUNCTION 4(timetz,timetz) btequalimage(oid);
CREATE OPERATOR FAMILY timetz_ops USING usbitmap;
CREATE OPERATOR CLASS timetz_ops
DEFAULT FOR TYPE timetz USING usbitmap FAMILY timetz_ops AS
OPERATOR 1 <(timetz,timetz),
OPERATOR 2 <=(timetz,timetz),
OPERATOR 3 =(timetz,timetz),
OPERATOR 4 >=(timetz,timetz),
OPERATOR 5 >(timetz,timetz),
FUNCTION 1(timetz,timetz) timetz_cmp(timetz,timetz),
FUNCTION 4(timetz,timetz) btequalimage(oid);
CREATE OPERATOR FAMILY uuid_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS uuid_minmax_multi_ops
FOR TYPE uuid USING usbrin FAMILY uuid_minmax_multi_ops AS
OPERATOR 1 <(uuid,uuid),
OPERATOR 2 <=(uuid,uuid),
OPERATOR 3 =(uuid,uuid),
OPERATOR 4 >=(uuid,uuid),
OPERATOR 5 >(uuid,uuid),
FUNCTION 1(uuid,uuid) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(uuid,uuid) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(uuid,uuid) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(uuid,uuid) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(uuid,uuid) brin_minmax_multi_options(internal),
FUNCTION 11(uuid,uuid) brin_minmax_multi_distance_uuid(internal,internal),
STORAGE uuid;
CREATE OPERATOR FAMILY cdbhash_inet_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_inet_ops
FOR TYPE inet USING ushash FAMILY cdbhash_inet_ops AS
OPERATOR 1 =(inet,inet),
FUNCTION 1(inet,inet) cdblegacyhash_inet(inet);
CREATE OPERATOR FAMILY bool_ops USING usbtree;
CREATE OPERATOR CLASS bool_ops
DEFAULT FOR TYPE bool USING usbtree FAMILY bool_ops AS
OPERATOR 1 <(bool,bool),
OPERATOR 2 <=(bool,bool),
OPERATOR 3 =(bool,bool),
OPERATOR 4 >=(bool,bool),
OPERATOR 5 >(bool,bool),
FUNCTION 1(bool,bool) btboolcmp(bool,bool),
FUNCTION 4(bool,bool) btequalimage(oid);
CREATE OPERATOR FAMILY bool_ops USING usbitmap;
CREATE OPERATOR CLASS bool_ops
DEFAULT FOR TYPE bool USING usbitmap FAMILY bool_ops AS
OPERATOR 1 <(bool,bool),
OPERATOR 2 <=(bool,bool),
OPERATOR 3 =(bool,bool),
OPERATOR 4 >=(bool,bool),
OPERATOR 5 >(bool,bool),
FUNCTION 1(bool,bool) btboolcmp(bool,bool),
FUNCTION 4(bool,bool) btequalimage(oid);
CREATE OPERATOR FAMILY bpchar_pattern_ops USING ushash;
CREATE OPERATOR CLASS bpchar_pattern_ops
FOR TYPE bpchar USING ushash FAMILY bpchar_pattern_ops AS
OPERATOR 1 =(bpchar,bpchar),
FUNCTION 1(bpchar,bpchar) hashbpchar(bpchar),
FUNCTION 2(bpchar,bpchar) hashbpcharextended(bpchar,int8);
CREATE OPERATOR FAMILY interval_bloom_ops USING usbrin;
CREATE OPERATOR CLASS interval_bloom_ops
FOR TYPE interval USING usbrin FAMILY interval_bloom_ops AS
OPERATOR 1 =(interval,interval),
FUNCTION 1(interval,interval) brin_bloom_opcinfo(internal),
FUNCTION 2(interval,interval) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(interval,interval) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(interval,interval) brin_bloom_union(internal,internal,internal),
FUNCTION 5(interval,interval) brin_bloom_options(internal),
FUNCTION 11(interval,interval) interval_hash(interval),
STORAGE interval;
CREATE OPERATOR FAMILY range_ops USING ushash;
CREATE OPERATOR CLASS range_ops
DEFAULT FOR TYPE anyrange USING ushash FAMILY range_ops AS
OPERATOR 1 =(anyrange,anyrange),
FUNCTION 1(anyrange,anyrange) hash_range(anyrange),
FUNCTION 2(anyrange,anyrange) hash_range_extended(anyrange,int8);
CREATE OPERATOR FAMILY interval_minmax_ops USING usbrin;
CREATE OPERATOR CLASS interval_minmax_ops
DEFAULT FOR TYPE interval USING usbrin FAMILY interval_minmax_ops AS
OPERATOR 1 <(interval,interval),
OPERATOR 2 <=(interval,interval),
OPERATOR 3 =(interval,interval),
OPERATOR 4 >=(interval,interval),
OPERATOR 5 >(interval,interval),
FUNCTION 1(interval,interval) brin_minmax_opcinfo(internal),
FUNCTION 2(interval,interval) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(interval,interval) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(interval,interval) brin_minmax_union(internal,internal,internal),
STORAGE interval;
CREATE OPERATOR FAMILY macaddr8_minmax_ops USING usbrin;
CREATE OPERATOR CLASS macaddr8_minmax_ops
DEFAULT FOR TYPE macaddr8 USING usbrin FAMILY macaddr8_minmax_ops AS
OPERATOR 1 <(macaddr8,macaddr8),
OPERATOR 2 <=(macaddr8,macaddr8),
OPERATOR 3 =(macaddr8,macaddr8),
OPERATOR 4 >=(macaddr8,macaddr8),
OPERATOR 5 >(macaddr8,macaddr8),
FUNCTION 1(macaddr8,macaddr8) brin_minmax_opcinfo(internal),
FUNCTION 2(macaddr8,macaddr8) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(macaddr8,macaddr8) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(macaddr8,macaddr8) brin_minmax_union(internal,internal,internal),
STORAGE macaddr8;
CREATE OPERATOR FAMILY bit_minmax_ops USING usbrin;
CREATE OPERATOR CLASS bit_minmax_ops
DEFAULT FOR TYPE bit USING usbrin FAMILY bit_minmax_ops AS
OPERATOR 1 <(bit,bit),
OPERATOR 2 <=(bit,bit),
OPERATOR 3 =(bit,bit),
OPERATOR 4 >=(bit,bit),
OPERATOR 5 >(bit,bit),
FUNCTION 1(bit,bit) brin_minmax_opcinfo(internal),
FUNCTION 2(bit,bit) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(bit,bit) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(bit,bit) brin_minmax_union(internal,internal,internal),
STORAGE bit;
CREATE OPERATOR FAMILY macaddr8_bloom_ops USING usbrin;
CREATE OPERATOR CLASS macaddr8_bloom_ops
FOR TYPE macaddr8 USING usbrin FAMILY macaddr8_bloom_ops AS
OPERATOR 1 =(macaddr8,macaddr8),
FUNCTION 1(macaddr8,macaddr8) brin_bloom_opcinfo(internal),
FUNCTION 2(macaddr8,macaddr8) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(macaddr8,macaddr8) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(macaddr8,macaddr8) brin_bloom_union(internal,internal,internal),
FUNCTION 5(macaddr8,macaddr8) brin_bloom_options(internal),
FUNCTION 11(macaddr8,macaddr8) hashmacaddr8(macaddr8),
STORAGE macaddr8;
CREATE OPERATOR FAMILY oidvector_ops USING usbtree;
CREATE OPERATOR CLASS oidvector_ops
DEFAULT FOR TYPE oidvector USING usbtree FAMILY oidvector_ops AS
OPERATOR 1 <(oidvector,oidvector),
OPERATOR 2 <=(oidvector,oidvector),
OPERATOR 3 =(oidvector,oidvector),
OPERATOR 4 >=(oidvector,oidvector),
OPERATOR 5 >(oidvector,oidvector),
FUNCTION 1(oidvector,oidvector) btoidvectorcmp(oidvector,oidvector),
FUNCTION 4(oidvector,oidvector) btequalimage(oid);
CREATE OPERATOR FAMILY oidvector_ops USING usbitmap;
CREATE OPERATOR CLASS oidvector_ops
DEFAULT FOR TYPE oidvector USING usbitmap FAMILY oidvector_ops AS
OPERATOR 1 <(oidvector,oidvector),
OPERATOR 2 <=(oidvector,oidvector),
OPERATOR 3 =(oidvector,oidvector),
OPERATOR 4 >=(oidvector,oidvector),
OPERATOR 5 >(oidvector,oidvector),
FUNCTION 1(oidvector,oidvector) btoidvectorcmp(oidvector,oidvector),
FUNCTION 4(oidvector,oidvector) btequalimage(oid);
CREATE OPERATOR FAMILY network_minmax_multi_ops USING usbrin;
CREATE OPERATOR CLASS inet_minmax_multi_ops
FOR TYPE inet USING usbrin FAMILY network_minmax_multi_ops AS
OPERATOR 1 <(inet,inet),
OPERATOR 2 <=(inet,inet),
OPERATOR 3 =(inet,inet),
OPERATOR 4 >=(inet,inet),
OPERATOR 5 >(inet,inet),
FUNCTION 1(inet,inet) brin_minmax_multi_opcinfo(internal),
FUNCTION 2(inet,inet) brin_minmax_multi_add_value(internal,internal,internal,internal),
FUNCTION 3(inet,inet) brin_minmax_multi_consistent(internal,internal,internal,int4),
FUNCTION 4(inet,inet) brin_minmax_multi_union(internal,internal,internal),
FUNCTION 5(inet,inet) brin_minmax_multi_options(internal),
FUNCTION 11(inet,inet) brin_minmax_multi_distance_inet(internal,internal),
STORAGE inet;
CREATE OPERATOR FAMILY numeric_minmax_ops USING usbrin;
CREATE OPERATOR CLASS numeric_minmax_ops
DEFAULT FOR TYPE numeric USING usbrin FAMILY numeric_minmax_ops AS
OPERATOR 1 <(numeric,numeric),
OPERATOR 2 <=(numeric,numeric),
OPERATOR 3 =(numeric,numeric),
OPERATOR 4 >=(numeric,numeric),
OPERATOR 5 >(numeric,numeric),
FUNCTION 1(numeric,numeric) brin_minmax_opcinfo(internal),
FUNCTION 2(numeric,numeric) brin_minmax_add_value(internal,internal,internal,internal),
FUNCTION 3(numeric,numeric) brin_minmax_consistent(internal,internal,internal),
FUNCTION 4(numeric,numeric) brin_minmax_union(internal,internal,internal),
STORAGE numeric;
CREATE OPERATOR FAMILY cdbhash_name_ops USING ushash;
CREATE OPERATOR CLASS cdbhash_name_ops
FOR TYPE name USING ushash FAMILY cdbhash_name_ops AS
OPERATOR 1 =(name,name),
FUNCTION 1(name,name) cdblegacyhash_name(name);
CREATE OPERATOR FAMILY network_bloom_ops USING usbrin;
CREATE OPERATOR CLASS inet_bloom_ops
FOR TYPE inet USING usbrin FAMILY network_bloom_ops AS
OPERATOR 1 =(inet,inet),
FUNCTION 1(inet,inet) brin_bloom_opcinfo(internal),
FUNCTION 2(inet,inet) brin_bloom_add_value(internal,internal,internal,internal),
FUNCTION 3(inet,inet) brin_bloom_consistent(internal,internal,internal,int4),
FUNCTION 4(inet,inet) brin_bloom_union(internal,internal,internal),
FUNCTION 5(inet,inet) brin_bloom_options(internal),
FUNCTION 11(inet,inet) hashinet(inet),
STORAGE inet;
ALTER OPERATOR FAMILY float_ops USING ushash ADD
OPERATOR 1 =(float8,float4),
OPERATOR 1 =(float4,float8);
ALTER OPERATOR FAMILY quad_point_ops USING usspgist ADD
OPERATOR 8 <@(point,box);
ALTER OPERATOR FAMILY tsvector_ops USING usgist ADD
OPERATOR 1 @@(tsvector,tsquery);
ALTER OPERATOR FAMILY float_ops USING usbtree ADD
OPERATOR 1 <(float4,float8),
OPERATOR 2 <=(float4,float8),
OPERATOR 3 =(float4,float8),
OPERATOR 4 >=(float4,float8),
OPERATOR 5 >(float4,float8),
FUNCTION 1(float4,float8) btfloat48cmp(float4,float8),
FUNCTION 3(float4,float8) in_range(float4,float4,float8,bool,bool),
OPERATOR 1 <(float8,float4),
OPERATOR 2 <=(float8,float4),
OPERATOR 3 =(float8,float4),
OPERATOR 4 >=(float8,float4),
OPERATOR 5 >(float8,float4),
FUNCTION 1(float8,float4) btfloat84cmp(float8,float4);
ALTER OPERATOR FAMILY float_ops USING usbitmap ADD
OPERATOR 1 <(float4,float8),
OPERATOR 2 <=(float4,float8),
OPERATOR 3 =(float4,float8),
OPERATOR 4 >=(float4,float8),
OPERATOR 5 >(float4,float8),
FUNCTION 1(float4,float8) btfloat48cmp(float4,float8),
FUNCTION 3(float4,float8) in_range(float4,float4,float8,bool,bool),
OPERATOR 1 <(float8,float4),
OPERATOR 2 <=(float8,float4),
OPERATOR 3 =(float8,float4),
OPERATOR 4 >=(float8,float4),
OPERATOR 5 >(float8,float4),
FUNCTION 1(float8,float4) btfloat84cmp(float8,float4);
ALTER OPERATOR FAMILY box_inclusion_ops USING usbrin ADD
OPERATOR 7 @>(box,point);
ALTER OPERATOR FAMILY jsonb_ops USING usgin ADD
OPERATOR 9 ?(jsonb,text),
OPERATOR 10 ?|(jsonb,_text),
OPERATOR 11 ?&(jsonb,_text),
OPERATOR 15 @?(jsonb,jsonpath),
OPERATOR 16 @@(jsonb,jsonpath);
ALTER OPERATOR FAMILY float_minmax_multi_ops USING usbrin ADD
OPERATOR 1 <(float8,float4),
OPERATOR 2 <=(float8,float4),
OPERATOR 3 =(float8,float4),
OPERATOR 4 >=(float8,float4),
OPERATOR 5 >(float8,float4),
OPERATOR 1 <(float4,float8),
OPERATOR 2 <=(float4,float8),
OPERATOR 3 =(float4,float8),
OPERATOR 4 >=(float4,float8),
OPERATOR 5 >(float4,float8);
ALTER OPERATOR FAMILY time_ops USING usbtree ADD
FUNCTION 3(time,interval) in_range(time,time,interval,bool,bool);
ALTER OPERATOR FAMILY time_ops USING usbitmap ADD
FUNCTION 3(time,interval) in_range(time,time,interval,bool,bool);
ALTER OPERATOR FAMILY point_ops USING usgist ADD
OPERATOR 28 <@(point,box),
OPERATOR 48 <@(point,polygon),
OPERATOR 68 <@(point,circle);
ALTER OPERATOR FAMILY datetime_ops USING usbtree ADD
OPERATOR 1 <(timestamp,date),
OPERATOR 2 <=(timestamp,date),
OPERATOR 3 =(timestamp,date),
OPERATOR 4 >=(timestamp,date),
OPERATOR 5 >(timestamp,date),
OPERATOR 1 <(timestamp,timestamptz),
OPERATOR 2 <=(timestamp,timestamptz),
OPERATOR 3 =(timestamp,timestamptz),
OPERATOR 4 >=(timestamp,timestamptz),
OPERATOR 5 >(timestamp,timestamptz),
FUNCTION 1(timestamp,date) timestamp_cmp_date(timestamp,date),
FUNCTION 1(timestamp,timestamptz) timestamp_cmp_timestamptz(timestamp,timestamptz),
FUNCTION 3(timestamp,interval) in_range(timestamp,timestamp,interval,bool,bool),
OPERATOR 1 <(date,timestamp),
OPERATOR 2 <=(date,timestamp),
OPERATOR 3 =(date,timestamp),
OPERATOR 4 >=(date,timestamp),
OPERATOR 5 >(date,timestamp),
OPERATOR 1 <(date,timestamptz),
OPERATOR 2 <=(date,timestamptz),
OPERATOR 3 =(date,timestamptz),
OPERATOR 4 >=(date,timestamptz),
OPERATOR 5 >(date,timestamptz),
FUNCTION 1(date,timestamp) date_cmp_timestamp(date,timestamp),
FUNCTION 1(date,timestamptz) date_cmp_timestamptz(date,timestamptz),
FUNCTION 3(date,interval) in_range(date,date,interval,bool,bool),
OPERATOR 1 <(timestamptz,date),
OPERATOR 2 <=(timestamptz,date),
OPERATOR 3 =(timestamptz,date),
OPERATOR 4 >=(timestamptz,date),
OPERATOR 5 >(timestamptz,date),
OPERATOR 1 <(timestamptz,timestamp),
OPERATOR 2 <=(timestamptz,timestamp),
OPERATOR 3 =(timestamptz,timestamp),
OPERATOR 4 >=(timestamptz,timestamp),
OPERATOR 5 >(timestamptz,timestamp),
FUNCTION 1(timestamptz,date) timestamptz_cmp_date(timestamptz,date),
FUNCTION 1(timestamptz,timestamp) timestamptz_cmp_timestamp(timestamptz,timestamp),
FUNCTION 3(timestamptz,interval) in_range(timestamptz,timestamptz,interval,bool,bool);
ALTER OPERATOR FAMILY datetime_ops USING usbitmap ADD
OPERATOR 1 <(timestamp,date),
OPERATOR 2 <=(timestamp,date),
OPERATOR 3 =(timestamp,date),
OPERATOR 4 >=(timestamp,date),
OPERATOR 5 >(timestamp,date),
OPERATOR 1 <(timestamp,timestamptz),
OPERATOR 2 <=(timestamp,timestamptz),
OPERATOR 3 =(timestamp,timestamptz),
OPERATOR 4 >=(timestamp,timestamptz),
OPERATOR 5 >(timestamp,timestamptz),
FUNCTION 1(timestamp,date) timestamp_cmp_date(timestamp,date),
FUNCTION 1(timestamp,timestamptz) timestamp_cmp_timestamptz(timestamp,timestamptz),
FUNCTION 3(timestamp,interval) in_range(timestamp,timestamp,interval,bool,bool),
OPERATOR 1 <(date,timestamp),
OPERATOR 2 <=(date,timestamp),
OPERATOR 3 =(date,timestamp),
OPERATOR 4 >=(date,timestamp),
OPERATOR 5 >(date,timestamp),
OPERATOR 1 <(date,timestamptz),
OPERATOR 2 <=(date,timestamptz),
OPERATOR 3 =(date,timestamptz),
OPERATOR 4 >=(date,timestamptz),
OPERATOR 5 >(date,timestamptz),
FUNCTION 1(date,timestamp) date_cmp_timestamp(date,timestamp),
FUNCTION 1(date,timestamptz) date_cmp_timestamptz(date,timestamptz),
FUNCTION 3(date,interval) in_range(date,date,interval,bool,bool),
OPERATOR 1 <(timestamptz,date),
OPERATOR 2 <=(timestamptz,date),
OPERATOR 3 =(timestamptz,date),
OPERATOR 4 >=(timestamptz,date),
OPERATOR 5 >(timestamptz,date),
OPERATOR 1 <(timestamptz,timestamp),
OPERATOR 2 <=(timestamptz,timestamp),
OPERATOR 3 =(timestamptz,timestamp),
OPERATOR 4 >=(timestamptz,timestamp),
OPERATOR 5 >(timestamptz,timestamp),
FUNCTION 1(timestamptz,date) timestamptz_cmp_date(timestamptz,date),
FUNCTION 1(timestamptz,timestamp) timestamptz_cmp_timestamp(timestamptz,timestamp),
FUNCTION 3(timestamptz,interval) in_range(timestamptz,timestamptz,interval,bool,bool);
ALTER OPERATOR FAMILY circle_ops USING usgist ADD
OPERATOR 15 <->(circle,point) FOR ORDER BY float_ops;
ALTER OPERATOR FAMILY box_ops USING usgist ADD
OPERATOR 15 <->(box,point) FOR ORDER BY float_ops;
ALTER OPERATOR FAMILY poly_ops USING usspgist ADD
OPERATOR 15 <->(polygon,point) FOR ORDER BY float_ops;
ALTER OPERATOR FAMILY multirange_ops USING usgist ADD
OPERATOR 1 <<(anymultirange,anyrange),
OPERATOR 2 &<(anymultirange,anyrange),
OPERATOR 3 &&(anymultirange,anyrange),
OPERATOR 4 &>(anymultirange,anyrange),
OPERATOR 5 >>(anymultirange,anyrange),
OPERATOR 6 -|-(anymultirange,anyrange),
OPERATOR 7 @>(anymultirange,anyrange),
OPERATOR 8 <@(anymultirange,anyrange),
OPERATOR 16 @>(anymultirange,anyelement);
ALTER OPERATOR FAMILY text_ops USING usbtree ADD
OPERATOR 1 <(text,name),
OPERATOR 2 <=(text,name),
OPERATOR 3 =(text,name),
OPERATOR 4 >=(text,name),
OPERATOR 5 >(text,name),
FUNCTION 1(text,name) bttextnamecmp(text,name),
OPERATOR 1 <(name,text),
OPERATOR 2 <=(name,text),
OPERATOR 3 =(name,text),
OPERATOR 4 >=(name,text),
OPERATOR 5 >(name,text),
FUNCTION 1(name,text) btnametextcmp(name,text);
ALTER OPERATOR FAMILY text_ops USING usbitmap ADD
OPERATOR 1 <(text,name),
OPERATOR 2 <=(text,name),
OPERATOR 3 =(text,name),
OPERATOR 4 >=(text,name),
OPERATOR 5 >(text,name),
FUNCTION 1(text,name) bttextnamecmp(text,name),
OPERATOR 1 <(name,text),
OPERATOR 2 <=(name,text),
OPERATOR 3 =(name,text),
OPERATOR 4 >=(name,text),
OPERATOR 5 >(name,text),
FUNCTION 1(name,text) btnametextcmp(name,text);
ALTER OPERATOR FAMILY kd_point_ops USING usspgist ADD
OPERATOR 8 <@(point,box);
ALTER OPERATOR FAMILY cdbhash_integer_ops USING ushash ADD
OPERATOR 1 =(int4,int2),
OPERATOR 1 =(int4,int8),
OPERATOR 1 =(int8,int2),
OPERATOR 1 =(int8,int4),
OPERATOR 1 =(int2,int4),
OPERATOR 1 =(int2,int8);
ALTER OPERATOR FAMILY jsonb_path_ops USING usgin ADD
OPERATOR 15 @?(jsonb,jsonpath),
OPERATOR 16 @@(jsonb,jsonpath);
ALTER OPERATOR FAMILY poly_ops USING usgist ADD
OPERATOR 15 <->(polygon,point) FOR ORDER BY float_ops;
ALTER OPERATOR FAMILY float_minmax_ops USING usbrin ADD
OPERATOR 1 <(float8,float4),
OPERATOR 2 <=(float8,float4),
OPERATOR 3 =(float8,float4),
OPERATOR 4 >=(float8,float4),
OPERATOR 5 >(float8,float4),
OPERATOR 1 <(float4,float8),
OPERATOR 2 <=(float4,float8),
OPERATOR 3 =(float4,float8),
OPERATOR 4 >=(float4,float8),
OPERATOR 5 >(float4,float8);
ALTER OPERATOR FAMILY integer_minmax_ops USING usbrin ADD
OPERATOR 1 <(int8,int2),
OPERATOR 2 <=(int8,int2),
OPERATOR 3 =(int8,int2),
OPERATOR 4 >=(int8,int2),
OPERATOR 5 >(int8,int2),
OPERATOR 1 <(int8,int4),
OPERATOR 2 <=(int8,int4),
OPERATOR 3 =(int8,int4),
OPERATOR 4 >=(int8,int4),
OPERATOR 5 >(int8,int4),
OPERATOR 1 <(int4,int2),
OPERATOR 2 <=(int4,int2),
OPERATOR 3 =(int4,int2),
OPERATOR 4 >=(int4,int2),
OPERATOR 5 >(int4,int2),
OPERATOR 1 <(int4,int8),
OPERATOR 2 <=(int4,int8),
OPERATOR 3 =(int4,int8),
OPERATOR 4 >=(int4,int8),
OPERATOR 5 >(int4,int8),
OPERATOR 1 <(int2,int8),
OPERATOR 2 <=(int2,int8),
OPERATOR 3 =(int2,int8),
OPERATOR 4 >=(int2,int8),
OPERATOR 5 >(int2,int8),
OPERATOR 1 <(int2,int4),
OPERATOR 2 <=(int2,int4),
OPERATOR 3 =(int2,int4),
OPERATOR 4 >=(int2,int4),
OPERATOR 5 >(int2,int4);
ALTER OPERATOR FAMILY text_ops USING ushash ADD
OPERATOR 1 =(text,name),
OPERATOR 1 =(name,text);
ALTER OPERATOR FAMILY datetime_minmax_ops USING usbrin ADD
OPERATOR 1 <(timestamp,date),
OPERATOR 2 <=(timestamp,date),
OPERATOR 3 =(timestamp,date),
OPERATOR 4 >=(timestamp,date),
OPERATOR 5 >(timestamp,date),
OPERATOR 1 <(timestamp,timestamptz),
OPERATOR 2 <=(timestamp,timestamptz),
OPERATOR 3 =(timestamp,timestamptz),
OPERATOR 4 >=(timestamp,timestamptz),
OPERATOR 5 >(timestamp,timestamptz),
OPERATOR 1 <(timestamptz,date),
OPERATOR 2 <=(timestamptz,date),
OPERATOR 3 =(timestamptz,date),
OPERATOR 4 >=(timestamptz,date),
OPERATOR 5 >(timestamptz,date),
OPERATOR 1 <(timestamptz,timestamp),
OPERATOR 2 <=(timestamptz,timestamp),
OPERATOR 3 =(timestamptz,timestamp),
OPERATOR 4 >=(timestamptz,timestamp),
OPERATOR 5 >(timestamptz,timestamp),
OPERATOR 1 <(date,timestamp),
OPERATOR 2 <=(date,timestamp),
OPERATOR 3 =(date,timestamp),
OPERATOR 4 >=(date,timestamp),
OPERATOR 5 >(date,timestamp),
OPERATOR 1 <(date,timestamptz),
OPERATOR 2 <=(date,timestamptz),
OPERATOR 3 =(date,timestamptz),
OPERATOR 4 >=(date,timestamptz),
OPERATOR 5 >(date,timestamptz);
ALTER OPERATOR FAMILY datetime_minmax_multi_ops USING usbrin ADD
OPERATOR 1 <(timestamptz,date),
OPERATOR 2 <=(timestamptz,date),
OPERATOR 3 =(timestamptz,date),
OPERATOR 4 >=(timestamptz,date),
OPERATOR 5 >(timestamptz,date),
OPERATOR 1 <(timestamptz,timestamp),
OPERATOR 2 <=(timestamptz,timestamp),
OPERATOR 3 =(timestamptz,timestamp),
OPERATOR 4 >=(timestamptz,timestamp),
OPERATOR 5 >(timestamptz,timestamp),
OPERATOR 1 <(date,timestamp),
OPERATOR 2 <=(date,timestamp),
OPERATOR 3 =(date,timestamp),
OPERATOR 4 >=(date,timestamp),
OPERATOR 5 >(date,timestamp),
OPERATOR 1 <(date,timestamptz),
OPERATOR 2 <=(date,timestamptz),
OPERATOR 3 =(date,timestamptz),
OPERATOR 4 >=(date,timestamptz),
OPERATOR 5 >(date,timestamptz),
OPERATOR 1 <(timestamp,date),
OPERATOR 2 <=(timestamp,date),
OPERATOR 3 =(timestamp,date),
OPERATOR 4 >=(timestamp,date),
OPERATOR 5 >(timestamp,date),
OPERATOR 1 <(timestamp,timestamptz),
OPERATOR 2 <=(timestamp,timestamptz),
OPERATOR 3 =(timestamp,timestamptz),
OPERATOR 4 >=(timestamp,timestamptz),
OPERATOR 5 >(timestamp,timestamptz);
ALTER OPERATOR FAMILY integer_ops USING usbtree ADD
OPERATOR 1 <(int8,int2),
OPERATOR 2 <=(int8,int2),
OPERATOR 3 =(int8,int2),
OPERATOR 4 >=(int8,int2),
OPERATOR 5 >(int8,int2),
OPERATOR 1 <(int8,int4),
OPERATOR 2 <=(int8,int4),
OPERATOR 3 =(int8,int4),
OPERATOR 4 >=(int8,int4),
OPERATOR 5 >(int8,int4),
FUNCTION 1(int8,int4) btint84cmp(int8,int4),
FUNCTION 1(int8,int2) btint82cmp(int8,int2),
OPERATOR 1 <(int2,int4),
OPERATOR 2 <=(int2,int4),
OPERATOR 3 =(int2,int4),
OPERATOR 4 >=(int2,int4),
OPERATOR 5 >(int2,int4),
OPERATOR 1 <(int2,int8),
OPERATOR 2 <=(int2,int8),
OPERATOR 3 =(int2,int8),
OPERATOR 4 >=(int2,int8),
OPERATOR 5 >(int2,int8),
FUNCTION 1(int2,int4) btint24cmp(int2,int4),
FUNCTION 1(int2,int8) btint28cmp(int2,int8),
FUNCTION 3(int2,int8) in_range(int2,int2,int8,bool,bool),
FUNCTION 3(int2,int4) in_range(int2,int2,int4,bool,bool),
OPERATOR 1 <(int4,int2),
OPERATOR 2 <=(int4,int2),
OPERATOR 3 =(int4,int2),
OPERATOR 4 >=(int4,int2),
OPERATOR 5 >(int4,int2),
OPERATOR 1 <(int4,int8),
OPERATOR 2 <=(int4,int8),
OPERATOR 3 =(int4,int8),
OPERATOR 4 >=(int4,int8),
OPERATOR 5 >(int4,int8),
FUNCTION 1(int4,int8) btint48cmp(int4,int8),
FUNCTION 1(int4,int2) btint42cmp(int4,int2),
FUNCTION 3(int4,int8) in_range(int4,int4,int8,bool,bool),
FUNCTION 3(int4,int2) in_range(int4,int4,int2,bool,bool);
ALTER OPERATOR FAMILY integer_ops USING usbitmap ADD
OPERATOR 1 <(int8,int2),
OPERATOR 2 <=(int8,int2),
OPERATOR 3 =(int8,int2),
OPERATOR 4 >=(int8,int2),
OPERATOR 5 >(int8,int2),
OPERATOR 1 <(int8,int4),
OPERATOR 2 <=(int8,int4),
OPERATOR 3 =(int8,int4),
OPERATOR 4 >=(int8,int4),
OPERATOR 5 >(int8,int4),
FUNCTION 1(int8,int4) btint84cmp(int8,int4),
FUNCTION 1(int8,int2) btint82cmp(int8,int2),
OPERATOR 1 <(int2,int4),
OPERATOR 2 <=(int2,int4),
OPERATOR 3 =(int2,int4),
OPERATOR 4 >=(int2,int4),
OPERATOR 5 >(int2,int4),
OPERATOR 1 <(int2,int8),
OPERATOR 2 <=(int2,int8),
OPERATOR 3 =(int2,int8),
OPERATOR 4 >=(int2,int8),
OPERATOR 5 >(int2,int8),
FUNCTION 1(int2,int4) btint24cmp(int2,int4),
FUNCTION 1(int2,int8) btint28cmp(int2,int8),
FUNCTION 3(int2,int8) in_range(int2,int2,int8,bool,bool),
FUNCTION 3(int2,int4) in_range(int2,int2,int4,bool,bool),
OPERATOR 1 <(int4,int2),
OPERATOR 2 <=(int4,int2),
OPERATOR 3 =(int4,int2),
OPERATOR 4 >=(int4,int2),
OPERATOR 5 >(int4,int2),
OPERATOR 1 <(int4,int8),
OPERATOR 2 <=(int4,int8),
OPERATOR 3 =(int4,int8),
OPERATOR 4 >=(int4,int8),
OPERATOR 5 >(int4,int8),
FUNCTION 1(int4,int8) btint48cmp(int4,int8),
FUNCTION 1(int4,int2) btint42cmp(int4,int2),
FUNCTION 3(int4,int8) in_range(int4,int4,int8,bool,bool),
FUNCTION 3(int4,int2) in_range(int4,int4,int2,bool,bool);
ALTER OPERATOR FAMILY integer_minmax_multi_ops USING usbrin ADD
OPERATOR 1 <(int4,int2),
OPERATOR 2 <=(int4,int2),
OPERATOR 3 =(int4,int2),
OPERATOR 4 >=(int4,int2),
OPERATOR 5 >(int4,int2),
OPERATOR 1 <(int4,int8),
OPERATOR 2 <=(int4,int8),
OPERATOR 3 =(int4,int8),
OPERATOR 4 >=(int4,int8),
OPERATOR 5 >(int4,int8),
OPERATOR 1 <(int2,int8),
OPERATOR 2 <=(int2,int8),
OPERATOR 3 =(int2,int8),
OPERATOR 4 >=(int2,int8),
OPERATOR 5 >(int2,int8),
OPERATOR 1 <(int2,int4),
OPERATOR 2 <=(int2,int4),
OPERATOR 3 =(int2,int4),
OPERATOR 4 >=(int2,int4),
OPERATOR 5 >(int2,int4),
OPERATOR 1 <(int8,int2),
OPERATOR 2 <=(int8,int2),
OPERATOR 3 =(int8,int2),
OPERATOR 4 >=(int8,int2),
OPERATOR 5 >(int8,int2),
OPERATOR 1 <(int8,int4),
OPERATOR 2 <=(int8,int4),
OPERATOR 3 =(int8,int4),
OPERATOR 4 >=(int8,int4),
OPERATOR 5 >(int8,int4);
ALTER OPERATOR FAMILY range_ops USING usgist ADD
OPERATOR 1 <<(anyrange,anymultirange),
OPERATOR 2 &<(anyrange,anymultirange),
OPERATOR 3 &&(anyrange,anymultirange),
OPERATOR 4 &>(anyrange,anymultirange),
OPERATOR 5 >>(anyrange,anymultirange),
OPERATOR 6 -|-(anyrange,anymultirange),
OPERATOR 7 @>(anyrange,anymultirange),
OPERATOR 8 <@(anyrange,anymultirange),
OPERATOR 16 @>(anyrange,anyelement);
ALTER OPERATOR FAMILY integer_ops USING ushash ADD
OPERATOR 1 =(int8,int2),
OPERATOR 1 =(int8,int4),
OPERATOR 1 =(int4,int2),
OPERATOR 1 =(int4,int8),
OPERATOR 1 =(int2,int4),
OPERATOR 1 =(int2,int8);
ALTER OPERATOR FAMILY range_inclusion_ops USING usbrin ADD
OPERATOR 16 @>(anyrange,anyelement);
ALTER OPERATOR FAMILY box_ops USING usspgist ADD
OPERATOR 15 <->(box,point) FOR ORDER BY float_ops;
ALTER OPERATOR FAMILY tsvector_ops USING usgin ADD
OPERATOR 1 @@(tsvector,tsquery),
OPERATOR 2 @@@(tsvector,tsquery);
ALTER OPERATOR FAMILY range_ops USING usspgist ADD
OPERATOR 16 @>(anyrange,anyelement);
ALTER OPERATOR FAMILY timetz_ops USING usbtree ADD
FUNCTION 3(timetz,interval) in_range(timetz,timetz,interval,bool,bool);
ALTER OPERATOR FAMILY timetz_ops USING usbitmap ADD
FUNCTION 3(timetz,interval) in_range(timetz,timetz,interval,bool,bool);
CREATE OPERATOR CLASS inet_ops
DEFAULT FOR TYPE inet USING usbtree FAMILY network_ops AS
STORAGE inet;
CREATE OPERATOR CLASS varchar_pattern_ops
FOR TYPE text USING usbtree FAMILY text_pattern_ops AS
STORAGE text;
CREATE OPERATOR CLASS varchar_ops
FOR TYPE text USING usbtree FAMILY text_ops AS
STORAGE text;
CREATE OPERATOR CLASS inet_ops
DEFAULT FOR TYPE inet USING usbitmap FAMILY network_ops AS
STORAGE inet;
CREATE OPERATOR CLASS varchar_pattern_ops
FOR TYPE text USING usbitmap FAMILY text_pattern_ops AS
STORAGE text;
CREATE OPERATOR CLASS varchar_ops
FOR TYPE text USING usbitmap FAMILY text_ops AS
STORAGE text;
CREATE OPERATOR CLASS text_pattern_ops
FOR TYPE text USING ushash FAMILY text_pattern_ops AS
STORAGE text;
CREATE OPERATOR CLASS cidr_ops
FOR TYPE inet USING ushash FAMILY network_ops AS
STORAGE inet;
CREATE OPERATOR CLASS text_ops
FOR TYPE text USING ushash FAMILY text_ops AS
STORAGE text;
set allow_system_table_mods=true;
update pg_opclass set opckeytype=2275 where opcname='name_ops' and opcmethod=(select oid from pg_am where amname='usbtree');