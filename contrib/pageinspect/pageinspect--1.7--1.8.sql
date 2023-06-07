/* contrib/pageinspect/pageinspect--1.7--1.8.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pageinspect UPDATE TO '1.8'" to load this file. \quit

--
-- heap_tuple_infomask_flags()
--
CREATE FUNCTION heap_tuple_infomask_flags(
       t_infomask integer,
       t_infomask2 integer,
       raw_flags OUT text[],
       combined_flags OUT text[])
RETURNS record
AS 'MODULE_PATHNAME', 'heap_tuple_infomask_flags'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_metap()
--
DROP FUNCTION bt_metap(text);
CREATE FUNCTION bt_metap(IN relname text,
    OUT magic int4,
    OUT version int4,
    OUT root int8,
    OUT level int8,
    OUT fastroot int8,
    OUT fastlevel int8,
    OUT oldest_xact xid,
    OUT last_cleanup_num_tuples float8,
    OUT allequalimage boolean)
AS 'MODULE_PATHNAME', 'bt_metap'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_page_items(text, int4)
--
DROP FUNCTION bt_page_items(text, int4);
CREATE FUNCTION bt_page_items(IN relname text, IN blkno int4,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT nulls bool,
    OUT vars bool,
    OUT data text,
    OUT dead boolean,
    OUT htid tid,
    OUT tids tid[])
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_page_items'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bt_page_items(bytea)
--
DROP FUNCTION bt_page_items(bytea);
CREATE FUNCTION bt_page_items(IN page bytea,
    OUT itemoffset smallint,
    OUT ctid tid,
    OUT itemlen smallint,
    OUT nulls bool,
    OUT vars bool,
    OUT data text,
    OUT dead boolean,
    OUT htid tid,
    OUT tids tid[])
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bt_page_items_bytea'
LANGUAGE C STRICT PARALLEL SAFE;

-- bm_metap()
--
CREATE FUNCTION bm_metap(IN relname text,
     OUT magic int4,
     OUT version int4,
     OUT auxrelid oid,
     OUT auxindexrelid oid,
     OUT lovlastblknum bigint)
AS 'MODULE_PATHNAME', 'bm_metap'
LANGUAGE C STRICT PARALLEL SAFE;

--
-- bm_lov_page_items()
--
CREATE FUNCTION bm_lov_page_items(IN relname text,
                                  IN blkno int4,
                                  OUT itemoffset smallint,
                                  OUT lov_head_blkno bigint,
                                  OUT lov_tail_blkno bigint,
                                  OUT last_complete_word text,
                                  OUT last_word text,
                                  OUT last_tid numeric,
                                  OUT last_setbit_tid numeric,
                                  OUT is_last_complete_word_fill bool,
                                  OUT is_last_word_fill bool)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bm_lov_page_items'
    LANGUAGE C STRICT PARALLEL SAFE;

--
-- bm_bitmap_page_header()
--
CREATE FUNCTION bm_bitmap_page_header(IN relname text,
                         IN blkno int4,
                         OUT num_words bigint,
                         OUT next_blkno bigint,
                         OUT last_tid numeric)
AS 'MODULE_PATHNAME', 'bm_bitmap_page_header'
    LANGUAGE C STRICT PARALLEL SAFE;

--
-- bm_bitmap_page_items()
--
CREATE FUNCTION bm_bitmap_page_items(IN relname text,
                                  IN blkno int4,
                                  OUT word_num bigint,
                                  OUT compressed bool,
                                  OUT content_word text)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bm_bitmap_page_items'
    LANGUAGE C STRICT PARALLEL SAFE;

--
-- bm_bitmap_page_items_bytea()
--
CREATE FUNCTION bm_bitmap_page_items(IN page bytea,
                                  OUT word_num bigint,
                                  OUT compressed bool,
                                  OUT content_word text)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'bm_bitmap_page_items_bytea'
    LANGUAGE C STRICT PARALLEL SAFE;
