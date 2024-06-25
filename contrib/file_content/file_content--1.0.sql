/* contrib/file_content/file_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION file_content" to load this file. \quit

CREATE FUNCTION file_content(text)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION directory_table_without_content(IN c1 text,
OUT scoped_file_url text,
OUT relative_path text,
OUT tag text,
OUT size int8,
OUT last_modified timestamptz,
OUT md5 text,
OUT dirtable_oid oid)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
EXECUTE ON ALL SEGMENTS
LANGUAGE C STRICT;
