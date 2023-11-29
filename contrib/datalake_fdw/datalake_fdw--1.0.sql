/* contrib/datalake_fdw/datalake_fdw--1.0.sql */

\echo Use "CREATE EXTENSION datalake_fdw" to load this file. \quit

CREATE FUNCTION datalake_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION datalake_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION gp_toolkit.__gopher_cache_free_relation_name(text)
RETURNS bool
AS '$libdir/datalake_fdw.so' , 'hdw_gopher_cache_free_relation_name_wrapper'
LANGUAGE C STRICT;
GRANT EXECUTE ON FUNCTION gp_toolkit.__gopher_cache_free_relation_name(text) TO public;


CREATE FUNCTION gp_toolkit.__gopher_free_all_cache()
RETURNS bool
AS '$libdir/datalake_fdw.so' , 'hdw_gopher_free_all_cache_wrapper'
LANGUAGE C STRICT;
GRANT EXECUTE ON FUNCTION gp_toolkit.__gopher_free_all_cache() TO public;

CREATE FUNCTION datalake_acquire_sample_rows(oid, int, boolean, text)
RETURNS setof record
AS 'MODULE_PATHNAME','datalake_acquire_sample_rows'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;
