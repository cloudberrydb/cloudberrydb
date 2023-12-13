/* contrib/dfs_tablespace/dfs_tablespace--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dfs_tablespace" to load this file. \quit

CREATE FUNCTION ufsdemo_am_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access method
CREATE ACCESS METHOD ufsdemo_am TYPE TABLE HANDLER ufsdemo_am_handler;
COMMENT ON ACCESS METHOD ufsdemo_am IS 'ufsdemo am';
