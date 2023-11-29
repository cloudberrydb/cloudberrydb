/* contrib/hive_connector/hive_connector--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hive_connector" to load this file. \quit

SET search_path = public;

CREATE OR REPLACE FUNCTION sync_hive_table(hiveClusterName text,
hiveDatabaseName text,
hiveTableName text,
hdfsClusterName text,
destTableName text,
serverName text) RETURNS boolean
AS '$libdir/hive_connector','sync_hive_table'
LANGUAGE C STRICT EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION sync_hive_database(hiveClusterName text,
hiveDatabaseName text,
hdfsClusterName text,
destSchemaName text,
serverName text) RETURNS boolean
AS '$libdir/hive_connector','sync_hive_database'
LANGUAGE C STRICT EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION sync_hive_table(hiveClusterName text,
hiveDatabaseName text,
hiveTableName text,
hdfsClusterName text,
destTableName text,
serverName text,
forceSync boolean) RETURNS boolean
AS '$libdir/hive_connector','sync_hive_table'
LANGUAGE C STRICT EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION sync_hive_database(hiveClusterName text,
hiveDatabaseName text,
hdfsClusterName text,
destSchemaName text,
serverName text,
forceSync boolean) RETURNS boolean
AS '$libdir/hive_connector','sync_hive_database'
LANGUAGE C STRICT EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION create_foreign_server(serverName text,
userMapName text,
dataWrapName text,
hdfsClusterName text) RETURNS boolean
AS '$libdir/hive_connector','create_foreign_server'
LANGUAGE C STRICT EXECUTE ON MASTER;
