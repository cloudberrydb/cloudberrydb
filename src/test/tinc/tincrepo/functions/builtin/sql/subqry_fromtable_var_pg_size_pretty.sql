-- @description subqry_fromtable_var_pg_size_pretty.sql
-- @db_name builtin_functionproperty
-- @author tungs1
-- @modified 2013-04-17 12:00:00
-- @created 2013-04-17 12:00:00
-- @executemode ORCA_PLANNER_DIFF
-- @tags functionPropertiesBuiltin HAWQ
SELECT * FROM bar, (SELECT pg_size_pretty(b) from foo ORDER BY 1) r ORDER BY 1,2,3;
