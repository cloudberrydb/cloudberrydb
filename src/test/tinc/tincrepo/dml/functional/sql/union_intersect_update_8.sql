-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test8: NULL values to distribution key
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

UPDATE dml_union_r SET a = (SELECT NULL INTERSECT SELECT NULL)::int;
SELECT DISTINCT(a) FROM dml_union_r;

