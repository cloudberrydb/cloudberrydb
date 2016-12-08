-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test12: Update partition key to NULL values when default partition present
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(DISTINCT(d)) FROM dml_union_s; 
UPDATE dml_union_s SET d = (SELECT NULL UNION SELECT NULL)::numeric;
SELECT COUNT(DISTINCT(d)) FROM dml_union_s; 

