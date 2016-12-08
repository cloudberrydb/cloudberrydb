-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test11: NULL values to text
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(DISTINCT(c)) FROM dml_union_r; 
UPDATE dml_union_r SET c = (SELECT NULL EXCEPT ALL SELECT NULL);
SELECT COUNT(DISTINCT(c)) FROM dml_union_r; 

