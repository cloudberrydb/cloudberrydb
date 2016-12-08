-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test1: Update distribution column with UNION
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(DISTINCT(a)) FROM dml_union_r;
SELECT a FROM dml_union_r UNION SELECT a FROM dml_union_s ORDER BY 1 LIMIT 1;
UPDATE dml_union_r SET a = (SELECT a FROM dml_union_r UNION SELECT a FROM dml_union_s ORDER BY 1 LIMIT 1);
SELECT COUNT(DISTINCT(a)) FROM dml_union_r;
