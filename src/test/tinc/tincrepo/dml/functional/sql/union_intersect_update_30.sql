-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @gpopt 1.532
-- @description union_update_test30: Negative Tests  more than one row returned by a sub-query used as an expression
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(DISTINCT(a)) FROM dml_union_r;
UPDATE dml_union_r SET a = ( SELECT a FROM dml_union_r UNION ALL SELECT a FROM dml_union_s);
SELECT COUNT(DISTINCT(a)) FROM dml_union_r;

