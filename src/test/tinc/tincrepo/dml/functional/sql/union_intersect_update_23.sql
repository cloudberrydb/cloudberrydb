-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test23: Update distribution column to constant value with UNION/INTERSECT/EXCEPT within dml_union_sub-query
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
SELECT COUNT(DISTINCT(a)) FROM dml_union_r;
SELECT COUNT(*) FROM dml_union_r WHERE a = 0;
UPDATE dml_union_r SET a = 0 WHERE a in (SELECT a FROM dml_union_r UNION ALL SELECT a FROM dml_union_s);
SELECT COUNT(*) FROM dml_union_r WHERE a = 0;
SELECT COUNT(DISTINCT(a)) FROM dml_union_r;
