-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_delete_test6:  With UNION/INTERSECT/EXCEPT in dml_union_subquery
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
DELETE FROM dml_union_r WHERE a in (SELECT a FROM dml_union_r EXCEPT ALL SELECT a FROM dml_union_s);
SELECT COUNT(*) FROM dml_union_r;

