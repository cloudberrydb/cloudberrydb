-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_delete_test14:  With UNION/INTERSECT/EXCEPT 
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
DELETE FROM dml_union_r USING (SELECT a FROM dml_union_r INTERSECT SELECT a FROM dml_union_s) foo WHERE foo.a = dml_union_r.a;
SELECT COUNT(*) FROM dml_union_r;

