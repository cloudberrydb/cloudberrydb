-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_delete_test10:  With UNION/INTERSECT/EXCEPT in the predicate condition
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
DELETE FROM dml_union_r WHERE a = (SELECT a FROM dml_union_r UNION SELECT a FROM dml_union_s ORDER BY a LIMIT 1);
SELECT COUNT(*) FROM dml_union_r;

