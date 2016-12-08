-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test19: Update partition key to constant value with UNION/INTERSECT/EXCEPT within dml_union_sub-query
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r WHERE d = 20000; 
SELECT COUNT(*) FROM (SELECT a FROM dml_union_r EXCEPT ALL SELECT a FROM dml_union_s)foo;
UPDATE dml_union_r SET d = 20000 WHERE a in (SELECT a FROM dml_union_r EXCEPT ALL SELECT a FROM dml_union_s);
SELECT COUNT(*) FROM dml_union_r WHERE d = 20000; 
