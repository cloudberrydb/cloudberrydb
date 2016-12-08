-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_delete_test17:  Using Partition table 
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_s;
DELETE FROM dml_union_s USING (SELECT * FROM dml_union_r UNION SELECT * FROM dml_union_s_1_prt_def) foo WHERE foo.d = dml_union_s.d;
SELECT COUNT(*) FROM dml_union_s;

