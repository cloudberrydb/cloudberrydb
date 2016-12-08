-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_test23: UNION with dml_union_sub-query in SELECT
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
SELECT COUNT(*) FROM (SELECT NULL,(SELECT NULL f1 FROM dml_union_r UNION SELECT NULL f1 FROM dml_union_s)::int, 'nullval',NULL)foo;
INSERT INTO dml_union_r SELECT NULL,(SELECT NULL f1 FROM dml_union_r UNION SELECT NULL f1 FROM dml_union_s)::int, 'nullval',NULL;
SELECT COUNT(*) FROM dml_union_r;

