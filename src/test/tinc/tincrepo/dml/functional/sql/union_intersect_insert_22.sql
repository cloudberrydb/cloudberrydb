-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_test22: UNION with limit
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
SELECT COUNT(*) FROM (SELECT * FROM dml_union_r UNION ALL SELECT * FROM dml_union_s ORDER BY 1,2,3,4) foo LIMIT 10;
SELECT COUNT(*) FROM dml_union_r;
INSERT INTO dml_union_r SELECT * FROM (SELECT * FROM dml_union_r UNION ALL SELECT * FROM dml_union_s ORDER BY 1,2,3,4) foo LIMIT 10;
SELECT COUNT(*) FROM dml_union_r;

