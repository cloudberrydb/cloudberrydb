-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_test24: UNION with exists 
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
SELECT COUNT(*) FROM (SELECT AVG(a),10,'avg',10 FROM dml_union_r WHERE exists (SELECT a FROM dml_union_r UNION ALL SELECT b FROM dml_union_s))foo;
INSERT INTO dml_union_r SELECT AVG(a),10,'avg',10 FROM dml_union_r WHERE exists (SELECT a FROM dml_union_r UNION ALL SELECT b FROM dml_union_s);
SELECT COUNT(*) FROM dml_union_r;

