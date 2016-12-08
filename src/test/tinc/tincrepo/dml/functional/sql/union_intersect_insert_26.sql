-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_test26: UNION with AGGREGATE
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
SELECT COUNT(*) FROM (SELECT * FROM (SELECT AVG(a) as a FROM dml_union_r UNION SELECT AVG(b) as a FROM dml_union_s) foo)bar;
INSERT INTO dml_union_r SELECT * FROM (SELECT AVG(a) as a FROM dml_union_r UNION SELECT AVG(b) as a FROM dml_union_s) foo;
SELECT COUNT(*) FROM dml_union_r;

