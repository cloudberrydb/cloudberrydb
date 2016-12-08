-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @gpopt 1.532
-- @description union_test27: Negative tests VIOLATES NULL VALUE CONSTRAINT 
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
SELECT COUNT(*) FROM ( SELECT * FROM (SELECT * FROM dml_union_r EXCEPT SELECT * FROM dml_union_s ) foo WHERE c='text')bar;
INSERT INTO dml_union_s SELECT * FROM (SELECT * FROM dml_union_r EXCEPT SELECT * FROM dml_union_s) foo WHERE c='text';
SELECT COUNT(*) FROM dml_union_r;

