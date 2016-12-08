-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test5: Update distribution column with EXCEPT
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT SUM(a) FROM dml_union_r;
SELECT COUNT(*) FROM (SELECT * FROM (SELECT a FROM dml_union_r limit 1) foo EXCEPT SELECT a FROM dml_union_s)bar;
UPDATE dml_union_r SET a = ( SELECT * FROM (SELECT a FROM dml_union_r limit 1) foo EXCEPT SELECT a FROM dml_union_s);
SELECT SUM(a) FROM dml_union_r;
