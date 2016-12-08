-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test3: Update distribution column with INTERSECT
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r WHERE a = 1;
SELECT COUNT(*) FROM (SELECT * FROM (SELECT a FROM dml_union_r order by a limit 1) foo INTERSECT SELECT a FROM dml_union_s)bar;
UPDATE dml_union_r SET a = ( SELECT * FROM (SELECT a FROM dml_union_r order by a limit 1) foo INTERSECT SELECT a FROM dml_union_s);
SELECT COUNT(*) FROM dml_union_r WHERE a = 1;
