-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_test30: INSERT NON ATOMICS with union/intersect/except
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
SELECT COUNT(*) FROM (SELECT c,c+1,case when c = 1 then 'r' else 's' end,c+1 FROM (    SELECT COUNT(distinct c) c FROM (SELECT f1, f2 , COUNT(*) c FROM (SELECT 10 f1, 'r' f2 FROM dml_union_r UNION SELECT 40 f1, 's' f2 FROM dml_union_r UNION SELECT a, c FROM dml_union_r INTERSECT SELECT a, c FROM dml_union_s ORDER BY 1) foo group by f1,f2) foo)foo)bar;
INSERT INTO dml_union_r SELECT c,c+1,case when c = 1 then 'r' else 's' end,c+1 FROM (SELECT COUNT(distinct c) c FROM (SELECT f1, f2 , COUNT(*) c FROM (SELECT 10 f1, 'r' f2 FROM dml_union_r UNION SELECT 40 f1, 's' f2 FROM dml_union_r UNION SELECT a, c FROM dml_union_r INTERSECT SELECT a, c FROM dml_union_s ORDER BY 1) foo group by f1,f2) foo)foo;
SELECT COUNT(*) FROM dml_union_r;

