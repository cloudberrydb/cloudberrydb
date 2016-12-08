-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_test31: INSERT NON ATOMICS with union/intersect/except
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
SELECT COUNT(*) FROM (SELECT * FROM dml_union_r WHERE a in (SELECT dml_union_r.d::int FROM dml_union_r INTERSECT SELECT dml_union_s.d FROM dml_union_s ORDER BY 1) UNION SELECT * FROM dml_union_s)bar;
INSERT INTO dml_union_r SELECT * FROM dml_union_r WHERE a in (SELECT dml_union_r.d::int FROM dml_union_r INTERSECT SELECT dml_union_s.d FROM dml_union_s ORDER BY 1) UNION SELECT * FROM dml_union_s;
SELECT COUNT(*) FROM dml_union_r;

