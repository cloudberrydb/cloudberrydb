-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @gpopt 1.532
-- @description union_test28: Negative tests MORE THAN ONE ROW RETURNED
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
INSERT INTO dml_union_r SELECT (SELECT dml_union_r.d::int FROM dml_union_r INTERSECT SELECT dml_union_s.d FROM dml_union_s ORDER BY 1),1,'newval',1.000;
SELECT COUNT(*) FROM dml_union_r;

