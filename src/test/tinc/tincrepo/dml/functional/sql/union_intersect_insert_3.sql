-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_test3: INTERSECT with generate_series
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) FROM dml_union_r;
SELECT COUNT(*) FROM (SELECT generate_series(1,10) INTERSECT SELECT generate_series(1,100))foo;
INSERT INTO dml_union_r SELECT generate_series(1,10) INTERSECT SELECT generate_series(1,100);
SELECT COUNT(*) FROM dml_union_r;

