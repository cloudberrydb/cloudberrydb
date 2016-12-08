-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test21:  UNION/INTERSECT/EXCEPT within dml_union_sub-query
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(DISTINCT(a)) FROM dml_union_r;
UPDATE dml_union_r SET a = dml_union_r.a WHERE b in (SELECT b FROM dml_union_r INTERSECT SELECT b FROM dml_union_s);
SELECT DISTINCT(a) FROM dml_union_r;
SELECT COUNT(DISTINCT(a)) FROM dml_union_r;
