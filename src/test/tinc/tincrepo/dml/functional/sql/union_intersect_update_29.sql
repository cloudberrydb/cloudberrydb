-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @gpopt 1.532
-- @description union_update_test29: Negative Tests  UPDATE violates the CHECK constraint on the column
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(DISTINCT(b)) FROM dml_union_s;
UPDATE dml_union_s SET b = (SELECT NULL UNION SELECT NULL)::numeric;

SELECT COUNT(DISTINCT(b)) FROM dml_union_s;
SELECT DISTINCT(b) FROM dml_union_s;

