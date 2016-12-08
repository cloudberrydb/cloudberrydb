-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description union_update_test28: Negative Tests Update the partition key to an out of dml_union_range value with no default partition
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

ALTER TABLE dml_union_s drop default partition;
SELECT COUNT(DISTINCT(d)) FROM dml_union_s;
UPDATE dml_union_s SET d = (SELECT NULL EXCEPT SELECT NULL)::numeric; 
SELECT DISTINCT(d) FROM dml_union_s;
SELECT COUNT(DISTINCT(d)) FROM dml_union_s;
ALTER TABLE dml_union_s ADD default partition def;

