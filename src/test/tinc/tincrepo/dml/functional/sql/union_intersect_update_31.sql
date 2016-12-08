-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @gpopt 1.532
-- @description union_update_test31: Negative Tests  more than one row returned by a sub-query used as an expression
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

UPDATE dml_union_r SET b = ( SELECT a FROM dml_union_r EXCEPT ALL SELECT a FROM dml_union_s);

