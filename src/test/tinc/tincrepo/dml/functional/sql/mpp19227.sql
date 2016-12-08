-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description MPP-19227 Check the error message on AO/CO tables when we issue updates
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

UPDATE f SET a = NULL from a where a.a = f.a;
UPDATE e SET a = NULL from a where a.a = e.a;
