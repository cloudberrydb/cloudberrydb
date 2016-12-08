-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for path
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_path;
CREATE TABLE dml_path(a path) DISTRIBUTED RANDOMLY;

-- Simple DML
INSERT INTO dml_path VALUES ('(3,1), (2,8), (10,4)');
SELECT length((select * from dml_path));

UPDATE dml_path SET a = '(4,2), (3,2), (11,5)';
SELECT length((select * from dml_path));
