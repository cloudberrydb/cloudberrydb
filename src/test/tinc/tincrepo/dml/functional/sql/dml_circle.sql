-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test1: Boundary test for circle
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_circle;
CREATE TABLE dml_circle(a circle) DISTRIBUTED RANDOMLY;

-- Simple DML
INSERT INTO dml_circle VALUES ('10, 4, 10');
SELECT area((select * from dml_circle));

UPDATE dml_circle SET a = '1,1,1';
SELECT area((select * from dml_circle));
