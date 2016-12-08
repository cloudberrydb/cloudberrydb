-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for point
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_point;
CREATE TABLE dml_point(a point) DISTRIBUTED RANDOMLY;

-- Simple DML
INSERT INTO dml_point VALUES ('1,2');
SELECT * FROM dml_point;

UPDATE dml_point SET a = '2,3';
SELECT * FROM dml_point;


