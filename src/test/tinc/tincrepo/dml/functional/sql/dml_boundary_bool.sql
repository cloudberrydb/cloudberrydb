-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for bool
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore


DROP TABLE IF EXISTS dml_bool;
CREATE TABLE dml_bool (a bool) distributed by (a);

-- Simple DML
INSERT INTO dml_bool VALUES('True');
SELECT * FROM dml_bool ORDER BY 1;
INSERT INTO dml_bool VALUES('False');
SELECT * FROM dml_bool ORDER BY 1;
UPDATE dml_bool SET a = 'True';
SELECT * FROM dml_bool ORDER BY 1;

