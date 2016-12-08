-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for double precision
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore


DROP TABLE IF EXISTS dml_dp;
CREATE TABLE dml_dp ( a double precision) distributed by (a);

-- Simple DML
INSERT INTO dml_dp VALUES('5.7107617381473e+45');
SELECT * FROM dml_dp ORDER BY 1;
INSERT INTO dml_dp VALUES('5.7107617381473e-10');
SELECT * FROM dml_dp ORDER BY 1;
UPDATE dml_dp SET a = 5.7107617381473e+45;
SELECT * FROM dml_dp ORDER BY 1;

-- OUT OF RANGE
INSERT INTO dml_dp VALUES('5.7107617381473e+1000');
SELECT * FROM dml_dp ORDER BY 1;
INSERT INTO dml_dp VALUES('5.7107617381473e-1000');
SELECT * FROM dml_dp ORDER BY 1;
UPDATE dml_dp SET a = 5.7107617381473e+1000;
SELECT * FROM dml_dp ORDER BY 1;

