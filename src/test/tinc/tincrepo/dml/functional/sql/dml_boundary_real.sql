-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for real
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_real;
CREATE TABLE dml_real ( a real) distributed by (a);

-- Simple DML
INSERT INTO dml_real VALUES(0);
SELECT * FROM dml_real ORDER BY 1;
INSERT INTO dml_real VALUES('-1.18e-38');
SELECT * FROM dml_real ORDER BY 1;
UPDATE dml_real SET a = 0;
SELECT * FROM dml_real ORDER BY 1;


-- OUT OF RANGE VALUES
INSERT INTO dml_real VALUES('-3.40e+39');
SELECT * FROM dml_real ORDER BY 1;
INSERT INTO dml_real VALUES('-1.00e-46');
SELECT * FROM dml_real ORDER BY 1;
UPDATE dml_real SET a = '-1.00e-46';
SELECT * FROM dml_real ORDER BY 1;

