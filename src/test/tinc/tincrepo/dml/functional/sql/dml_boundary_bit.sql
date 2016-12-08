-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for bit
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_bit;
CREATE TABLE dml_bit( a bit ) distributed by (a);

-- Simple DML
INSERT INTO dml_bit VALUES('1');
SELECT * FROM dml_bit ORDER BY 1;
UPDATE dml_bit SET a = '0';
SELECT * FROM dml_bit ORDER BY 1;

-- OUT OF RANGE VALUES
INSERT INTO dml_bit VALUES('True');
SELECT * FROM dml_bit ORDER BY 1;
INSERT INTO dml_bit VALUES('11');
SELECT * FROM dml_bit ORDER BY 1;
UPDATE dml_bit SET a = '00';
SELECT * FROM dml_bit ORDER BY 1;
