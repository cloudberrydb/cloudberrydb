-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for char
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore


DROP TABLE IF EXISTS dml_char;
CREATE TABLE dml_char( a char) distributed by (a);
-- Simple DML
INSERT INTO dml_char VALUES('1');
SELECT * FROM dml_char ORDER BY 1;
UPDATE dml_char SET a = 'b';
SELECT * FROM dml_char ORDER BY 1;

-- OUT OF RANGE VALUES
INSERT INTO dml_char VALUES('ab');
SELECT * FROM dml_char ORDER BY 1;
INSERT INTO dml_char VALUES('-a');
SELECT * FROM dml_char ORDER BY 1;
UPDATE dml_char SET a ='-1';
SELECT * FROM dml_char ORDER BY 1;
