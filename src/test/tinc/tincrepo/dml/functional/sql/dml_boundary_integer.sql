-- @author prabhd 
-- @CREATEd 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for integer
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_integer;
CREATE TABLE dml_integer (a integer) distributed by (a);

-- Simple DML
INSERT INTO dml_integer VALUES(-2147483648);
SELECT * FROM dml_integer ORDER BY 1;
INSERT INTO dml_integer VALUES(2147483647);
SELECT * FROM dml_integer ORDER BY 1;
UPDATE dml_integer SET a = 2147483647;
SELECT * FROM dml_integer ORDER BY 1;
UPDATE dml_integer SET a = -2147483648;
SELECT * FROM dml_integer ORDER BY 1;

-- OUT OF RANGE VALUES
INSERT INTO dml_integer VALUES(-2147483649);
SELECT * FROM dml_integer ORDER BY 1;
INSERT INTO dml_integer VALUES(2147483648);
SELECT * FROM dml_integer ORDER BY 1;
UPDATE dml_integer SET a = 2147483648;
SELECT * FROM dml_integer ORDER BY 1;
UPDATE dml_integer SET a = -2147483649;
SELECT * FROM dml_integer ORDER BY 1;

