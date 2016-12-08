-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for bigserial
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_bigserial;
CREATE TABLE dml_bigserial (a bigserial) distributed by (a);

-- Simple DML
INSERT INTO dml_bigserial VALUES(9223372036854775807);
SELECT * FROM dml_bigserial ORDER BY 1;
INSERT INTO dml_bigserial VALUES(0);
SELECT * FROM dml_bigserial ORDER BY 1;
UPDATE dml_bigserial SET a = 9223372036854775807;
SELECT * FROM dml_bigserial ORDER BY 1;
UPDATE dml_bigserial SET a = 0;
SELECT * FROM dml_bigserial ORDER BY 1;

-- OUT OF RANGE
INSERT INTO dml_bigserial VALUES(9223372036854775808);
SELECT * FROM dml_bigserial ORDER BY 1;
UPDATE dml_bigserial SET a = 9223372036854775808;
SELECT * FROM dml_bigserial ORDER BY 1;


