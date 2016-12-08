-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for bigint
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_tab_bigint;
CREATE TABLE dml_tab_bigint (a bigint DEFAULT 1234567891011) distributed by (a);

-- Simple DML
INSERT INTO dml_tab_bigint values(-9223372036854775808);
INSERT INTO dml_tab_bigint values(9223372036854775807);
SELECT * FROM dml_tab_bigint ORDER BY 1;
INSERT INTO dml_tab_bigint DEFAULT VALUES;
SELECT * FROM dml_tab_bigint ORDER BY 1;
UPDATE dml_tab_bigint SET a = 9223372036854775807;
SELECT * FROM dml_tab_bigint ORDER BY 1;
UPDATE dml_tab_bigint SET a = -9223372036854775808;
SELECT * FROM dml_tab_bigint ORDER BY 1;

-- OUT OF RANGE
SELECT * FROM dml_tab_bigint ORDER BY 1;
INSERT INTO dml_tab_bigint values(-9223372036854775809);
SELECT * FROM dml_tab_bigint ORDER BY 1;
INSERT INTO dml_tab_bigint values(9223372036854775808);
SELECT * FROM dml_tab_bigint ORDER BY 1;
UPDATE dml_tab_bigint SET a = -9223372036854775809;
SELECT * FROM dml_tab_bigint ORDER BY 1;
UPDATE dml_tab_bigint SET a = 9223372036854775808;
SELECT * FROM dml_tab_bigint ORDER BY 1;

