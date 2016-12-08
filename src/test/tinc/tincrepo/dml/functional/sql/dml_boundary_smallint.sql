-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for smallint
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_tab_smallint;
CREATE TABLE dml_tab_smallint (a smallint) distributed by (a);

-- Simple DML
INSERT INTO dml_tab_smallint values(-32768);
SELECT * FROM dml_tab_smallint ORDER BY 1;
INSERT INTO dml_tab_smallint values(32767);
SELECT * FROM dml_tab_smallint ORDER BY 1;
UPDATE dml_tab_smallint SET a = 32767;
SELECT * FROM dml_tab_smallint ORDER BY 1;
UPDATE dml_tab_smallint SET a = -32768;
SELECT * FROM dml_tab_smallint ORDER BY 1;

-- OUT OF RANGE
INSERT INTO dml_tab_smallint values(-32769);
SELECT * FROM dml_tab_smallint ORDER BY 1;
INSERT INTO dml_tab_smallint values(32768);
SELECT * FROM dml_tab_smallint ORDER BY 1;
UPDATE dml_tab_smallint SET a = 32768;
SELECT * FROM dml_tab_smallint ORDER BY 1;
UPDATE dml_tab_smallint SET a = -32769;
SELECT * FROM dml_tab_smallint ORDER BY 1;





