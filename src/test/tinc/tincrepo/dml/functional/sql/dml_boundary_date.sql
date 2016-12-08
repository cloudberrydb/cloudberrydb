-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for date
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_date;
CREATE TABLE dml_date ( a date) distributed by (a);

-- Simple DML
INSERT INTO dml_date VALUES ('2013-01-01 08:00:00.000000'::date);
SELECT * FROM dml_date ORDER BY 1;
INSERT INTO dml_date VALUES ('4713-01-01 BC');
SELECT * FROM dml_date ORDER BY 1;
INSERT INTO dml_date select to_date('3232098', 'MM/DD/YYYY');
SELECT * FROM dml_date ORDER BY 1;
UPDATE dml_date SET a = '4713-01-01 BC';
SELECT * FROM dml_date ORDER BY 1;

-- OUT OF RANGE VALUES
INSERT INTO dml_date VALUES ('0000-01-01 08:00:00.000000'::date);
SELECT * FROM dml_date ORDER BY 1;
INSERT INTO dml_date VALUES ('4714-01-01 BC');
SELECT * FROM dml_date ORDER BY 1;
UPDATE dml_date SET a = '4714-01-01 BC';
SELECT * FROM dml_date ORDER BY 1;

