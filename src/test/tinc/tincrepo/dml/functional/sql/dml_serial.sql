-- @author prabhd
-- @created 2012-12-05 12:00:00
-- @modified 2012-12-05 12:00:00
-- @tags dml
-- @db_name dmldb
-- @description test: Boundary test for serial
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP SEQUENCE IF EXISTS serial;
CREATE SEQUENCE serial START 101;

DROP TABLE IF EXISTS dml_serial;
CREATE TABLE dml_serial(a serial) distributed by (a);

INSERT INTO dml_serial VALUES (nextval('serial'));
INSERT INTO dml_serial VALUES (nextval('serial'));

SELECT * FROM dml_serial ORDER BY 1;
UPDATE dml_serial set a = nextval('serial');

SELECT * FROM dml_serial ORDER BY 1;

UPDATE dml_serial set a = nextval('serial');

SELECT * FROM dml_serial ORDER BY 1;
