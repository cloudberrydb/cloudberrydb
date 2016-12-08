-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for inet
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_inet;
CREATE TABLE dml_inet(a inet) distributed by (a);

-- Simple DML
INSERT INTO dml_inet VALUES ('192.168.1.6');
SELECT * FROM dml_inet ORDER BY 1;
INSERT INTO dml_inet VALUES ('204.248.199.199/30');
SELECT * FROM dml_inet ORDER BY 1;
INSERT INTO dml_inet VALUES('::1');
SELECT * FROM dml_inet ORDER BY 1;
UPDATE dml_inet SET a = '::1';
SELECT * FROM dml_inet ORDER BY 1;

-- OUT OF RANGE VALUES
INSERT INTO dml_inet VALUES ('192.168.23.20/33');
SELECT * FROM dml_inet ORDER BY 1;
INSERT INTO dml_inet VALUES('');
SELECT * FROM dml_inet ORDER BY 1;
UPDATE dml_inet SET a = '192.168.23.20/33';
SELECT * FROM dml_inet ORDER BY 1;
