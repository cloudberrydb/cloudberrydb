-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for macaddr
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_macaddr;
CREATE TABLE dml_macaddr( a macaddr) distributed by (a);
-- SIMPLE INSERTS
INSERT INTO dml_macaddr VALUES('08002b:010203');
SELECT * FROM dml_macaddr ORDER BY 1;
UPDATE dml_macaddr SET a = 'FF:FF:FF:FF:FF:FF';
SELECT * FROM dml_macaddr ORDER BY 1;

--INVALID VALUES
INSERT INTO dml_macaddr VALUES('FF:FF:FF:FF:FF:FF0');
SELECT * FROM dml_macaddr ORDER BY 1;
UPDATE dml_macaddr SET a = 'FF:FF:FF:FF:FF:FF0';
SELECT * FROM dml_macaddr ORDER BY 1;
