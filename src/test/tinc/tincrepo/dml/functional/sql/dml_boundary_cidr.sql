-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for cidr
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_cidr;
CREATE TABLE dml_cidr (a cidr) distributed by (a);

-- Simple DML
INSERT INTO dml_cidr VALUES('192.168.100.128/25');
SELECT * FROM dml_cidr ORDER BY 1;
INSERT INTO dml_cidr VALUES('128');
SELECT * FROM dml_cidr ORDER BY 1;
INSERT INTO dml_cidr VALUES('2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128');
SELECT * FROM dml_cidr ORDER BY 1;
UPDATE dml_cidr SET a = '192.168.100.128/25';
SELECT * FROM dml_cidr ORDER BY 1;

-- OUT OF RANGE
INSERT INTO dml_cidr VALUES('204.248.199.199/30');
SELECT * FROM dml_cidr ORDER BY 1;
INSERT INTO dml_cidr VALUES('2001:4f8:3:ba:2e0:81ff:fe22:d1f1/120');
SELECT * FROM dml_cidr ORDER BY 1;
UPDATE dml_cidr SET a = '204.248.199.199/30';
SELECT * FROM dml_cidr ORDER BY 1;

