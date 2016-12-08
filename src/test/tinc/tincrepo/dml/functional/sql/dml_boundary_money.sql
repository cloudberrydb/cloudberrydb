-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @skip 'Money datatype was enlarged to 64 bits in PostgreSQL 8.3 so will be tested via installcheck-good' 
-- @db_name dmldb
-- @description test: Boundary test for money
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_money;
CREATE TABLE dml_money (a money) distributed by (a);

-- SIMPLE INSERTS
INSERT INTO dml_money VALUES('-2147483648'::money);
SELECT * FROM dml_money ORDER BY 1;
INSERT INTO dml_money VALUES('2147483647'::money);
SELECT * FROM dml_money ORDER BY 1;
UPDATE dml_money SET a = '2147483647'::money;
SELECT * FROM dml_money ORDER BY 1;
UPDATE dml_money SET a = '-2147483648'::money;
SELECT * FROM dml_money ORDER BY 1;

-- OUT OF RANGE (no error observed)
INSERT INTO dml_money VALUES('-2147483649'::money);
SELECT * FROM dml_money ORDER BY 1;
INSERT INTO dml_money VALUES('2147483648'::money);
SELECT * FROM dml_money ORDER BY 1;
UPDATE dml_money SET a = '2147483648'::money;
SELECT * FROM dml_money ORDER BY 1;
UPDATE dml_money SET a = '-2147483649'::money;
SELECT * FROM dml_money ORDER BY 1;
