-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for box
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_box;
CREATE TABLE dml_box(a box) DISTRIBUTED RANDOMLY;
-- Simple DML
INSERT INTO dml_box VALUES ('(8,9), (1,3)');
SELECT area((select * from dml_box));

UPDATE dml_box SET a = '(1,1), (1,1)';
SELECT area((select * from dml_box));

