-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test: Boundary test for polygon
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_polygon;
CREATE TABLE dml_polygon (a polygon) DISTRIBUTED RANDOMLY;

-- Simple DML 
INSERT INTO dml_polygon VALUES ('((2,2),(3,4),(3,6),(1,1))');
SELECT point(a) FROM dml_polygon;

UPDATE dml_polygon SET a = '((2,2),(3,3),(4,4),(1,1))';
SELECT point(a) FROM dml_polygon;



