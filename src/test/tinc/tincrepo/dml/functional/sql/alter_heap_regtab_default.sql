-- @author prabhd
-- @created 2012-12-05 12:00:00
-- @modified 2012-12-05 12:00:00
-- @tags dml
-- @db_name dmldb
-- @description test: Alter table set/drop default values
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

-- SET DEFAULT
SELECT COUNT(*) FROM dml_tab WHERE x = 'abcdefghijklmnopqrstuvwxyz';
ALTER TABLE dml_tab ALTER COLUMN x SET DEFAULT 'abcdefghijklmnopqrstuvwxyz';
INSERT INTO dml_tab(i,c,v,d,n,t,tz) VALUES(13,'d','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_tab WHERE x = 'abcdefghijklmnopqrstuvwxyz';

-- DROP DEFAULT
SELECT COUNT(*) FROM dml_tab;
ALTER TABLE dml_tab ALTER COLUMN x DROP DEFAULT;
INSERT INTO dml_tab(i,c,v,d,n,t,tz) VALUES(13,'i','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_tab;


