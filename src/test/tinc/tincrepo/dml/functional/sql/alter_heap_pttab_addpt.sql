-- @author prabhd
-- @created 2012-12-05 12:00:00
-- @modified 2012-12-05 12:00:00
-- @tags dml
-- @db_name dmldb
-- @description test: Alter table add/drop partition
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

-- ALTER TABLE ADD PARTITION
SELECT COUNT(*) FROM dml_pt_tab;
ALTER TABLE dml_pt_tab ADD PARTITION padd start(61) end(71) ;
INSERT INTO dml_pt_tab VALUES(generate_series(61,70),'dml_pt_tab','u','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab;
UPDATE dml_pt_tab SET x = 'split partition';
SELECT DISTINCT(x) FROM dml_pt_tab;

-- DROP PARTITION( DML should fail )
ALTER TABLE dml_pt_tab DROP PARTITION padd;
INSERT INTO dml_pt_tab VALUES(generate_series(61,71),'dml_pt_tab','u','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab;

