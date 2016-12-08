-- @author prabhd
-- @created 2012-12-05 12:00:00
-- @modified 2012-12-05 12:00:00
-- @tags dml
-- @db_name dmldb
-- @description test: Alter table alter column
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

-- ADD COLUMN
SELECT COUNT(*) FROM dml_pt_tab;
ALTER TABLE dml_pt_tab add column addcol1 decimal default 10.00 ;
SELECT COUNT(*) FROM dml_pt_tab WHERE addcol1 = 10.00;
UPDATE dml_pt_tab SET addcol1 = 1.00;
SELECT COUNT(*) FROM dml_pt_tab WHERE addcol1 = 1.00;
INSERT INTO dml_pt_tab VALUES(generate_series(11,12),'dml_pt_tab','b','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab WHERE addcol1 = 1.00;

-- CHANGE COLUMN TYPE
SELECT COUNT(*) FROM dml_pt_tab;
ALTER TABLE dml_pt_tab ALTER COLUMN addcol1 type numeric ;
INSERT INTO dml_pt_tab VALUES(generate_series(1,12),'dml_pt_tab','c','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02', 10.10);
SELECT COUNT(*) FROM dml_pt_tab;

-- RENAME COLUMN
ALTER TABLE dml_pt_tab RENAME COLUMN addcol1 to newaddcol1 ;
UPDATE dml_pt_tab SET newaddcol1 = 1.11 , i = 1;
SELECT COUNT(*) FROM dml_pt_tab WHERE i = 1;

-- DROP COLUMN
ALTER TABLE dml_pt_tab DROP COLUMN newaddcol1;
SELECT COUNT(*) FROM dml_pt_tab;
INSERT INTO dml_pt_tab VALUES(generate_series(1,12),'dml_pt_tab','e','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab;


