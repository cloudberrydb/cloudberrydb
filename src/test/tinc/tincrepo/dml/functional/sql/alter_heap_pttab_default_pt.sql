-- @author prabhd
-- @created 2012-12-05 12:00:00
-- @modified 2012-12-05 12:00:00
-- @tags dml
-- @db_name dmldb
-- @description test: Alter default partition
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

-- ADD DEFAULT PARTITION
SELECT COUNT(*) FROM dml_pt_tab;
ALTER TABLE dml_pt_tab ADD DEFAULT PARTITION def_part;
INSERT INTO dml_pt_tab VALUES(NULL,'dml_pt_tab','q','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab;
UPDATE dml_pt_tab SET i = NULL WHERE c = 'a';
SELECT DISTINCT(i) FROM dml_pt_tab WHERE c = 'a';
SELECT COUNT(*) FROM dml_pt_tab_1_prt_def_part;

-- SPLIT DEFAULT PARTITION
ALTER TABLE dml_pt_tab SPLIT DEFAULT PARTITION start(41) end(51) into (partition p5, partition def_part);
INSERT INTO dml_pt_tab VALUES(NULL,'dml_pt_tab','r','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
INSERT INTO dml_pt_tab VALUES(generate_series(40,50),'dml_pt_tab','s','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab_1_prt_def_part;
SELECT COUNT(*) FROM dml_pt_tab_1_prt_p5;
SELECT COUNT(*) FROM dml_pt_tab;

-- DROP DEFAULT PARTITION
ALTER TABLE dml_pt_tab DROP DEFAULT PARTITION ;
UPDATE dml_pt_tab SET i = NULL;
SELECT DISTINCT(i) FROM dml_pt_tab;
SELECT COUNT(*) FROM dml_pt_tab WHERE i is NULL;
DELETE FROM dml_pt_tab WHERE i is NULL;
SELECT COUNT(*) FROM dml_pt_tab WHERE i is NULL;


