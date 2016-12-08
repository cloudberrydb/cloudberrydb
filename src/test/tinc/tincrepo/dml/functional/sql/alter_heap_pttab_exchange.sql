-- @author prabhd
-- @created 2012-12-05 12:00:00
-- @modified 2012-12-05 12:00:00
-- @tags dml
-- @db_name dmldb
-- @description test: Alter table exchange partition
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

-- ALTER TABLE EXCHANGE PARTITION
SELECT COUNT(*) FROM dml_pt_tab;

DROP TABLE IF EXISTS dml_pt_can_tab;
CREATE TABLE dml_pt_can_tab( like dml_pt_tab) distributed by (c);
INSERT INTO dml_pt_can_tab VALUES(generate_series(1,4),'dml_pt_can_tab','t','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_can_tab;

ALTER TABLE dml_pt_tab exchange partition p1 with table dml_pt_can_tab;
SELECT COUNT(*) FROM dml_pt_can_tab;
SELECT COUNT(*) FROM dml_pt_tab;

INSERT INTO dml_pt_can_tab VALUES(generate_series(1,4),'dml_pt_can_tab','t','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
INSERT INTO dml_pt_tab VALUES(generate_series(1,15),'dml_pt_tab','u','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');

SELECT COUNT(*) FROM dml_pt_can_tab;
SELECT COUNT(*) FROM dml_pt_tab;

