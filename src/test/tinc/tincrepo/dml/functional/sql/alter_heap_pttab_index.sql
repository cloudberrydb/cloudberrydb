-- @author prabhd
-- @created 2012-12-05 12:00:00
-- @modified 2012-12-05 12:00:00
-- @tags dml
-- @db_name dmldb
-- @description test: Alter table alter index
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

-- CREATE INDEX
SELECT COUNT(*) FROM dml_pt_tab;
CREATE INDEX dml_pt_tab_idx1 on dml_pt_tab (i);
INSERT INTO dml_pt_tab VALUES(generate_series(12,22),'dml_pt_tab','j','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab;

CREATE INDEX dml_pt_tab_idx2 on dml_pt_tab using bitmap (n);
INSERT INTO dml_pt_tab VALUES(generate_series(12,22),'dml_pt_tab','k','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
INSERT INTO dml_pt_tab VALUES(generate_series(12,22),'dml_pt_tab','l','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab;

DELETE FROM dml_pt_tab;
SELECT COUNT(*) FROM dml_pt_tab;
ALTER INDEX dml_pt_tab_idx1 RENAME TO dml_pt_tab_idx1_new;
ALTER INDEX dml_pt_tab_idx2 RENAME TO dml_pt_tab_idx2_new;

INSERT INTO dml_pt_tab VALUES(generate_series(12,22),'dml_pt_tab','m','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');
SELECT COUNT(*) FROM dml_pt_tab;

