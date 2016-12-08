-- @author prabhd 
-- @created 2014-04-01 12:00:00
-- @modified 2012-04-01 12:00:00
-- @tags dml MPP-21090 ORCA
-- @product_version gpdb: [4.3-]
-- @optimizer_mode on	
-- @description Tests for MPP-21090
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP TABLE IF EXISTS changedistpolicy_dml_regtab_timestamp;
CREATE TABLE changedistpolicy_dml_regtab_timestamp
(
    col1 timestamp,
    col2 timestamp,
    col3 char,
    col4 timestamp,
    col5 int
) DISTRIBUTED by (col1);

INSERT INTO changedistpolicy_dml_regtab_timestamp VALUES('2013-12-30 12:00:00','2013-12-30 12:00:00','a','2013-12-30 12:00:00',0);
SELECT * FROM changedistpolicy_dml_regtab_timestamp ORDER BY 1,2,3,4;

ALTER TABLE changedistpolicy_dml_regtab_timestamp SET DISTRIBUTED BY (col3);

INSERT INTO changedistpolicy_dml_regtab_timestamp SELECT '2014-01-01 12:00:00', '2014-01-01 12:00:00','b', '2014-01-01 12:00:00', 1;
SELECT * FROM changedistpolicy_dml_regtab_timestamp ORDER BY 1,2,3;

-- Update distribution key
UPDATE changedistpolicy_dml_regtab_timestamp SET col3 ='c' WHERE col3 ='b' AND col5 = 1;
SELECT * FROM changedistpolicy_dml_regtab_timestamp ORDER BY 1,2,3;

DELETE FROM changedistpolicy_dml_regtab_timestamp WHERE col3 ='c';
SELECT * FROM changedistpolicy_dml_regtab_timestamp ORDER BY 1,2,3;

