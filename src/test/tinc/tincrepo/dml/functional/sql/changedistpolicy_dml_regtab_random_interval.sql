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
DROP TABLE IF EXISTS changedistpolicy_dml_regtab_random_interval;
CREATE TABLE changedistpolicy_dml_regtab_random_interval
(
    col1 interval,
    col2 interval,
    col3 char,
    col4 interval,
    col5 int
) DISTRIBUTED by (col1);

INSERT INTO changedistpolicy_dml_regtab_random_interval VALUES('10 secs','10 secs','a','10 secs',0);
SELECT * FROM changedistpolicy_dml_regtab_random_interval ORDER BY 1,2,3,4;

ALTER TABLE changedistpolicy_dml_regtab_random_interval SET DISTRIBUTED randomly;

INSERT INTO changedistpolicy_dml_regtab_random_interval SELECT '11 hours', '11 hours','b', '11 hours', 1;
SELECT * FROM changedistpolicy_dml_regtab_random_interval ORDER BY 1,2,3;

-- Update distribution key
UPDATE changedistpolicy_dml_regtab_random_interval SET col3 ='c' WHERE col3 ='b' AND col5 = 1;
SELECT * FROM changedistpolicy_dml_regtab_random_interval ORDER BY 1,2,3;

DELETE FROM changedistpolicy_dml_regtab_random_interval WHERE col3 ='c';
SELECT * FROM changedistpolicy_dml_regtab_random_interval ORDER BY 1,2,3;

