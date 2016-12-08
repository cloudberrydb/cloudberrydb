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
DROP TABLE IF EXISTS changedistpolicy_dml_regtab_int;
CREATE TABLE changedistpolicy_dml_regtab_int
(
    col1 int,
    col2 int,
    col3 char,
    col4 int,
    col5 int
) DISTRIBUTED by (col1);

INSERT INTO changedistpolicy_dml_regtab_int VALUES(10000,10000,'a',10000,0);
SELECT * FROM changedistpolicy_dml_regtab_int ORDER BY 1,2,3,4;

ALTER TABLE changedistpolicy_dml_regtab_int SET DISTRIBUTED BY (col3);

INSERT INTO changedistpolicy_dml_regtab_int SELECT 20000, 20000,'b', 20000, 1;
SELECT * FROM changedistpolicy_dml_regtab_int ORDER BY 1,2,3;

-- Update distribution key
UPDATE changedistpolicy_dml_regtab_int SET col3 ='c' WHERE col3 ='b' AND col5 = 1;
SELECT * FROM changedistpolicy_dml_regtab_int ORDER BY 1,2,3;

DELETE FROM changedistpolicy_dml_regtab_int WHERE col3 ='c';
SELECT * FROM changedistpolicy_dml_regtab_int ORDER BY 1,2,3;

