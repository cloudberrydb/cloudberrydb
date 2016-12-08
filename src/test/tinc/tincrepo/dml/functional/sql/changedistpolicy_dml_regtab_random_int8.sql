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
DROP TABLE IF EXISTS changedistpolicy_dml_regtab_random_int8;
CREATE TABLE changedistpolicy_dml_regtab_random_int8
(
    col1 int8,
    col2 int8,
    col3 char,
    col4 int8,
    col5 int
) DISTRIBUTED by (col1);

INSERT INTO changedistpolicy_dml_regtab_random_int8 VALUES(200000000000000000,200000000000000000,'a',200000000000000000,0);
SELECT * FROM changedistpolicy_dml_regtab_random_int8 ORDER BY 1,2,3,4;

ALTER TABLE changedistpolicy_dml_regtab_random_int8 SET DISTRIBUTED randomly;

INSERT INTO changedistpolicy_dml_regtab_random_int8 SELECT 1000000000000000000, 1000000000000000000,'b', 1000000000000000000, 1;
SELECT * FROM changedistpolicy_dml_regtab_random_int8 ORDER BY 1,2,3;

-- Update distribution key
UPDATE changedistpolicy_dml_regtab_random_int8 SET col3 ='c' WHERE col3 ='b' AND col5 = 1;
SELECT * FROM changedistpolicy_dml_regtab_random_int8 ORDER BY 1,2,3;

DELETE FROM changedistpolicy_dml_regtab_random_int8 WHERE col3 ='c';
SELECT * FROM changedistpolicy_dml_regtab_random_int8 ORDER BY 1,2,3;

