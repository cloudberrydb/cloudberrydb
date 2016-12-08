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
DROP TABLE IF EXISTS changedistpolicy_dml_pttab_float;
CREATE TABLE changedistpolicy_dml_pttab_float
(
    col1 float,
    col2 float,
    col3 char,
    col4 float,
    col5 int
) DISTRIBUTED BY (col1) PARTITION BY RANGE(col2)(partition partone start(1.00) end(10.00)  WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=column),partition parttwo start(10.00) end(20.00) WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=row),partition partthree start(20.00) end(30.00));

INSERT INTO changedistpolicy_dml_pttab_float VALUES(2.00,2.00,'a',2.00,0);
SELECT * FROM changedistpolicy_dml_pttab_float ORDER BY 1,2,3,4;

ALTER TABLE changedistpolicy_dml_pttab_float SET DISTRIBUTED BY (col3);

INSERT INTO changedistpolicy_dml_pttab_float SELECT 1.00, 1.00,'b', 1.00, 1;
SELECT * FROM changedistpolicy_dml_pttab_float ORDER BY 1,2,3;

-- Update distribution key
UPDATE changedistpolicy_dml_pttab_float SET col3 ='c' WHERE col3 ='b' AND col5 = 1;
SELECT * FROM changedistpolicy_dml_pttab_float ORDER BY 1,2,3;

DELETE FROM changedistpolicy_dml_pttab_float WHERE col3 ='c';
SELECT * FROM changedistpolicy_dml_pttab_float ORDER BY 1,2,3;

