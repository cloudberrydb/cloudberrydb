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
DROP TABLE IF EXISTS changedistpolicy_dml_pttab_int8;
CREATE TABLE changedistpolicy_dml_pttab_int8
(
    col1 int8,
    col2 int8,
    col3 char,
    col4 int8,
    col5 int
) DISTRIBUTED BY (col1) PARTITION BY RANGE(col2)(partition partone start(1) end(1000000000000000001)  WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=column),partition parttwo start(1000000000000000001) end(2000000000000000001) WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=row),partition partthree start(2000000000000000001) end(3000000000000000001));

INSERT INTO changedistpolicy_dml_pttab_int8 VALUES(200000000000000000,200000000000000000,'a',200000000000000000,0);
SELECT * FROM changedistpolicy_dml_pttab_int8 ORDER BY 1,2,3,4;

ALTER TABLE changedistpolicy_dml_pttab_int8 SET DISTRIBUTED BY (col3);

INSERT INTO changedistpolicy_dml_pttab_int8 SELECT 1000000000000000000, 1000000000000000000,'b', 1000000000000000000, 1;
SELECT * FROM changedistpolicy_dml_pttab_int8 ORDER BY 1,2,3;

-- Update distribution key
UPDATE changedistpolicy_dml_pttab_int8 SET col3 ='c' WHERE col3 ='b' AND col5 = 1;
SELECT * FROM changedistpolicy_dml_pttab_int8 ORDER BY 1,2,3;

DELETE FROM changedistpolicy_dml_pttab_int8 WHERE col3 ='c';
SELECT * FROM changedistpolicy_dml_pttab_int8 ORDER BY 1,2,3;

