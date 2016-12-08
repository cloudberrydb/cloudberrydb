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
DROP TABLE IF EXISTS changedistpolicy_dml_pttab_int4;
CREATE TABLE changedistpolicy_dml_pttab_int4
(
    col1 int4,
    col2 int4,
    col3 char,
    col4 int4,
    col5 int
) DISTRIBUTED BY (col1) PARTITION BY RANGE(col2)(partition partone start(1) end(100000001)  WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=column),partition parttwo start(100000001) end(200000001) WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=row),partition partthree start(200000001) end(300000001));

INSERT INTO changedistpolicy_dml_pttab_int4 VALUES(20000000,20000000,'a',20000000,0);
SELECT * FROM changedistpolicy_dml_pttab_int4 ORDER BY 1,2,3,4;

ALTER TABLE changedistpolicy_dml_pttab_int4 SET DISTRIBUTED BY (col3);

INSERT INTO changedistpolicy_dml_pttab_int4 SELECT 10000000, 10000000,'b', 10000000, 1;
SELECT * FROM changedistpolicy_dml_pttab_int4 ORDER BY 1,2,3;

-- Update distribution key
UPDATE changedistpolicy_dml_pttab_int4 SET col3 ='c' WHERE col3 ='b' AND col5 = 1;
SELECT * FROM changedistpolicy_dml_pttab_int4 ORDER BY 1,2,3;

DELETE FROM changedistpolicy_dml_pttab_int4 WHERE col3 ='c';
SELECT * FROM changedistpolicy_dml_pttab_int4 ORDER BY 1,2,3;

