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
DROP TABLE IF EXISTS changedistpolicy_dml_pttab_char;
CREATE TABLE changedistpolicy_dml_pttab_char
(
    col1 char,
    col2 char,
    col3 char,
    col4 char,
    col5 int
) DISTRIBUTED BY (col1) PARTITION BY LIST(col2)(partition partone VALUES('a','b','c','d','e','f','g','h') WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=column),partition parttwo VALUES('i','j','k','l','m','n','o','p') WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=row),partition partthree VALUES('q','r','s','t','u','v','w','x'));

INSERT INTO changedistpolicy_dml_pttab_char VALUES('g','g','a','g',0);
SELECT * FROM changedistpolicy_dml_pttab_char ORDER BY 1,2,3,4;

ALTER TABLE changedistpolicy_dml_pttab_char SET DISTRIBUTED BY (col3);

INSERT INTO changedistpolicy_dml_pttab_char SELECT 'a', 'a','b', 'a', 1;
SELECT * FROM changedistpolicy_dml_pttab_char ORDER BY 1,2,3;

-- Update distribution key
UPDATE changedistpolicy_dml_pttab_char SET col3 ='c' WHERE col3 ='b' AND col5 = 1;
SELECT * FROM changedistpolicy_dml_pttab_char ORDER BY 1,2,3;

DELETE FROM changedistpolicy_dml_pttab_char WHERE col3 ='c';
SELECT * FROM changedistpolicy_dml_pttab_char ORDER BY 1,2,3;

