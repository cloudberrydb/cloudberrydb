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
DROP TABLE IF EXISTS volatilefn_dml_interval;
CREATE TABLE volatilefn_dml_interval
(
    col1 interval DEFAULT '11 hours',
    col2 interval DEFAULT '11 hours',
    col3 int,
    col4 interval DEFAULT '11 hours'
) 
DISTRIBUTED by (col1)
PARTITION BY LIST(col2)
(
default partition def 
);
DROP TABLE IF EXISTS volatilefn_dml_interval_candidate;
CREATE TABLE volatilefn_dml_interval_candidate
(
    col1 interval DEFAULT '11 hours',
    col2 interval DEFAULT '11 hours',
    col3 int,
    col4 interval 
) 
DISTRIBUTED by (col2);
INSERT INTO volatilefn_dml_interval_candidate(col3,col4) VALUES(10,'10 secs');

-- Create volatile UDF
CREATE OR REPLACE FUNCTION func(x int) RETURNS int AS $$
BEGIN

DROP TABLE IF EXISTS foo;
CREATE TABLE foo (a int, b int) distributed by (a);
INSERT INTO foo select i, i+1 from generate_series(1,10) i;

DROP TABLE IF EXISTS bar;
CREATE TABLE bar (c int, d int) distributed by (c);
INSERT INTO bar select i, i+1 from generate_series(1,10) i;

UPDATE bar SET d = d+1 WHERE c = $1;
RETURN $1 + 1;
END
$$ LANGUAGE plpgsql VOLATILE MODIFIES SQL DATA;

INSERT INTO volatilefn_dml_interval(col1,col3) SELECT col2,func(14) FROM volatilefn_dml_interval_candidate;
SELECT * FROM volatilefn_dml_interval ORDER BY 1,2,3;

UPDATE volatilefn_dml_interval SET col3 = (SELECT func(1));
SELECT * FROM volatilefn_dml_interval ORDER BY 1,2,3;

DELETE FROM volatilefn_dml_interval WHERE col3 = (SELECT func(1));
SELECT * FROM volatilefn_dml_interval ORDER BY 1,2,3;

