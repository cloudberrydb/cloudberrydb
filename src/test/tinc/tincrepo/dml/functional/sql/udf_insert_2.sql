-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test2: UDF with Insert within transaction
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP FUNCTION IF EXISTS dml_insertdata(integer);
CREATE OR REPLACE FUNCTION dml_insertdata (startvalue INTEGER) RETURNS VOID
AS
$$
DECLARE
   i INTEGER;
BEGIN
   i = startvalue;
    EXECUTE 'INSERT INTO dml_plpgsql_t2(a) VALUES (' || i || ')';
END;
$$
LANGUAGE PLPGSQL;
DROP TABLE IF EXISTS dml_plpgsql_t2;
CREATE TABLE dml_plpgsql_t2( a int ) distributed by (a);
BEGIN;
select dml_insertdata(1);
select dml_insertdata(2);
select dml_insertdata(3);
select dml_insertdata(4);
select dml_insertdata(5);
select dml_insertdata(6);
select dml_insertdata(7);
select dml_insertdata(8);
select dml_insertdata(9);
select dml_insertdata(10);
COMMIT;
SELECT COUNT(*) FROM dml_plpgsql_t2;
