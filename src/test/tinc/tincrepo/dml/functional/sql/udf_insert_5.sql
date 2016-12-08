-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test5: Negative test - UDF with Insert. Different data type
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP FUNCTION IF EXISTS dml_insertvalue(integer);
CREATE OR REPLACE FUNCTION dml_insertvalue (inp integer) RETURNS VOID
AS
$$
DECLARE
BEGIN
     EXECUTE 'INSERT INTO dml_plpgsql_t1(a) VALUES (%)' , i;
EXCEPTION
    WHEN others THEN
    RAISE NOTICE 'Error in data';
END;
$$
LANGUAGE PLPGSQL;
DROP FUNCTION IF EXISTS dml_indata(integer,integer);
CREATE OR REPLACE FUNCTION dml_indata (startvalue integer, endvalue integer) RETURNS VOID
AS
$$
DECLARE
   i INTEGER;
BEGIN
   i = startvalue;
   WHILE i <= endvalue LOOP
       PERFORM dml_insertvalue(100);
       i = i + 1;
   END LOOP;
END;
$$
LANGUAGE PLPGSQL;
DROP TABLE IF EXISTS dml_plpgsql_t1;
CREATE TABLE dml_plpgsql_t1(a char) distributed by (a);
SELECT dml_indata(1,10);
SELECT COUNT(*) FROM dml_plpgsql_t1;
