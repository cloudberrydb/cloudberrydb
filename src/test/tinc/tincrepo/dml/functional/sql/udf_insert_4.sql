-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test4: Negative test - UDF with Insert
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
-- start_ignore
DROP FUNCTION IF EXISTS insert_wrong();
-- end_ignore
CREATE or REPLACE FUNCTION insert_wrong() RETURNS void as $$
BEGIN
INSERT INTO  errorhandlingtmpTABLE VALUES ('fjdk');
END;
$$ language plpgsql;
DROP FUNCTION IF EXISTS dml_plperl_fn2 (int,int);
CREATE or REPLACE FUNCTION dml_plperl_fn2 (st int,en int) returns void as $$
DECLARE
i integer;
begin
  i=st;
  while i <= en LOOP
    perform insert_wrong();
    i = i + 1;
  END LOOP;
end;
$$ LANGUAGE 'plpgsql';
DROP TABLE IF EXISTS dml_plperl_t2;
CREATE TABLE dml_plperl_t2(i int) distributed by (i);
SELECT dml_plperl_fn2(1,10);
SELECT COUNT(*) FROM dml_plperl_t2;
