-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test1: UDF with Insert
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
-- start_ignore
DROP FUNCTION IF EXISTS insert_correct();
-- end_ignore
CREATE or REPLACE FUNCTION  insert_correct () RETURNS void as $$
    plpy.execute('INSERT INTO  dml_plperl_t1 VALUES (1)');
    plpy.execute('INSERT INTO  dml_plperl_t1 VALUES (2)');
    plpy.execute('INSERT INTO  dml_plperl_t1 VALUES (4)');
    return;
$$ language plpythonu;
DROP FUNCTION IF EXISTS dml_plperl_fn1 (int,int);
CREATE or REPLACE FUNCTION dml_plperl_fn1 (st int,en int) returns void as $$
DECLARE
i integer;
begin
  i=st;
  while i <= en LOOP
    perform insert_correct();
    i = i + 1;
  END LOOP;
end;
$$ LANGUAGE 'plpgsql';
DROP TABLE IF EXISTS dml_plperl_t1;
CREATE TABLE dml_plperl_t1 ( i int) distributed by (i);
SELECT dml_plperl_fn1(1,10);
SELECT COUNT(*) FROM dml_plperl_t1;
