-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @description test3: UDF with Insert within transaction
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP FUNCTION IF EXISTS dml_fn2(int);
CREATE OR REPLACE FUNCTION dml_fn2(x int) RETURNS INT as $$
for i in range(0, x):
  plpy.execute('INSERT INTO dml_plpython_t2 values(%d)' % i);
return plpy.execute('SELECT COUNT(*) as a FROM dml_plpython_t2')[0]["a"]
$$ language plpythonu;
DROP TABLE IF EXISTS dml_plpython_t2;
CREATE TABLE dml_plpython_t2(a int) DISTRIBUTED randomly;
BEGIN;
SELECT dml_fn2(20);
ROLLBACK;
BEGIN;
SELECT dml_fn2(10);
COMMIT;
SELECT COUNT(*) FROM dml_plpython_t2;
