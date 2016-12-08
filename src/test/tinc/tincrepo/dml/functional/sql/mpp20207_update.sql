-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
-- @gpopt 1.532
-- @description Mpp-20207
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
ALTER TABLE altable DROP COLUMN b;

ALTER TABLE altable ADD CONSTRAINT c_check CHECK (c > 0);
INSERT INTO altable(a, c) VALUES(0, 10);
SELECT * FROM altable ORDER BY 1;
UPDATE altable SET c = -10;
SELECT * FROM altable ORDER BY 1;
UPDATE altable SET c = 1;
SELECT * FROM altable ORDER BY 1;
