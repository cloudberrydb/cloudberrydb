-- @author prabhd 
-- @created 2012-12-05 12:00:00 
-- @modified 2012-12-05 12:00:00 
-- @tags dml HAWQ
-- @db_name dmldb
-- @description MPP-20083
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SELECT COUNT(*) 
FROM r, s c, s d, s e
WHERE r.a = c.c AND r.a = d.d AND r.a = e.e;

SELECT COUNT(*)
FROM r_p, s c, s d, s e
WHERE r_p.a = c.c AND r_p.a = d.d AND r_p.a = e.e;
