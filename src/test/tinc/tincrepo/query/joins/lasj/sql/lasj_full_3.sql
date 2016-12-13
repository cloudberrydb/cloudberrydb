-- @author ramans2
-- @created 2013-07-05 12:00:00 
-- @modified 2013-07-05 12:00:00
-- @tags lasj HAWQ
-- @db_name lasjdb
-- @product_version gpdb: [4.3.0.0-9.9.99.99] , hawq: [1.1.4.0-9.9.99.99]
-- @description OPT-3351: Add test cases for LASJ

-- disable hash join
SELECT disable_xform('CXformLeftAntiSemiJoin2HashJoin');

-- non-empty outer, non-empty inner
SELECT * FROM (SELECT * FROM foo WHERE b = 2) foo2 FULL OUTER JOIN (SELECT * FROM bar WHERE y BETWEEN 16 AND 22 OR x IS NULL) bar2 ON (a = x);
