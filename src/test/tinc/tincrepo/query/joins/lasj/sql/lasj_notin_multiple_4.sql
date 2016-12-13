-- @author ramans2
-- @created 2013-07-05 12:00:00 
-- @modified 2013-07-05 12:00:00
-- @tags lasj HAWQ
-- @db_name lasjdb
-- @product_version gpdb: [4.3.0.0-9.9.99.99] , hawq: [1.1.4.0-9.9.99.99]
-- @description OPT-3351: Add test cases for LASJ

-- disable hash join
SELECT disable_xform('CXformLeftAntiSemiJoin2HashJoin');

-- outer with nulls, inner with partial nulls
SELECT * FROM foo WHERE (a, b) NOT IN (SELECT x, y FROM bar WHERE y IS NOT NULL);
