--start_ignore
-- @author tungs1
-- @modified 2013-07-17 12:00:00
-- @created 2013-07-17 12:00:00
-- @description SplitDQA lineitem1_SK7_1_12.sql
-- @db_name splitdqa
-- @tags SplitAgg HAWQ
-- @executemode normal
--end_ignore
SELECT * FROM (SELECT SUM(DISTINCT l_partkey) AS DQA1_dqacol1 FROM lineitem1  GROUP BY l_suppkey) as t1, (SELECT COUNT(DISTINCT p_size) AS DQA2_dqacol1 FROM part1 ) as t2 WHERE t1.DQA1_dqacol1 = t2.DQA2_dqacol1;
