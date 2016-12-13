--start_ignore
-- @author tungs1
-- @modified 2013-07-17 12:00:00
-- @created 2013-07-17 12:00:00
-- @description SplitDQA lineitem1_SK2_1_SUM.sql
-- @db_name splitdqa
-- @tags SplitAgg HAWQ
-- @executemode enumerate
--end_ignore
SELECT SUM(DISTINCT l_partkey) AS DQA1_dqacol1 FROM lineitem1  GROUP BY l_suppkey;
