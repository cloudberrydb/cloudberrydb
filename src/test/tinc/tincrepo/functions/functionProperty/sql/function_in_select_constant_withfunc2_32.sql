-- @description function_in_select_constant_withfunc2_32.sql
-- @db_name functionproperty
-- @author tungs1
-- @modified 2013-04-03 12:00:00
-- @created 2013-04-03 12:00:00
-- @tags functionProperties 
SELECT func1_sql_int_vol(func2_nosql_imm(5)) FROM foo order by 1; 
