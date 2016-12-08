-- @description function_in_select_constant_withfunc2_136.sql
-- @db_name functionproperty
-- @author tungs1
-- @modified 2013-04-03 12:00:00
-- @created 2013-04-03 12:00:00
-- @tags functionProperties 
SELECT func1_mod_int_vol(func2_read_int_vol(5)) FROM foo order by 1; 
