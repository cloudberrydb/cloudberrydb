-- @description function_in_select_constant_withfunc2_147.sql
-- @db_name functionproperty
-- @author tungs1
-- @modified 2013-04-03 12:00:00
-- @created 2013-04-03 12:00:00
-- @tags functionProperties 
SELECT func1_mod_int_stb(func2_read_int_stb(5)) FROM foo order by 1; 
