-- Scan test2:
-- select several columns from lineitem
select max(length(l_linenumber) + length(l_linestatus) + length(l_comment))
from lineitem;

