-- select into

select * into tmplineitem from lineitem
where l_orderkey % 20 = 1;
