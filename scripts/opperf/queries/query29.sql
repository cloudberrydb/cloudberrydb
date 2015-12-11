-- update

update tmplineitem set l_linestatus = 'O'
where l_suppkey % 3 = 1;
