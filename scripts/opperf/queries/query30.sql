-- delete

delete from tmplineitem 
where l_partkey % 4 = 1;
