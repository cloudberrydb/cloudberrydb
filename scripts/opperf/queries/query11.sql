-- scan test with selection
-- Select one column from lineitem

select max(l_partkey) from lineitem where l_quantity > 20 and l_discount < 0.9 ;
