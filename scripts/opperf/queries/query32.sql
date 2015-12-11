-- stats functions, min, max, stddev and variance

select min(l_quantity), max(l_tax), stddev(l_tax), variance(l_tax) 
	from lineitem;
