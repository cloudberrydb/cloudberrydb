select  'tpch19',
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#23'
		and (p_container = 'SM CASE' or  p_container = 'SM BOX' or p_container = 'SM PACK' or p_container = 'SM PKG')
		and l_quantity >= 3 and l_quantity <= 3 + 10
		and p_size between 1 and 5
		and (l_shipmode = 'AIR' or l_shipmode = 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#53'
		and (p_container = 'MED BAG' or p_container = 'MED BOX' or p_container = 'MED PKG' or p_container = 'MED PACK')
		and l_quantity >= 11 and l_quantity <= 11 + 10
		and p_size between 1 and 10
		and (l_shipmode = 'AIR' or l_shipmode = 'AIR REG') 
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#21'
		and (p_container = 'LG CASE' or p_container = 'LG BOX' or p_container = 'LG PACK' or p_container = 'LG PKG')
		and l_quantity >= 29 and l_quantity <= 29 + 10
		and p_size between 1 and 15
		and (l_shipmode = 'AIR' or l_shipmode = 'AIR REG') 
		and l_shipinstruct = 'DELIVER IN PERSON'
	);
