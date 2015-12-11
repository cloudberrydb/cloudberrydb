select  'tpch22',
	cntrycode, 
	count(*) as numcust, 
	sum(c_acctbal) as totacctbal 
from 
	( 
		select 
			substring(c_phone from 1 for 2) as cntrycode, 
			c_acctbal 
		from 
			customer left join orders 
				on c_custkey = o_custkey 
		where 
			(substring(c_phone from 1 for 2) = '11' or
			 substring(c_phone from 1 for 2) = '28' or
			 substring(c_phone from 1 for 2) = '21' or
			 substring(c_phone from 1 for 2) = '26' or
			 substring(c_phone from 1 for 2) = '19' or
			 substring(c_phone from 1 for 2) =  '13' or
			 substring(c_phone from 1 for 2) = '22') 
			and c_acctbal > ( 
				select 
					avg(c_acctbal) 
				from 
					customer 
				where 
					c_acctbal > 0.00 
					and (substring(c_phone from 1 for 2) = '11' or
					substring(c_phone from 1 for 2) = '28' or
					substring(c_phone from 1 for 2) = '21' or
					substring(c_phone from 1 for 2) = '26' or
					substring(c_phone from 1 for 2) = '19' or
					substring(c_phone from 1 for 2) = '13' or
					substring(c_phone from 1 for 2) = '22') 
			) 
			and o_custkey is null 
	) as custsale 
group by 
	cntrycode 
order by 
	cntrycode;
