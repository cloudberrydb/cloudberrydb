set extra_float_digits = -1;
set default_table_access_method=ao_column;
set vector.enable_vectorization = on;

ANALYZE;
explain (costs off) select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				('13', '31', '23', '29', '30', '18', '17')
			and c_acctbal > (
				select
						avg(c_acctbal)
				from
						customer
				where
						c_acctbal > 0.00
						and substring(c_phone from 1 for 2) in
								('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists (
				select
						*
				from
						orders
				where
						o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;	

select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				('13', '31', '23', '29', '30', '18', '17')
			and c_acctbal > (
				select
						avg(c_acctbal)
				from
						customer
				where
						c_acctbal > 0.00
						and substring(c_phone from 1 for 2) in
								('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists (
				select
						*
				from
						orders
				where
						o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;	
