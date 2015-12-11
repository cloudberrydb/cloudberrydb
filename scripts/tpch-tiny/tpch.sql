-- This script dumps query results of GPDB and ORCA for rewitten TPC-H queries (no subqueries, and using derived tables to replace some joins)
 
-- Q1
select  'tpch1' as tpch1,
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
 where
	l_shipdate <= date '1998-12-01' - interval '108 day'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;


select gpoptutils.RestorePlanDXL(
gpoptutils.Optimize(' select   l_returnflag, l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date ''1998-12-01'' - interval ''108 day''
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus 
	')
) as TPCH1;



-- Q2


select  'tpch2' as tpch2,
	s.s_acctbal,
	s.s_name,
	n.n_name,
	p.p_partkey,
	p.p_mfgr,
	s.s_address,
	s.s_phone,
	s.s_comment 
from 
	supplier s, 
	partsupp ps, 
	nation n, 
	region r, 
	part p, 
	(select p_partkey, min(ps_supplycost) as min_ps_cost 
		from 
			part, 
			partsupp , 	
			supplier,
			nation, 
			region 
		where 
			p_partkey=ps_partkey 
			and s_suppkey = ps_suppkey 
			and s_nationkey = n_nationkey 
			and n_regionkey = r_regionkey 
			and r_name = 'EUROPE' 
		group by p_partkey ) g 
where 
	p.p_partkey = ps.ps_partkey 
	and g.p_partkey = p.p_partkey 
	and g. min_ps_cost = ps.ps_supplycost 
	and s.s_suppkey = ps.ps_suppkey 
	and p.p_size = 45 
	and p.p_type like '%NICKEL' 
	and s.s_nationkey = n.n_nationkey 
	and n.n_regionkey = r.r_regionkey 
	and r.r_name = 'EUROPE' 
order by 
	s.s_acctbal desc,
	n.n_name,
	s.s_name,
	p.p_partkey
LIMIT 100;


select gpoptutils.RestorePlanDXL(
gpoptutils.Optimize('  select s.s_acctbal,
	s.s_name,
	n.n_name,
	p.p_partkey,
	p.p_mfgr,
	s.s_address,
	s.s_phone,
	s.s_comment 
from 
	supplier s, 
	partsupp ps, 
	nation n, 
	region r, 
	part p, 
	(select p_partkey, min(ps_supplycost) as min_ps_cost 
		from 
			part, 
			partsupp , 	
			supplier,
			nation, 
			region 
		where 
			p_partkey=ps_partkey 
			and s_suppkey = ps_suppkey 
			and s_nationkey = n_nationkey 
			and n_regionkey = r_regionkey 
			and r_name = ''EUROPE''
		group by p_partkey ) g 
where 
	p.p_partkey = ps.ps_partkey 
	and g.p_partkey = p.p_partkey 
	and g. min_ps_cost = ps.ps_supplycost 
	and s.s_suppkey = ps.ps_suppkey 
	and p.p_size = 45 
	and p.p_type like ''%NICKEL''
	and s.s_nationkey = n.n_nationkey 
	and n.n_regionkey = r.r_regionkey 
	and r.r_name = ''EUROPE''
order by 
	s.s_acctbal desc,
	n.n_name,
	s.s_name,
	p.p_partkey
LIMIT 100 ')
) as TPCH2; 


-- Q3

select  'tpch3' as tpch3,
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'MACHINERY'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
LIMIT 10;


select gpoptutils.RestorePlanDXL(
gpoptutils.Optimize('   select * from (
select l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = ''MACHINERY''
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date ''1995-03-15''
	and l_shipdate > date ''1995-03-15''
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
)  as foo
order by
	revenue desc,
	o_orderdate
LIMIT 10 ')
) as TPCH3;



-- Q4

SELECT 'tpch4' as tpch4,
       o_orderpriority,
       count(*) AS order_count
FROM orders
INNER JOIN
  (SELECT l_orderkey
   FROM lineitem
   WHERE l_commitdate < l_receiptdate
   GROUP BY l_orderkey) l ON (o_orderkey = l.l_orderkey)
WHERE o_orderdate >= date '1994-05-01'
  AND o_orderdate < date '1994-05-01' + interval '3 month'
GROUP BY o_orderpriority
ORDER BY o_orderpriority;


select gpoptutils.RestorePlanDXL(
gpoptutils.Optimize(' SELECT  o_orderpriority,
       count(*) AS order_count
FROM orders
INNER JOIN
  (SELECT l_orderkey
   FROM lineitem
   WHERE l_commitdate < l_receiptdate
   GROUP BY l_orderkey) l ON (o_orderkey = l.l_orderkey)
WHERE o_orderdate >= date ''1994-05-01''
  AND o_orderdate < date ''1994-05-01'' + interval ''3 month''
GROUP BY o_orderpriority
ORDER BY o_orderpriority ')
) as TPCH4;



-- Q5


select  'tpch5' as tpch5,
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AMERICA'
	and o_orderdate >= date '1997-01-01'
	and o_orderdate < date '1997-01-01' + interval '1 year'
group by
	n_name
order by
	revenue desc;



select gpoptutils.RestorePlanDXL(
gpoptutils.Optimize('  select * from (
select n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = ''AMERICA''
	and o_orderdate >= date ''1997-01-01''
	and o_orderdate < date ''1997-01-01'' + interval ''1 year''	
group by
	n_name
)  as foo	
order by
	revenue desc ')
) as TPCH5;



-- Q6


select  'tpch6' as tpch6,
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1996-01-01'
	and l_shipdate < date '1996-01-01' + interval '1 year'
	and l_discount between 0.04 - 0.01 and 0.04 + 0.01
	and l_quantity < 24;



select gpoptutils.RestorePlanDXL( 
 gpoptutils.Optimize('select   sum(l_extendedprice * l_discount) as revenue
 from
 	lineitem
 where
 	l_shipdate >= date ''1996-01-01''
 	and l_shipdate < date ''1996-01-01'' + interval ''1 year''
 	and l_discount between 0.04 - 0.01 and 0.04 + 0.01
 	and l_quantity < 24 ') 
 ) as TPCH6;



-- Q7


select  'tpch7' as tpch7,
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'GERMANY')
				or (n1.n_name = 'GERMANY' and n2.n_name = 'MOZAMBIQUE')
			)
			and l_shipdate between date '1995-01-01' and date '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;



select gpoptutils.RestorePlanDXL(
gpoptutils.Optimize(' select   supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = ''MOZAMBIQUE'' and n2.n_name = ''GERMANY'')
				or (n1.n_name = ''GERMANY'' and n2.n_name = ''MOZAMBIQUE'')
			)
			and l_shipdate between date ''1995-01-01'' and date ''1996-12-31''
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year ') 
) as TPCH7;



-- Q8



select  'tpch8' as tpch8,
	o_year,
	sum(case
		when nation = 'PERU' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'AMERICA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date '1995-01-01' and date '1996-12-31'
			and p_type = 'MEDIUM POLISHED TIN'
	) as all_nations
group by
	o_year
order by
	o_year;

	
select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select  o_year, sum(case when nation = ''PERU'' then volume else 0 end) / sum(volume) as mkt_share
from
	(select extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation
	from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = ''AMERICA''
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date ''1995-01-01'' and date ''1996-12-31''
			and p_type = ''MEDIUM POLISHED T-bone'' ) as all_nations group by o_year order by o_year ')
) as TPCH8;


-- Q9


select  'tpch9' as tpch9,
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%tan%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;




select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select  nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like ''%tan%''
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc ')
) as TPCH9;	


-- Q10


select  'tpch10' as tpch10,
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date '1994-10-01'
	and o_orderdate < date '1994-10-01' + interval '3 month'
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
LIMIT 20;



select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select * from (
select  c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= date ''1994-10-01''
	and o_orderdate < date ''1994-10-01'' + interval ''3 month''
	and l_returnflag = ''R''
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
) as foo	
order by
	revenue desc
LIMIT 20 ')
) as TPCH10;


-- Q11


SELECT 'tpch11' as tpch11,  pv.ps_partkey, pv.value
FROM
  ( SELECT ps_partkey,
           sum(ps_supplycost * ps_availqty) AS value
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'ROMANIA'
   GROUP BY ps_partkey ) pv,
  ( SELECT sum(ps_supplycost * ps_availqty) * 0.0001000000 AS value
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = 'ROMANIA') r
WHERE pv.value > r.value
ORDER BY value DESC ;



select gpoptutils.RestorePlanDXL(
gpoptutils.Optimize(' SELECT   pv.ps_partkey, pv.value
FROM
  ( SELECT ps_partkey,
           sum(ps_supplycost * ps_availqty) AS value
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = ''ROMANIA''
   GROUP BY ps_partkey ) pv,
  ( SELECT sum(ps_supplycost * ps_availqty) * 0.0001000000 AS value
   FROM partsupp,
        supplier,
        nation
   WHERE ps_suppkey = s_suppkey
     AND s_nationkey = n_nationkey
     AND n_name = ''ROMANIA'') r
WHERE pv.value > r.value
ORDER BY value DESC ')
) as TPCH11;


-- Q12


select  'tpch12' as tpch12,
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('RAIL', 'MAIL')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date '1993-01-01'
	and l_receiptdate < date '1993-01-01' + interval '1 year'
group by
	l_shipmode
order by
	l_shipmode;


select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select  l_shipmode,
	sum(case
		when o_orderpriority = ''1-URGENT''
			or o_orderpriority = ''2-HIGH''
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> ''1-URGENT''
			and o_orderpriority <> ''2-HIGH''
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and ( l_shipmode = ''RAIL''  or   l_shipmode =   ''MAIL'')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date ''1993-01-01''
	and l_receiptdate < date ''1993-01-01'' + interval ''1 year''
group by
	l_shipmode
order by
	l_shipmode ')
) as TPCH12;


-- Q13


select  'tpch13' as tpch13,
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%express%deposits%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc;



select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select * from (
select c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like ''%express%deposits%''
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
) as foo	
order by
	custdist desc,
	c_count desc
')
) as TPCH13;


-- Q14


select  100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as tpch14
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1993-01-01'
	and l_shipdate < date '1993-01-01' + interval '1 month';



select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select  100.00 * sum(case
		when p_type like ''PROMO%''
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date ''1993-01-01''
	and l_shipdate < date ''1993-01-01'' + interval ''1 month'' ')
) as TPCH14;


-- Q15


SELECT 'tpch15' as tpch15,
       s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM supplier,
     revenue0,
  (SELECT max(total_revenue) AS max_total
   FROM revenue0) AS maxrev0
WHERE s_suppkey = supplier_no
  AND total_revenue = maxrev0.max_total
ORDER BY s_suppkey;



select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' SELECT  s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM supplier,
     revenue0,
  (SELECT max(total_revenue) AS max_total
   FROM revenue0) AS maxrev0
WHERE s_suppkey = supplier_no
  AND total_revenue = maxrev0.max_total
ORDER BY s_suppkey ')
) as TPCH15;


-- Q16


select 'tpch16' as tpch16,
	p_brand, 
	p_type, 
	p_size,
	count(*) as supplier_cnt 
from 
(
select 
	p_brand, 
	p_type, 
	p_size, 
	ps_suppkey 
from 
	part, 
	partsupp left join supplier on (ps_suppkey=s_suppkey and s_comment like '%Customer%Complaints%' ) 
where 
	p_partkey = ps_partkey 
	and p_brand <> 'Brand#35' 
	and p_type not like 'MEDIUM ANODIZED%' 
--	and p_size in (39, 31, 24, 22, 46, 20, 42, 15)
	and (p_size = 39 or p_size = 31 or p_size = 24 or p_size = 22 or p_size = 46 or p_size = 20 or p_size = 42 or p_size = 15)
	and s_suppkey is null 
group by 
	p_brand, 
	p_type, 
	p_size,
	ps_suppkey
) foo

group by 
	p_brand, 
	p_type, 
	p_size 
order by 
	supplier_cnt desc, 
	p_brand, 
	p_type, 
	p_size;



select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select * from (
select p_brand, 
	p_type, 
	p_size,
	count(*) as supplier_cnt 
from 
(
select 
	p_brand, 
	p_type, 
	p_size, 
	ps_suppkey 
from 
	part, 
	partsupp left join supplier on (ps_suppkey=s_suppkey and s_comment like ''%Customer%Complaints%'') 
where 
	p_partkey = ps_partkey 
	and p_brand <> ''Brand#35''
	and p_type not like ''MEDIUM ANODIZED%''
	and (p_size = 39 or p_size = 31 or p_size = 24 or p_size = 22 or p_size = 46 or p_size = 20 or p_size = 42 or p_size = 15)
	and s_suppkey is null 
group by 
	p_brand, 
	p_type, 
	p_size,
	ps_suppkey
) foo

group by 
	p_brand, 
	p_type, 
	p_size 
) as foo	
order by 
	supplier_cnt desc, 
	p_brand, 
	p_type, 
	p_size ')
) as TPCH16;


-- Q17


select 'tpch17' as tpch17,
      sum(l_extendedprice) / 7.0 as avg_yearly
from
      lineitem,
      (
          select
              p_partkey as x_partkey,
              0.2 * avg(l_quantity) as x_avg_20
          from
              part,
              lineitem
          where
              p_partkey = l_partkey
              and p_brand = 'Brand#32'
              and p_container = 'JUMBO PKG'
          group by
              p_partkey
      ) x
where
      x_partkey = l_partkey
and l_quantity < x_avg_20;




select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select sum(l_extendedprice) / 7.0 as avg_yearly
from
      lineitem,
      (
          select
              p_partkey as x_partkey,
              0.2 * avg(l_quantity) as x_avg_20
          from
              part,
              lineitem
          where
              p_partkey = l_partkey
              and p_brand = ''Brand#32''
              and p_container = ''JUMBO PKG''
          group by
              p_partkey
      ) x
where
      x_partkey = l_partkey
and l_quantity < x_avg_20 ')
) as TPCH17;


-- Q18


SELECT 'tpch18' as tpch18,
       c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
FROM customer, orders, lineitem l,
  (
  select ll1.l_orderkey
  from
  ( SELECT l_orderkey, sum(l_quantity) as q
   FROM lineitem
   GROUP BY l_orderkey) ll1
   WHERE ll1.q > 313) ll
WHERE o_orderkey = ll.l_orderkey
  AND c_custkey = o_custkey
  AND o_orderkey = l.l_orderkey
GROUP BY c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;




select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' SELECT  c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       sum(l_quantity)
FROM customer, orders, lineitem l,
  (
  select ll1.l_orderkey
  from
  ( SELECT l_orderkey, sum(l_quantity) as q
   FROM lineitem
   GROUP BY l_orderkey) ll1
   WHERE ll1.q > 313) ll
WHERE o_orderkey = ll.l_orderkey
  AND c_custkey = o_custkey
  AND o_orderkey = l.l_orderkey
GROUP BY c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
ORDER BY o_totalprice DESC, o_orderdate LIMIT 100 ')
) as TPCH18;


-- Q19


select  'tpch19' as tpch19,
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




select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select  sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = ''Brand#23''
		and (p_container = ''SM CASE'' or  p_container = ''SM BOX'' or p_container = ''SM PACK'' or p_container = ''SM PKG'')
		and l_quantity >= 3 and l_quantity <= 3 + 10
		and p_size between 1 and 5
		and (l_shipmode = ''AIR'' or l_shipmode = ''AIR REG'')
		and l_shipinstruct = ''DELIVER IN PERSON''
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = ''Brand#53''
		and (p_container = ''MED BAG'' or p_container = ''MED BOX'' or p_container = ''MED PKG'' or p_container = ''MED PACK'')
		and l_quantity >= 11 and l_quantity <= 11 + 10
		and p_size between 1 and 10
		and (l_shipmode = ''AIR'' or l_shipmode = ''AIR REG'') 
		and l_shipinstruct = ''DELIVER IN PERSON''
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = ''Brand#21''
		and (p_container = ''LG CASE'' or p_container = ''LG BOX'' or p_container = ''LG PACK'' or p_container = ''LG PKG'')
		and l_quantity >= 29 and l_quantity <= 29 + 10
		and p_size between 1 and 15
		and (l_shipmode = ''AIR'' or l_shipmode = ''AIR REG'') 
		and l_shipinstruct = ''DELIVER IN PERSON''
	) ')
) as TPCH19;

-- Q20


select  'tpch20' as tpch20, s_name, s_address 
from 
	supplier inner join ( 
		select 
			ps_suppkey 
		from 
			partsupp, 
			( 
				select 
					sum(l_quantity) as qty_sum, l_partkey, l_suppkey 
				from 
					lineitem 
				where 
					l_shipdate >= date '1996-01-01'
					and l_shipdate < date '1996-01-01' + interval '1 year'
				group by l_partkey, l_suppkey ) g,
			(       select p_partkey from part where p_name like 'medium%' group by p_partkey ) m 
		where 
			g.l_partkey = ps_partkey 
			and g.l_suppkey = ps_suppkey 
			and ps_availqty > 0.5 * g.qty_sum 
			and ps_partkey = m.p_partkey
		group by
		      ps_suppkey
		) foo
	on (s_suppkey = foo.ps_suppkey), 
	nation 
where 
	s_nationkey = n_nationkey 
	and n_name = 'IRAQ'
order by 
	s_name ;


select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select  s_name, s_address 
from 
	supplier inner join ( 
		select 
			ps_suppkey 
		from 
			partsupp, 
			( 
				select 
					sum(l_quantity) as qty_sum, l_partkey, l_suppkey 
				from 
					lineitem 
				where 
					l_shipdate >= date ''1996-01-01''
					and l_shipdate < date ''1996-01-01'' + interval ''1 year''
				group by l_partkey, l_suppkey ) g,
			(       select p_partkey from part where p_name like ''medium%'' group by p_partkey ) m 
		where 
			g.l_partkey = ps_partkey 
			and g.l_suppkey = ps_suppkey 
			and ps_availqty > 0.5 * g.qty_sum 
			and ps_partkey = m.p_partkey
		group by
		      ps_suppkey
		) foo
	on (s_suppkey = foo.ps_suppkey), 
	nation 
where 
	s_nationkey = n_nationkey 
	and n_name = ''IRAQ''
order by 
	s_name  ')
) as TPCH20;



-- Q21


select  'tpch21' as tpch21,
	s_name, 
	count(*) as numwait 

from
(
select 
	s_name
from 
	supplier, 
	orders, 
	nation, 
	lineitem l1 
		left join lineitem l2 
			on (l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) 
		left join (
			select 
				l3.l_orderkey,
				l3.l_suppkey 
			from 
				lineitem l3 
			where 
				l3.l_receiptdate > l3.l_commitdate) l4 
			on (l4.l_orderkey = l1.l_orderkey and l4.l_suppkey <> l1.l_suppkey) 
where 
	s_suppkey = l1.l_suppkey 
	and o_orderkey = l1.l_orderkey 
	and o_orderstatus = 'F' 
	and l1.l_receiptdate > l1.l_commitdate 
	and l2.l_orderkey is not null 
	and l4.l_orderkey is null 
	and s_nationkey = n_nationkey 
	and n_name = 'MOZAMBIQUE' 
group by 
	s_name,
	(l1.l_orderkey::text || l1.l_linenumber::text)
) foo

group by 
	s_name 

order by 
	numwait desc, 
	s_name
LIMIT 100;



select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize(' select * from (
select   s_name, 
	count(*) as numwait 
from
(
select 
	s_name
from 
	supplier, 
	orders, 
	nation, 
	lineitem l1 
		left join lineitem l2 
			on (l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) 
		left join (
			select 
				l3.l_orderkey,
				l3.l_suppkey 
			from 
				lineitem l3 
			where 
				l3.l_receiptdate > l3.l_commitdate) l4 
			on (l4.l_orderkey = l1.l_orderkey and l4.l_suppkey <> l1.l_suppkey) 
where 
	s_suppkey = l1.l_suppkey 
	and o_orderkey = l1.l_orderkey 
	and o_orderstatus = ''F''
	and l1.l_receiptdate > l1.l_commitdate 
	and l2.l_orderkey is not null 
	and l4.l_orderkey is null 
	and s_nationkey = n_nationkey 
	and n_name = ''MOZAMBIQUE''
group by 
	s_name,
	(l1.l_orderkey::text || l1.l_linenumber::text)
) foo

group by 
	s_name 
) as bar
order by 
	numwait desc, 
	s_name
LIMIT 100 ')
) as TPCH21;


-- Q22



select  'tpch22' as tpch22,
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
				on (c_custkey = o_custkey),
			( select 
				avg(c_acctbal) as avg_bal
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
			)  avg_cust 
		where 
			(substring(c_phone from 1 for 2) = '11' or
			 substring(c_phone from 1 for 2) = '28' or
			 substring(c_phone from 1 for 2) = '21' or
			 substring(c_phone from 1 for 2) = '26' or
			 substring(c_phone from 1 for 2) = '19' or
			 substring(c_phone from 1 for 2) =  '13' or
			 substring(c_phone from 1 for 2) = '22') 
			and c_acctbal > avg_cust.avg_bal
			and o_custkey is null 
	) as custsale 
group by 
	cntrycode 
order by 
	cntrycode;



select gpoptutils.RestorePlanDXL(	
gpoptutils.Optimize('select  cntrycode, 
	count(*) as numcust, 
	sum(c_acctbal) as totacctbal 
from 
	( 
		select 
			substring(c_phone from 1 for 2) as cntrycode, 
			c_acctbal 
		from 
			customer left join orders 
				on (c_custkey = o_custkey),
			( select 
				avg(c_acctbal) as avg_bal
			from 
				customer 
			where 
				c_acctbal > 0.00 
				and (substring(c_phone from 1 for 2) = ''11'' or
				substring(c_phone from 1 for 2) = ''28'' or
				substring(c_phone from 1 for 2) = ''21'' or
				substring(c_phone from 1 for 2) = ''26'' or
				substring(c_phone from 1 for 2) = ''19'' or
				substring(c_phone from 1 for 2) = ''13'' or
				substring(c_phone from 1 for 2) = ''22'') 
			)  avg_cust 
		where 
			(substring(c_phone from 1 for 2) = ''11'' or
			 substring(c_phone from 1 for 2) = ''28'' or
			 substring(c_phone from 1 for 2) = ''21'' or
			 substring(c_phone from 1 for 2) = ''26'' or
			 substring(c_phone from 1 for 2) = ''19'' or
			 substring(c_phone from 1 for 2) =  ''13'' or
			 substring(c_phone from 1 for 2) = ''22'') 
			and c_acctbal > avg_cust.avg_bal
			and o_custkey is null 
	) as custsale 
group by 
	cntrycode 
order by 
	cntrycode ')
) as TPCH22;

