DROP TABLE REGION;

CREATE TABLE REGION ( R_REGIONKEY  INTEGER NOT NULL, R_NAME CHAR(25) NOT NULL, R_COMMENT VARCHAR(152));

DROP TABLE NATION;

CREATE TABLE NATION ( N_NATIONKEY  INTEGER NOT NULL, N_NAME CHAR(25) NOT NULL, N_REGIONKEY  INTEGER NOT NULL, N_COMMENT    VARCHAR(152));

DROP TABLE SUPPLIER; 

CREATE TABLE SUPPLIER (S_SUPPKEY INTEGER NOT NULL, S_NAME CHAR(25) NOT NULL, S_ADDRESS VARCHAR(40) NOT NULL, S_NATIONKEY INTEGER NOT NULL, S_PHONE CHAR(15) NOT NULL, S_ACCTBAL DECIMAL(15,2) NOT NULL, S_COMMENT VARCHAR(101) NOT NULL);

DROP TABLE PARTSUPP;

CREATE TABLE PARTSUPP ( PS_PARTKEY INTEGER NOT NULL, PS_SUPPKEY INTEGER NOT NULL, PS_AVAILQTY INTEGER NOT NULL, PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL, PS_COMMENT VARCHAR(199) NOT NULL );

DROP TABLE CUSTOMER;

CREATE TABLE CUSTOMER (C_CUSTKEY INTEGER NOT NULL, C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY INTEGER NOT NULL, C_PHONE CHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT NULL, C_MKTSEGMENT CHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL);

DROP TABLE ORDERS;

CREATE TABLE ORDERS (O_ORDERKEY INTEGER NOT NULL, O_CUSTKEY INTEGER NOT NULL, O_ORDERSTATUS CHAR(1) NOT NULL, O_TOTALPRICE DECIMAL(15,2) NOT NULL, O_ORDERDATE DATE NOT NULL, O_ORDERPRIORITY  CHAR(15) NOT NULL, O_CLERK CHAR(15) NOT NULL, O_SHIPPRIORITY INTEGER NOT NULL, O_COMMENT VARCHAR(79) NOT NULL);

DROP TABLE LINEITEM;

CREATE TABLE LINEITEM ( L_ORDERKEY INTEGER NOT NULL, L_PARTKEY INTEGER NOT NULL, L_SUPPKEY INTEGER NOT NULL, L_LINENUMBER  INTEGER NOT NULL, L_QUANTITY DECIMAL(15,2) NOT NULL, L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL, L_DISCOUNT    DECIMAL(15,2) NOT NULL, L_TAX DECIMAL(15,2) NOT NULL, L_RETURNFLAG  CHAR(1) NOT NULL, L_LINESTATUS  CHAR(1) NOT NULL, L_SHIPDATE DATE NOT NULL, L_COMMITDATE  DATE NOT NULL, L_RECEIPTDATE DATE NOT NULL, L_SHIPINSTRUCT CHAR(25) NOT NULL, L_SHIPMODE CHAR(15) NOT NULL, L_COMMENT CHAR(44) NOT NULL);

DROP TABLE PART;

CREATE TABLE PART ( P_PARTKEY INTEGER NOT NULL, P_NAME VARCHAR(55) NOT NULL, P_MFGR CHAR(25) NOT NULL, P_BRAND CHAR(10) NOT NULL, P_TYPE VARCHAR(25) NOT NULL, P_SIZE        INTEGER NOT NULL, P_CONTAINER CHAR(10) NOT NULL, P_RETAILPRICE  DECIMAL(15,2) NOT NULL, P_COMMENT VARCHAR(25) NOT NULL);

select RestorePlan(DumpPlan('select * from customer, orders'));

select RestorePlan(DumpPlan('select *  
from supplier s, partsupp ps, nation n, region r, part p, 
	(select p_partkey, min(ps_supplycost) as min_ps_cost 
	from part, partsupp, supplier, nation, region 
	where p_partkey=ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = \'EUROPE\' group by p_partkey) g
where p.p_partkey = ps.ps_partkey
and g.p_partkey = p.p_partkey and g. min_ps_cost = ps.ps_supplycost and s.s_suppkey = ps.ps_suppkey and p.p_size = 45 and p.p_type like \'%NICKEL\' and s.s_nationkey = n.n_nationkey and n.n_regionkey = r.r_regionkey and r.r_name = \'EUROPE\' '));

select DumpPlanToFile('select *
from supplier s, partsupp ps, nation n, region r, part p,
        (select p_partkey, min(ps_supplycost) as min_ps_cost
        from part, partsupp, supplier, nation, region
        where p_partkey=ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = \'EUROPE\' group by p_partkey) g
where p.p_partkey = ps.ps_partkey
and g.p_partkey = p.p_partkey and g. min_ps_cost = ps.ps_supplycost and s.s_suppkey = ps.ps_suppkey and p.p_size = 45 and p.p_type like \'%NICKEL\' and s.s_nationkey = n.n_nationkey and n.n_regionkey = r.r_regionkey and r.r_name = \'EUROPE\'', 'tpch-q2.aux');

select RestorePlanFromFile('tpch-q2.aux');

select DumpPlanToFile('
select
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
	l_shipdate <= date \'1998-12-01\' - interval \'30 day\'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus',
'tpch-q1.aux');

select DumpPlanToFile('
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = \'BUILDING\'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date \'1995-03-01\'
	and l_shipdate > date \'1995-03-31\'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate',
'tpch-q3.aux');

select DumpPlanToFile('
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date \'1993-07-01\'
	and o_orderdate < date \'1993-07-01\' + interval \'3 month\'
	and exists (
		select * from lineitem
		where l_orderkey = o_orderkey and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority',
'tpch-q4.aux');

select DumpPlanToFile('
select
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
	and r_name = \'ASIA\'
	and o_orderdate >= date \'1994-01-01\'
	and o_orderdate < date \'1994-01-01\' + interval \'1 year\'
group by
	n_name
order by
	revenue desc',
'tpch-q5.aux');


select DumpPlanToFile('
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date \'1994-01-01\'
	and l_shipdate < date \'1994-01-01\' + interval \'1 year\'
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24',
'tpch-q6.aux');

select DumpPlanToFile('
select
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
				(n1.n_name = \'FRANCE\' and n2.n_name = \'GERMANY\')
				or (n1.n_name = \'GERMANY\' and n2.n_name = \'FRANCE\')
			)
			and l_shipdate between date \'1995-01-01\' and date \'1996-12-31\'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year',
'tpch-q7.aux');

select DumpPlanToFile('
select
	o_year,
	sum(case
		when nation = \'BRAZIL\' then volume
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
			and r_name = \'AMERICA\'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between date \'1995-01-01\' and date \'1996-12-31\'
			and p_type = \'ECONOMY ANODIZED STEEL\'
	) as all_nations
group by
	o_year
order by
	o_year',
'tpch-q8.aux');

select DumpPlanToFile('
select nation, o_year, sum(amount) as sum_profit from
(
select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from part, supplier, lineitem, partsupp, orders, nation 
where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like \'%green%\'
) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc', 'tpch-q9.aux');

select DumpPlanToFile('
select
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
	and o_orderdate >= date \'1993-10-01\'
	and o_orderdate < date \'1993-10-01\' + interval \'3 month\'
	and l_returnflag = \'R\'
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
	revenue desc',
'tpch-q10.aux');

select DumpPlanToFile('
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = \'GERMANY\'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = \'GERMANY\'
		)
order by
	value desc',
'tpch-q11.aux');

select DumpPlanToFile('
select
	l_shipmode,
	sum(case
		when o_orderpriority = \'1-URGENT\'
			or o_orderpriority = \'2-HIGH\'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> \'1-URGENT\'
			and o_orderpriority <> \'2-HIGH\'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in (\'MAIL\', \'SHIP\')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date \'1994-01-01\'
	and l_receiptdate < date \'1994-01-01\' + interval \'1 year\'
group by
	l_shipmode
order by
	l_shipmode',
'tpch-q12.aux');

select DumpPlanToFile('
select c_count, count(*) as custdist
from( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like \' %special%requests% \' group by
			c_custkey) as c_orders (c_custkey, c_count)
group by c_count
order by custdist desc, c_count desc', 'tpch-q13.aux');

select DumpPlanToFile('
select
	100.00 * sum(case
		when p_type like \'PROMO%\'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date \'1995-09-01\'
	and l_shipdate < date \'1995-09-01\' + interval \'1 month\'',
'tpch-q14.aux');


select DumpPlanToFile('
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> \'Brand#45\'
	and p_type not like \'MEDIUM POLISHED%\'
	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like \' %Customer%Complaints% \'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size',
'tpch-q16.aux');

select DumpPlanToFile('
select
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
              and p_brand = \'Brand#32\'
              and p_container = \'JUMBO PKG\'
          group by
              p_partkey
      ) x
where
      x_partkey = l_partkey
and l_quantity < x_avg_20;
',
'tpch-q17.aux');

select DumpPlanToFile('
select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
from customer, orders, lineitem
where o_orderkey in ( select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300 )
and c_custkey = o_custkey and o_orderkey = l_orderkey
group by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
order by o_totalprice desc, o_orderdate LIMIT 100',
'tpch-q18.aux');

select DumpPlanToFile('
select
 	        sum(l_extendedprice* (1 - l_discount)) as revenue
 	from
	        lineitem,
	        part
	where
	        (
	                p_partkey = l_partkey
	                and p_brand = \'Brand#23\'
 	                and p_container in (\'SM CASE\', \'SM BOX\', \'SM PACK\', \'SM PKG\')
 	                and l_quantity >= 3 and l_quantity <= 3 + 10
 	                and p_size between 1 and 5
 	                and l_shipmode in (\'AIR\', \'AIR REG\')
 	                and l_shipinstruct = \'DELIVER IN PERSON\'
 	        )
 	        or
 	        (
 	                p_partkey = l_partkey
 	                and p_brand = \'Brand#53\'
 	                and p_container in (\'MED BAG\', \'MED BOX\', \'MED PKG\', \'MED PACK\')
 	                and l_quantity >= 11 and l_quantity <= 11 + 10
 	                and p_size between 1 and 10
 	                and l_shipmode in (\'AIR\', \'AIR REG\')
 	                and l_shipinstruct = \'DELIVER IN PERSON\'
 	        )
 	        or
 	        (
	                p_partkey = l_partkey
	                and p_brand = \'Brand#21\'
 	                and p_container in (\'LG CASE\', \'LG BOX\', \'LG PACK\', \'LG PKG\')
 	                and l_quantity >= 29 and l_quantity <= 29 + 10
 	                and p_size between 1 and 15
 	                and l_shipmode in (\'AIR\', \'AIR REG\')
 	                and l_shipinstruct = \'DELIVER IN PERSON\'
		)',
'tpch-q19.aux');

select DumpPlanToFile('
select s_name, count(distinct(l1.l_orderkey||l1.l_linenumber)) as numwait
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
 	        and o_orderstatus = \'F\'
 	        and l1.l_receiptdate > l1.l_commitdate
 	        and l2.l_orderkey is not null
 	        and l4.l_orderkey is null
 	        and s_nationkey = n_nationkey
 	        and n_name = \'MOZAMBIQUE\'
 	group by
 	        s_name
 	order by
 	        numwait desc,
 	        s_name 	
LIMIT 100',
'tpch-q21.aux');

select DumpPlanToFile('
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
 	                        customer left join orders
 	                                on c_custkey = o_custkey
 	                where
 	                        substring(c_phone from 1 for 2) in
 	                                (\'11\', \'28\', \'21\', \'26\', \'19\', \'13\', \'22\')
 	                        and c_acctbal > (
 	                                select
 	                                        avg(c_acctbal)
 	                                from
 	                                        customer
 	                                where
 	                                        c_acctbal > 0.00
 	                                        and substring(c_phone from 1 for 2) in
 	                                                (\'11\', \'28\', \'21\', \'26\', \'19\', \'13\', \'22\')
 	                        )
 	                        and o_custkey is null
 	        ) as custsale
 	group by
 	        cntrycode
 	order by
 	        cntrycode',
'tpch-q22.aux');

select DumpPlanToFile('
  	select
 	        p_brand,
 	        p_type,
 	        p_size,
 	        count(distinct ps_suppkey) as supplier_cnt
 	from
 	        part,
 	        partsupp left join supplier on (ps_suppkey=s_suppkey and s_comment like \' %Customer%Complaints% \' )
 	where
 	        p_partkey = ps_partkey
 	        and p_brand <> \'Brand#35\'
 	        and p_type not like \'MEDIUM ANODIZED% \'
 	        and p_size in (39, 31, 24, 22, 46, 20, 42, 15)
 	        and s_suppkey is null
 	group by
 	        p_brand,
 	        p_type,
 	        p_size
 	order by
	        supplier_cnt desc,
 	        p_brand,
 	        p_type,
 	        p_size',
'tpch-q24.aux');