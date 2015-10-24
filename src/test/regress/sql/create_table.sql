--
-- CREATE_TABLE
--

--
-- CLASS DEFINITIONS
--
CREATE TABLE hobbies_r (
	name		text, 
	person 		text
);

CREATE TABLE equipment_r (
	name 		text,
	hobby		text
);

CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) WITH OIDS;

CREATE TABLE tenk2 (
	unique1 	int4,
	unique2 	int4,
	two 	 	int4,
	four 		int4,
	ten			int4,
	twenty 		int4,
	hundred 	int4,
	thousand 	int4,
	twothousand int4,
	fivethous 	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);


CREATE TABLE person (
	name 		text,
	age			int4,
	location 	point
);


CREATE TABLE emp (
	salary 		int4,
	manager 	name
) INHERITS (person) WITH OIDS;


CREATE TABLE student (
	gpa 		float8
) INHERITS (person);


CREATE TABLE stud_emp (
	percent 	int4
) INHERITS (emp, student);


CREATE TABLE city (
	name		name,
	location 	box,
	budget 		city_budget
);

CREATE TABLE dept (
	dname		name,
	mgrname 	text
);

CREATE TABLE slow_emp4000 (
	home_base	 box
);

CREATE TABLE fast_emp4000 (
	home_base	 box
);

CREATE TABLE road (
	name		text,
	thepath 	path
);

CREATE TABLE ihighway () INHERITS (road);

CREATE TABLE shighway (
	surface		text
) INHERITS (road);

CREATE TABLE real_city (
	pop			int4,
	cname		text,
	outline 	path
);

--
-- test the "star" operators a bit more thoroughly -- this time,
-- throw in lots of NULL fields...
--
-- a is the type root
-- b and c inherit from a (one-level single inheritance)
-- d inherits from b and c (two-level multiple inheritance)
-- e inherits from c (two-level single inheritance)
-- f inherits from e (three-level single inheritance)
--
CREATE TABLE a_star (
	class		char, 
	a 			int4
);

CREATE TABLE b_star (
	b 			text
) INHERITS (a_star);

CREATE TABLE c_star (
	c 			name
) INHERITS (a_star);

CREATE TABLE d_star (
	d 			float8
) INHERITS (b_star, c_star);

CREATE TABLE e_star (
	e 			int2
) INHERITS (c_star);

CREATE TABLE f_star (
	f 			polygon
) INHERITS (e_star);

CREATE TABLE aggtest (
	a 			int2,
	b			float4
);

CREATE TABLE hash_i4_heap (
	seqno 		int4,
	random 		int4
) distributed by (seqno);

CREATE TABLE hash_name_heap (
	seqno 		int4,
	random 		name
) distributed by (seqno);

CREATE TABLE hash_txt_heap (
	seqno 		int4,
	random 		text
) distributed by (seqno);

CREATE TABLE hash_f8_heap (
	seqno		int4,
	random 		float8
) distributed by (seqno);

-- don't include the hash_ovfl_heap stuff in the distribution
-- the data set is too large for what it's worth
-- 
-- CREATE TABLE hash_ovfl_heap (
--	x			int4,
--	y			int4
-- );

CREATE TABLE bt_i4_heap (
	seqno 		int4,
	random 		int4
);

CREATE TABLE bt_name_heap (
	seqno 		name,
	random 		int4
);

CREATE TABLE bt_txt_heap (
	seqno 		text,
	random 		int4
);

CREATE TABLE bt_f8_heap (
	seqno 		float8, 
	random 		int4
);

CREATE TABLE array_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
);

CREATE TABLE array_index_op_test (
	seqno		int4,
	i			int4[],
	t			text[]
);

--MPP-22020: Dis-allow duplicate constraint names for the same table.
create table dupconstr (
						i int,
						j int constraint test CHECK (j > 10),
						CONSTRAINT test UNIQUE (i,j))
						distributed by (i);

-- MPP-2764: distributed randomly is not compatible with primary key or unique
-- constraints
create table distrand(i int, j int, primary key (i)) distributed randomly;
create table distrand(i int, j int, unique (i)) distributed randomly;
create table distrand(i int, j int, primary key (i, j)) distributed randomly;
create table distrand(i int, j int, unique (i, j)) distributed randomly;
create table distrand(i int, j int, constraint "test" primary key (i)) 
   distributed randomly;
create table distrand(i int, j int, constraint "test" unique (i)) 
   distributed randomly;
-- this should work though
create table distrand(i int, j int, constraint "test" unique (i, j)) 
   distributed by(i, j);
drop table distrand;
create table distrand(i int, j int) distributed randomly;
create unique index distrand_idx on distrand(i);
drop table distrand; 

-- Make sure distribution policy determined from CTAS actually works, MPP-101
create table distpol as select random(), 1 as a, 2 as b distributed by (random);
select attrnums from gp_distribution_policy where 
  localoid = 'distpol'::regclass;
drop table distpol;
create table distpol as select random(), 2 as foo distributed by (foo);
select attrnums from gp_distribution_policy where 
  localoid = 'distpol'::regclass;
drop table distpol;
-- now test that MPP-101 /actually/ works
create table distpol (i int, j int, k int) distributed by (i);
alter table distpol add primary key (j);
select attrnums from gp_distribution_policy where 
  localoid = 'distpol'::regclass;
-- make sure we can't overwrite it
create unique index distpol_uidx on distpol(k);
-- should be able to now
alter table distpol drop constraint distpol_pkey;
create unique index distpol_uidx on distpol(k);
select attrnums from gp_distribution_policy where 
  localoid = 'distpol'::regclass;
drop index distpol_uidx;
-- expressions shouldn't be able to update the distribution key
create unique index distpol_uidx on distpol(ln(k));
drop index distpol_uidx;
-- lets make sure we don't change the policy when the table is full
insert into distpol values(1, 2, 3);
create unique index distpol_uidx on distpol(i);
alter table distpol add primary key (i);
drop table distpol;

-- MPP-2872: set ops with distributed by should work as advertised
create table distpol1 (i int, j int);
create table distpol2 (i int, j int);
create table distpol3 as select i, j from distpol1 union 
  select i, j from distpol2 distributed by (j);
select attrnums from gp_distribution_policy where
  localoid = 'distpol3'::regclass;
drop table distpol3;
create table distpol3 as (select i, j from distpol1 union
  select i, j from distpol2) distributed by (j);
select attrnums from gp_distribution_policy where
  localoid = 'distpol3'::regclass;


-- MPP-7268: CTAS produces incorrect distribution.
drop table if exists foo;
drop table if exists bar;
create table foo (a varchar(15), b int) distributed by (b);
create table bar as select * from foo distributed by (b);
select attrnums from gp_distribution_policy where localoid='bar'::regclass;

drop table if exists foo;
drop table if exists bar;
create table foo (a int, b varchar(15)) distributed by (b);
create table bar as select * from foo distributed by (b);
select attrnums from gp_distribution_policy where localoid='bar'::regclass;

drop table if exists foo;
drop table if exists bar;

CREATE TABLE foo (
col_with_default numeric DEFAULT 0,
col_with_default_drop_default character varying(30) DEFAULT 'test1',
col_with_constraint numeric UNIQUE
) DISTRIBUTED BY (col_with_constraint);

CREATE TABLE bar AS SELECT * FROM foo distributed by (col_with_constraint);
select attrnums from gp_distribution_policy where localoid='bar'::regclass;

drop table if exists foo;
drop table if exists bar;

-- MPP-14770: check for duplicate columns in DISTRIBUTED BY clause
create table foo (a int, b text) distributed by (b,B);
create table foo (a int, b int) distributed by (a,aA,A);
create table foo (a int, b int) distributed by (b,a,aabb);
create table foo (a int, b int) distributed by (c,C);
create table foo ("I" int, i int) distributed by ("I",I);
select attrnums from gp_distribution_policy where localoid='foo'::regclass;
drop table if exists foo;
