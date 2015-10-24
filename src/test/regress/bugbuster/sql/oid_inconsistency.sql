DROP language if exists plperl;

CREATE language plperl;

select distinct oid, lanname from pg_language where lanname='plperl';
select distinct oid, lanname from gp_dist_random('pg_language') where lanname='plperl';


--
DROP language if exists plperl;

CREATE language plperl;

select distinct lanvalidator, lanname from pg_language where lanname='plperl';
select distinct lanvalidator, lanname from gp_dist_random('pg_language') where lanname='plperl';


--
DROP language if exists plperl;

CREATE language plperl;

select distinct lanplcallfoid, lanname from pg_language where lanname='plperl';
select distinct lanplcallfoid, lanname from gp_dist_random('pg_language') where lanname='plperl';


--
DROP language if exists plperl;

CREATE language plperl;

select distinct oid, proname from pg_proc where proname ~ 'plperl_call_handler';
select distinct oid, proname from gp_dist_random('pg_proc') where proname ~ 'plperl_call_handler';


--
DROP language if exists  plperl;

CREATE language plperl;

select distinct oid, proname from pg_proc where proname ~ 'plperl_validator';
select distinct oid, proname from gp_dist_random('pg_proc') where proname ~ 'plperl_validator';


--
DROP view if exists rewrite_view;
DROP table if exists rewrite_table;

CREATE table rewrite_table(i int, t text) distributed by (i);

INSERT into rewrite_table values(generate_series(1,10),'rewrite rules are created on view creation');

CREATE view rewrite_view as select * from rewrite_table where i <5;

select oid, rulename, ev_class::regclass from pg_rewrite where ev_class='rewrite_view'::regclass;
select oid, rulename, ev_class::regclass from gp_dist_random('pg_rewrite') where ev_class='rewrite_view'::regclass;


--
DROP view if exists tt1 cascade;
DROP table if exists tt2 cascade;

CREATE table tt1 (a int) distributed by (a);
CREATE table tt2 (a int) distributed by (a);

CREATE rule "_RETURN" as on select to tt1
        do instead select * from tt2;

select oid,gp_segment_id,rulename,ev_class::regclass from pg_rewrite where ev_class = 'tt1'::regclass;

select oid,gp_segment_id,rulename,ev_class::regclass from gp_dist_random('pg_rewrite') where ev_class = 'tt1'::regclass;



--
DROP table if exists bar_ao cascade;
DROP table if exists foo_ao cascade;

CREATE table foo_ao (a int) with (appendonly=true) distributed by (a);
CREATE table bar_ao (a int) distributed by (a);

CREATE rule one as on insert to bar_ao do instead update foo_ao set a=1;

SELECT oid,gp_segment_id,rulename,ev_class::regclass from pg_rewrite where ev_class = 'bar_ao'::regclass;

SELECT oid,gp_segment_id,rulename,ev_class::regclass from gp_dist_random('pg_rewrite') where ev_class = 'bar_ao'::regclass;


--
DROP table if exists bar2_ao cascade;
DROP table if exists foo2_ao cascade;

CREATE table foo2_ao (a int) with (appendonly=true) distributed by (a);
CREATE table bar2_ao (a int) distributed by (a);

CREATE rule two as on insert to bar2_ao do instead delete from foo2_ao where a=1;

SELECT oid,gp_segment_id,rulename,ev_class::regclass from pg_rewrite where ev_class = 'bar2_ao'::regclass;

SELECT oid,gp_segment_id,rulename,ev_class::regclass from gp_dist_random('pg_rewrite') where ev_class = 'bar2_ao'::regclass;


--
DROP table if exists bar3 cascade;
DROP table if exists foo3 cascade;

CREATE table foo3 (a int) distributed by (a);
CREATE table bar3 (a int) distributed by (a);

CREATE rule three as on insert to bar3 do instead insert into foo3(a) values(1);

SELECT oid,gp_segment_id,rulename,ev_class::regclass from pg_rewrite where ev_class = 'bar3'::regclass;

SELECT oid,gp_segment_id,rulename,ev_class::regclass from gp_dist_random('pg_rewrite') where ev_class = 'bar3'::regclass;


--
DROP table if exists trigger_oid cascade;
--DROP language if exists plpgsql;

--CREATE language plpgsql;

CREATE table trigger_oid(a int, b int) distributed by (a);

CREATE OR REPLACE function trig_func() RETURNS trigger AS $$
  BEGIN
    RETURN new;
  END
$$ LANGUAGE plpgsql NO SQL;

CREATE trigger troid_trigger AFTER INSERT ON trigger_oid
    FOR EACH ROW EXECUTE PROCEDURE trig_func();


select distinct oid, tgrelid::regclass, tgname 
from   pg_trigger 
where tgname='troid_trigger';


select distinct oid, tgrelid::regclass, tgname 
from   gp_dist_random('pg_trigger') 
where tgname='troid_trigger';


--
DROP table if exists pg_table_column_unique cascade;

CREATE table pg_table_column_unique (i int  constraint unique1 unique, t text) distributed by (i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_column_unique'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_column_unique'::regclass;


--
DROP table if exists pg_table_column_unique_2 cascade;

CREATE table pg_table_column_unique_2 (i int  constraint unique2 unique, t text) distributed by (i);

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_column_unique_2'::regclass;
select conrelid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_column_unique_2'::regclass;


--
DROP table if exists pg_table_column_primary cascade;

CREATE table pg_table_column_primary (i int constraint primary1 primary key, t text) distributed by (i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_column_primary'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_column_primary'::regclass;


--
DROP table if exists pg_table_column_primary_2 cascade;

CREATE table pg_table_column_primary_2 (i int constraint primary1 primary key, t text) distributed by (i);

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_column_primary_2'::regclass;
select conrelid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_column_primary_2'::regclass;



--
DROP table if exists pg_table_column_check;

CREATE TABLE pg_table_column_check  (
    did integer,
    name varchar(40) CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_column_check'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_column_check'::regclass;


--
DROP table if exists pg_table_column_check_2;

CREATE table pg_table_column_check_2  (
    did integer,
    name varchar(40) CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_column_check_2'::regclass;
select conrelid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_column_check_2'::regclass;


--
DROP table if exists pg_table_unique cascade;

CREATE table pg_table_unique (i int, t text, constraint unique1 unique(i)) distributed by (i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_unique'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_unique'::regclass;


--
DROP table if exists pg_table_unique_2 cascade;

CREATE table pg_table_unique_2 (i int, t text, constraint unique2 unique(i)) distributed by (i);

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_unique_2'::regclass;
select conrelid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_unique_2'::regclass;


--
DROP table if exists pg_table_primary cascade;

CREATE table pg_table_primary (i int, t text, constraint primary1 primary key (i)) distributed by (i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_primary'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_primary'::regclass;


--
DROP table if exists pg_table_primary_2 cascade;

CREATE table pg_table_primary_2 (i int, t text, constraint primary2 primary key (i)) distributed by (i);

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_primary_2'::regclass;
select conrelid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_primary_2'::regclass;


--
DROP table if exists pg_table_check;

CREATE TABLE pg_table_check  (
    did integer,
    name varchar(40),
    CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_check'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_check'::regclass;


--
DROP table if exists pg_table_check_2;

CREATE table pg_table_check_2  (
    did integer,
    name varchar(40)
    CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_check_2'::regclass;
select conrelid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_check_2'::regclass;


--
DROP table if exists pg_table_like;
DROP table if exists pg_table_like_base;

CREATE table pg_table_like_base  (
    did integer,
    name varchar(40),
    CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

CREATE table pg_table_like (like pg_table_like_base including constraints) distributed randomly;

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_like'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_like'::regclass;


--
DROP table if exists pg_table_like_2;
DROP table if exists pg_table_like_base_2;

CREATE table pg_table_like_base_2  (
    did integer,
    name varchar(40),
    CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

CREATE table pg_table_like_2 (like pg_table_like_base_2 including constraints) distributed randomly;

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_like_2'::regclass;
select conrelid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_like_2'::regclass;


--
-- QA-2003
set gp_enable_inheritance = true;
DROP table if exists pg_table_inherit;
DROP table if exists pg_table_inherit_base;

CREATE table pg_table_inherit_base  (
    did integer,
    name varchar(40),
    CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

CREATE table pg_table_inherit() inherits( pg_table_inherit_base) distributed randomly;

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_inherit'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_inherit'::regclass;


--
-- QA-2003
set gp_enable_inheritance = true;

DROP table if exists pg_table_inherit_2;
DROP table if exists pg_table_inherit_base_2;

CREATE table pg_table_inherit_base_2  (
    did integer,
    name varchar(40),
    CONSTRAINT con1 CHECK (did > 99 AND name <> '')
    )DISTRIBUTED RANDOMLY;

CREATE table pg_table_inherit_2() inherits( pg_table_inherit_base_2) distributed randomly;

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_inherit_2'::regclass;
select conrelid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_inherit_2'::regclass;


--
DROP table if exists pg_part_table;

CREATE table pg_part_table (trans_id int, date date, amount 
decimal(9,2), region text) 
DISTRIBUTED BY (trans_id) 
PARTITION BY RANGE (date) 
SUBPARTITION BY LIST (region) 
SUBPARTITION TEMPLATE 
( SUBPARTITION usa VALUES ('usa'))  
( START (date '2008-01-01') INCLUSIVE 
   END (date '2009-01-01') EXCLUSIVE 
   EVERY (INTERVAL '6 month') );

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_part_table_1_prt_1'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_part_table_1_prt_1'::regclass;

 
--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_part_table_1_prt_2'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_part_table_1_prt_2'::regclass;


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_part_table_1_prt_1_2_prt_usa'::regclass and conname ~'usa';
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_part_table_1_prt_1_2_prt_usa'::regclass and conname ~'usa';


--
DROP table if exists pg_part_table_unique;

CREATE table pg_part_table_unique (trans_id int, date1 date, amount 
decimal(9,2), region text, constraint unq1 unique(date1)) 
DISTRIBUTED BY (date1) 
PARTITION BY RANGE (date1)
( START (date '2008-01-01') INCLUSIVE 
   END (date '2009-01-01') EXCLUSIVE 
   EVERY (INTERVAL '6 month') );

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_part_table_unique_1_prt_1'::regclass and conname = 'unq1';
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_part_table_unique_1_prt_1'::regclass and conname = 'unq1';


--
DROP table if exists pg_part_table_unique_2;

CREATE table pg_part_table_unique_2 (trans_id int, date1 date, amount
decimal(9,2), region text, constraint unq1 unique(date1))
DISTRIBUTED BY (date1)
PARTITION BY RANGE (date1)
( START (date '2008-01-01') INCLUSIVE
   END (date '2009-01-01') EXCLUSIVE
   EVERY (INTERVAL '12 month') );

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_part_table_unique_2_1_prt_1'::regclass and conname = 'unq1';
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_part_table_unique_2_1_prt_1'::regclass and conname = 'unq1';


--
DROP table if exists pg_part_table_primary;

CREATE table pg_part_table_primary (trans_id int, date1 date, amount
decimal(9,2), region text, constraint primary1 primary key(date1))
DISTRIBUTED BY (date1)
PARTITION BY RANGE (date1)
( START (date '2008-01-01') INCLUSIVE
   END (date '2009-01-01') EXCLUSIVE
   EVERY (INTERVAL '12 month') );

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_part_table_primary_1_prt_1'::regclass and conname = 'primary1';
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_part_table_primary_1_prt_1'::regclass and conname = 'primary1';


--
DROP table if exists pg_part_table_primary_2;

CREATE table pg_part_table_primary_2 (trans_id int, date1 date, amount
decimal(9,2), region text, constraint primary1 primary key(date1))
DISTRIBUTED BY (date1)
PARTITION BY RANGE (date1)
( START (date '2008-01-01') INCLUSIVE
   END (date '2009-01-01') EXCLUSIVE
   EVERY (INTERVAL '12 month') );

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_part_table_primary_2_1_prt_1'::regclass and conname = 'primary1';
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_part_table_primary_2_1_prt_1'::regclass and conname = 'primary1';


--
DROP table if exists pg_table_part_add_primary;

CREATE table pg_table_part_add_primary (i int, t text) distributed by (i) partition by range(i)  (start (1) end (10) every (5) );

ALTER table pg_table_part_add_primary  add constraint primary3 primary key (i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_add_primary_1_prt_1'::regclass and conname = 'primary3';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_add_primary_1_prt_1'::regclass and conname = 'primary3';


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_add_primary_1_prt_2'::regclass and conname = 'primary3';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_add_primary_1_prt_2'::regclass and conname = 'primary3';

--
DROP table if exists pg_table_part_add_unique;

CREATE table pg_table_part_add_unique (i int, t text) distributed by (i) partition by range(i)  (start (1) end (10) every (5) );

ALTER table pg_table_part_add_unique  add constraint unique3 unique (i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_add_unique_1_prt_1'::regclass and conname = 'unique3';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_add_unique_1_prt_1'::regclass and conname = 'unique3';

--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_add_unique_1_prt_2'::regclass and conname = 'unique3';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_add_unique_1_prt_2'::regclass and conname = 'unique3';

--
DROP table  if exists pg_table_alter_add_check;

CREATE table pg_table_alter_add_check  (
    did integer,
    name varchar(40)
    )DISTRIBUTED RANDOMLY;

ALTER table pg_table_alter_add_check add constraint check1 CHECK (did > 99 AND name <> '') ;

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_add_check'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_add_check'::regclass;


--
DROP table if exists pg_table_alter_add_check_2;

CREATE table pg_table_alter_add_check_2  (
    did integer,
    name varchar(40)
    )DISTRIBUTED RANDOMLY;

ALTER table pg_table_alter_add_check_2 add constraint check1 CHECK (did > 99 AND name <> '') ;

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_add_check_2'::regclass;
select conrelid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_add_check_2'::regclass;


--
DROP table if exists pg_table_alter_add_column_check cascade;

CREATE table pg_table_alter_add_column_check (i int , t text) distributed by (i);

ALTER table pg_table_alter_add_column_check add column k int constraint check1 CHECK (k> 99);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_add_column_check'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_add_column_check'::regclass;


--
DROP table if exists pg_table_alter_add_column_check_2 cascade;

CREATE table pg_table_alter_add_column_check_2 (i int , t text) distributed by (i);

ALTER table pg_table_alter_add_column_check_2 add column k int constraint check1 CHECK (k> 99);

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_add_column_check_2'::regclass;
select conrelid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_add_column_check_2'::regclass;


--
DROP table if exists pg_table_part_alter_add_column_check cascade;

CREATE table pg_table_part_alter_add_column_check (i int ,t text) distributed by (i) partition by range(i)  (start (1) end (10) every (5) );

ALTER table pg_table_part_alter_add_column_check add column k int constraint check2 CHECK (k> 99);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_alter_add_column_check_1_prt_1'::regclass and conname = 'check2';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_alter_add_column_check_1_prt_1'::regclass and conname = 'check2';


--
DROP table if exists pg_table_part_alter_add_column_check_2 cascade;

CREATE table pg_table_part_alter_add_column_check_2 (i int ,t text) distributed by (i) partition by range(i)  (start (1) end (10) every (5) );

ALTER table pg_table_part_alter_add_column_check_2 add column k int constraint check2 CHECK (k> 99);

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_alter_add_column_check_2_1_prt_2'::regclass and conname = 'check2';
select conrelid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_alter_add_column_check_2_1_prt_2'::regclass and conname = 'check2';
 

--
DROP table if exists pg_table_alter_add_part;

CREATE table pg_table_alter_add_part (i int,t text) distributed by (i) partition by range(i)  (start (1) end (10) every (5) );

ALTER table pg_table_alter_add_part add partition part3 start (11) end (15);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_add_part_1_prt_part3'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_add_part_1_prt_part3'::regclass;


--
DROP table if exists exchange_part;
DROP table if exists pg_table_alter_exchange_part;

CREATE table pg_table_alter_exchange_part (i int,t text, constraint check2 check(i<10)) distributed by (i) partition by range(i)
                                          (default partition p0, partition p1  start (1) end (5) ,
                                           partition p2 start (6) end (10));

CREATE table exchange_part(i int,t text) distributed by (i);

ALTER table pg_table_alter_exchange_part exchange partition p2 with table exchange_part;

select oid, gp_segment_id, conname from pg_constraint where conname ~ 'pg_table_alter_exchange_part_1_prt_p2' and oid = (select max(oid) from pg_constraint where conname ~ 'pg_table_alter_exchange_part_1_prt_p2');
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conname ~ 'pg_table_alter_exchange_part_1_prt_p2' and oid = (select max(oid) from gp_dist_random('pg_constraint') where conname ~ 'pg_table_alter_exchange_part_1_prt_p2');


--
DROP table if exists pg_table_alter_split_part;

CREATE table pg_table_alter_split_part (i int,t text) distributed by (i) partition by range(i)  (start (1) end (10) every (10) );

ALTER table pg_table_alter_split_part split partition for ('1') at('5') into (partition p3, partition p4);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_split_part_1_prt_p3'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_split_part_1_prt_p3'::regclass;


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_split_part_1_prt_p4'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_split_part_1_prt_p4'::regclass;

--
DROP domain if exists pg_create_domain;

CREATE domain pg_create_domain AS TEXT CONSTRAINT cons2_check check (char_length(VALUE) = 5);

select oid, gp_segment_id, conname from pg_constraint where conname ~ 'cons2_check';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conname ~ 'cons2_check';


--
DROP domain if exists pg_alter_domain;

CREATE domain pg_alter_domain AS TEXT ;

ALTER domain pg_alter_domain add CONSTRAINT cons_check check (char_length(VALUE) = 5);
select oid, gp_segment_id, conname from pg_constraint where conname ~ 'cons_check';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conname ~ 'cons_check';


--
DROP table if exists pg_table_foreign cascade;
DROP table if exists pg_table_primary1 cascade;

CREATE table pg_table_primary1 (i int, t text, constraint primary1 primary key (i)) distributed by (i);

CREATE table pg_table_foreign (a1 int, a2 int, a3 text, constraint foreign1 foreign key (a2) references pg_table_primary1(i)) distributed by (a1);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_primary1'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_primary1'::regclass;


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_foreign'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_foreign'::regclass;

--
DROP table if exists pg_table_part_foreign_2;
DROP table if exists pg_table_part_primary2;

CREATE table pg_table_part_primary2 (i int, t text, constraint primary2 primary key (i)) distributed by (i) partition by range(i)  (start (1) end (10) every (5) );

CREATE table pg_table_part_foreign_2 (a1 int, a2 int, a3 text) distributed by (a1)
             partition by range(a1) (start (1) end (10) every (5) );

ALTER table pg_table_part_foreign_2  add constraint foreign1 foreign key (a2) references pg_table_part_primary2(i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_primary2_1_prt_1'::regclass and conname = 'primary2';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_primary2_1_prt_1'::regclass and conname = 'primary2';


--
DROP table if exists pg_alter_table_foreign cascade;
DROP table if exists pg_alter_table_primary1 cascade;

CREATE table pg_alter_table_primary1 (i int, t text, constraint primary1 primary key (i)) distributed by (i);

CREATE table pg_alter_table_foreign (a1 int, a2 int, a3 text) distributed by (a1);

ALTER table pg_alter_table_foreign add constraint foreign1 foreign key (a2) references pg_alter_table_primary1(i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_alter_table_primary1'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_alter_table_primary1'::regclass;


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_alter_table_foreign'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_alter_table_foreign'::regclass;


--
-- QA-2003
set gp_enable_inheritance = true;
DROP table if exists pg_table_inherit;
DROP table if exists pg_table_inherit_alter_base;

CREATE table pg_table_inherit_alter_base  (
    did integer,
    name varchar(40)
    )DISTRIBUTED RANDOMLY;

CREATE table pg_table_inherit() inherits(pg_table_inherit_alter_base) distributed randomly;

ALTER table pg_table_inherit_alter_base add CONSTRAINT con1 CHECK (did > 99 AND name <> '');
 
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_inherit_alter_base'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_inherit_alter_base'::regclass;


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_inherit'::regclass;
select oid, gp_segment_id, conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_inherit'::regclass;


--
DROP table if exists pg_table_alter_alter_type cascade;

CREATE table pg_table_alter_alter_type (i int constraint con1 check (i<99), t text) distributed by (i);

ALTER table pg_table_alter_alter_type alter column i TYPE float;

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_alter_type'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_alter_type'::regclass;


--
DROP table if exists pg_table_alter_alter_type_2 cascade;

CREATE table pg_table_alter_alter_type_2 (i int constraint con1 check (i<99), t text) distributed by (i);

ALTER table pg_table_alter_alter_type_2 alter column i TYPE float;

select conrelid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_alter_alter_type_2'::regclass;
select conrelid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_alter_alter_type_2'::regclass;


--
DROP table if exists pg_table_alter_default_part;

CREATE table pg_table_alter_default_part (i int,t text, constraint check2 check(i<10)) distributed by (i) partition by range(i)
                                          (partition p1  start (1) end (5) ,
                                           partition p2 start (6) end (10));

ALTER table pg_table_alter_default_part add default partition p0;

select oid, gp_segment_id, conname from pg_constraint where conname ~ 'check2' and conrelid = ' pg_table_alter_default_part_1_prt_p0'::regclass;
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conname ~ 'check2' and conrelid = ' pg_table_alter_default_part_1_prt_p0'::regclass;

--
DROP table if exists pg_table_part_check;

CREATE table pg_table_part_check (i int, t text, CONSTRAINT part_con1 CHECK (i> 99 AND t <> '')) distributed by (i)
                                      partition by range(i)  (start (1) end (10) every (5) );

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_check_1_prt_1'::regclass and conname = 'part_con1';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_check_1_prt_1'::regclass and conname = 'part_con1';


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_check_1_prt_2'::regclass and conname = 'part_con1';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_check_1_prt_2'::regclass and conname = 'part_con1';


--
DROP table if exists pg_table_part_add_unique;

CREATE table pg_table_part_add_unique (i int, t text) distributed by (i) partition by range(i)  (start (1) end (10) every (5) );

ALTER table pg_table_part_add_unique  add constraint unique3 unique (i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_add_unique_1_prt_1'::regclass and conname = 'unique3';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_add_unique_1_prt_1'::regclass and conname = 'unique3';


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_add_unique_1_prt_2'::regclass and conname = 'unique3';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_add_unique_1_prt_2'::regclass and conname = 'unique3';

--
DROP table if exists pg_table_part_add_primary;

CREATE table pg_table_part_add_primary (i int, t text) distributed by (i) partition by range(i)  (start (1) end (10) every (5) );

ALTER table pg_table_part_add_primary  add constraint primary3 primary key (i);

select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_add_primary_1_prt_1'::regclass and conname = 'primary3';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_add_primary_1_prt_1'::regclass and conname = 'primary3';


--
select oid, gp_segment_id, conname from pg_constraint where conrelid = 'pg_table_part_add_primary_1_prt_2'::regclass and conname = 'primary3';
select oid, gp_segment_id,conname from gp_dist_random('pg_constraint') where conrelid = 'pg_table_part_add_primary_1_prt_2'::regclass and conname = 'primary3';

 

