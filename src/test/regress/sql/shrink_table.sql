drop schema if exists test_shrink_table cascade;
create schema test_shrink_table;
set search_path=test_shrink_table,public;
set default_table_access_method='heap';
set allow_system_table_mods=true;

-- Hash distributed tables
Create table t1(a int, b int, c int) distributed by (a);
insert into t1 select i,i,0 from generate_series(1,100) I;
Update t1 set c = gp_segment_id;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

begin;
Alter table t1 shrink table to 2;
Select gp_segment_id, count(*) from t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

Alter table t1 shrink table to 2;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='t1'::regclass;
drop table t1;


Create table t1(a int, b int, c int) distributed by (a,b);
insert into t1 select i,i,0 from generate_series(1,100) I;
Update t1 set c = gp_segment_id;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

begin;
Alter table t1 shrink table to 1;
Select gp_segment_id, count(*) from t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

Alter table t1 shrink table to 1;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='t1'::regclass;
drop table t1;

-- Test NULLs.
Create table t1(a int, b int, c int) distributed by (a,b,c);
insert into t1 values
  (1,    1,    1   ),
  (null, 2,    2   ),
  (3,    null, 3   ),
  (4,    4,    null),
  (null, null, 5   ),
  (null, 6,    null),
  (7,    null, null),
  (null, null, null);
Select gp_segment_id, count(*) from t1 group by gp_segment_id;

begin;
Alter table t1 shrink table to 2;
Select gp_segment_id, count(*) from t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

Alter table t1 shrink table to 2;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='t1'::regclass;
drop table t1;

Create table t1(a int, b int, c int) distributed by (a) partition by list(b) (partition t1_1 values(1), partition t1_2 values(2), default partition other);
insert into t1 select i,i,0 from generate_series(1,100) I;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

begin;
Alter table t1 shrink table to 2;
Select gp_segment_id, count(*) from t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

Alter table t1 shrink table to 2;

Select gp_segment_id, count(*) from t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='t1'::regclass;
drop table t1;

-- Random distributed tables
Create table r1(a int, b int, c int) distributed randomly;
insert into r1 select i,i,0 from generate_series(1,100) I;
Update r1 set c = gp_segment_id;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

begin;
Alter table r1 shrink table to 2;
Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;
abort;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

Alter table r1 shrink table to 2;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

Create table r1(a int, b int, c int) distributed randomly;
insert into r1 select i,i,0 from generate_series(1,100) I;
Update r1 set c = gp_segment_id;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

begin;
Alter table r1 shrink table to 2;
Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;
abort;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

Alter table r1 shrink table to 2;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

Create table r1(a int, b int, c int) distributed randomly partition by list(b) (partition r1_1 values(1), partition r1_2 values(2), default partition other);
insert into r1 select i,i,0 from generate_series(1,100) I;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

begin;
Alter table r1 shrink table to 2;
Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;
abort;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

Alter table r1 shrink table to 2;

Select count(*) from r1;
Select count(*) > 0 from r1 where gp_segment_id=2;

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

-- Replicated tables
Create table r1(a int, b int, c int) distributed replicated;

insert into r1 select i,i,0 from generate_series(1,100) I;

Select count(*) from gp_dist_random('r1');

begin;
Alter table r1 shrink table to 2;
Select count(*) from gp_dist_random('r1');
abort;

Select count(*) from gp_dist_random('r1');

Alter table r1 shrink table to 2;

Select count(*) from gp_dist_random('r1');

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

--
Create table r1(a int, b int, c int) distributed replicated;

insert into r1 select i,i,0 from generate_series(1,100) I;

Select count(*) from gp_dist_random('r1');

begin;
Alter table r1 shrink table to 2;
Select count(*) from gp_dist_random('r1');
abort;

Select count(*) from gp_dist_random('r1');

Alter table r1 shrink table to 2;

Select count(*) from gp_dist_random('r1');

select numsegments from gp_distribution_policy where localoid='r1'::regclass;
drop table r1;

-- table with update triggers on distributed key column
CREATE OR REPLACE FUNCTION trigger_func() RETURNS trigger AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE table_with_update_trigger(a int, b int, c int);
insert into table_with_update_trigger select i,i,0 from generate_series(1,100) I;
select gp_segment_id, count(*) from table_with_update_trigger group by 1 order by 1;

CREATE TRIGGER foo_br_trigger BEFORE INSERT OR UPDATE OR DELETE ON table_with_update_trigger 
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_stmt');
CREATE TRIGGER foo_ar_trigger AFTER INSERT OR UPDATE OR DELETE ON table_with_update_trigger 
FOR EACH ROW EXECUTE PROCEDURE trigger_func('before_stmt');

CREATE TRIGGER foo_bs_trigger BEFORE INSERT OR UPDATE OR DELETE ON table_with_update_trigger 
FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func('before_stmt');
CREATE TRIGGER foo_as_trigger AFTER INSERT OR UPDATE OR DELETE ON table_with_update_trigger 
FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func('before_stmt');

-- update should fail
update table_with_update_trigger set a = a + 1;
-- data expansion should success and not hiting any triggers.
Alter table table_with_update_trigger shrink table to 2;
select gp_segment_id, count(*) from table_with_update_trigger group by 1 order by 1;
drop table table_with_update_trigger;

--
-- Test shrinking inheritance parent table, parent table has different
-- numsegments with child tables.
--
create table mix_base_tbl (a int4, b int4) DISTRIBUTED RANDOMLY;
insert into mix_base_tbl select g, g from generate_series(1, 3) g;
create table mix_child_a (a int4, b int4) inherits (mix_base_tbl) distributed by (a);
insert into mix_child_a select g, g from generate_series(11, 13) g;
create table mix_child_b (a int4, b int4) inherits (mix_base_tbl) distributed by (b);
insert into mix_child_b select g, g from generate_series(21, 23) g;
-- shrink the child table, not effect parent table
Alter table mix_child_a shrink table to 2;
select numsegments from gp_distribution_policy where localoid='mix_base_tbl'::regclass;
-- shrink the parent table, both parent and child table will be rebalanced to all
-- segments
select count(*) from mix_child_a where gp_segment_id = 2;
select count(*) from mix_child_b where gp_segment_id = 2;
Alter table mix_base_tbl shrink table to 2;
select count(*) from mix_child_a where gp_segment_id = 2;
select count(*) from mix_child_b where gp_segment_id = 2;
drop table mix_base_tbl cascade;

-- multi-level partition tables
CREATE TABLE part_t1(a int, b int, c int, d int, e int)
DISTRIBUTED BY(a)
PARTITION BY RANGE (b)
    SUBPARTITION BY RANGE (c)
        SUBPARTITION TEMPLATE (
            START(1) END (3) EVERY(1),
            DEFAULT SUBPARTITION others_c)

    SUBPARTITION BY LIST (d)
        SUBPARTITION TEMPLATE (
            SUBPARTITION one VALUES (1),
            SUBPARTITION two VALUES (2),
            SUBPARTITION three VALUES (3),
            DEFAULT SUBPARTITION others_d)

( START (1) END (2) EVERY (1),
    DEFAULT PARTITION other_b);

insert into part_t1 select i,i%3,i%4,i%5,i from generate_series(1,100) I;
Update part_t1 set e = gp_segment_id;

Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;

begin;
Alter table part_t1 shrink table to 2;
Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;
abort;

Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;

Alter table part_t1 shrink table to 2;

Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;

select numsegments from gp_distribution_policy where localoid='part_t1'::regclass;
drop table part_t1;

--
CREATE TABLE part_t1(a int, b int, c int, d int, e int)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (b)
    SUBPARTITION BY RANGE (c)
        SUBPARTITION TEMPLATE (
            START(1) END (3) EVERY(1),
            DEFAULT SUBPARTITION others_c)

    SUBPARTITION BY LIST (d)
        SUBPARTITION TEMPLATE (
            SUBPARTITION one VALUES (1),
            SUBPARTITION two VALUES (2),
            SUBPARTITION three VALUES (3),
            DEFAULT SUBPARTITION others_d)

( START (1) END (2) EVERY (1),
    DEFAULT PARTITION other_b);

insert into part_t1 select i,i%3,i%4,i%5,i from generate_series(1,100) I;
Update part_t1 set e = gp_segment_id;

Select count(*) from part_t1;
Select count(*) > 0 from part_t1 where gp_segment_id=2;

begin;
Alter table part_t1 shrink table to 2;
Select count(*) from part_t1;
Select count(*) > 0 from part_t1 where gp_segment_id=2;
abort;

Select count(*) from part_t1;
Select count(*) > 0 from part_t1 where gp_segment_id=2;

Alter table part_t1 shrink table to 2;

Select count(*) from part_t1;
Select count(*) > 0 from part_t1 where gp_segment_id=2;

select numsegments from gp_distribution_policy where localoid='part_t1'::regclass;
drop table part_t1;

-- only shrink leaf partitions, not allowed now
CREATE TABLE part_t1(a int, b int, c int, d int, e int)
DISTRIBUTED BY(a)
PARTITION BY RANGE (b)
    SUBPARTITION BY RANGE (c)
        SUBPARTITION TEMPLATE (
            START(1) END (3) EVERY(1),
            DEFAULT SUBPARTITION others_c)

    SUBPARTITION BY LIST (d)
        SUBPARTITION TEMPLATE (
            SUBPARTITION one VALUES (1),
            SUBPARTITION two VALUES (2),
            SUBPARTITION three VALUES (3),
            DEFAULT SUBPARTITION others_d)

( START (1) END (2) EVERY (1),
    DEFAULT PARTITION other_b);

insert into part_t1 select i,i%3,i%4,i%5,i from generate_series(1,100) I;
Update part_t1 set e = gp_segment_id;

select gp_segment_id, * from part_t1_1_prt_other_b_2_prt_2_3_prt_others_d;

alter table part_t1_1_prt_other_b_2_prt_2_3_prt_others_d shrink table to 2;
select gp_segment_id, * from part_t1_1_prt_other_b_2_prt_2_3_prt_others_d;

-- try to shrink root partition, should success
Alter table part_t1 shrink table to 2;
Select gp_segment_id, count(*) from part_t1 group by gp_segment_id;

drop table part_t1;


-- inherits tables
CREATE TABLE inherit_t1_p1(a int, b int);
CREATE TABLE inherit_t1_p2(a int, b int) INHERITS (inherit_t1_p1);
CREATE TABLE inherit_t1_p3(a int, b int) INHERITS (inherit_t1_p1);
CREATE TABLE inherit_t1_p4(a int, b int) INHERITS (inherit_t1_p2);
CREATE TABLE inherit_t1_p5(a int, b int) INHERITS (inherit_t1_p3);

insert into inherit_t1_p1 select i,i from generate_series(1,10) i;
insert into inherit_t1_p2 select i,i from generate_series(1,10) i;
insert into inherit_t1_p3 select i,i from generate_series(1,10) i;
insert into inherit_t1_p4 select i,i from generate_series(1,10) i;
insert into inherit_t1_p5 select i,i from generate_series(1,10) i;

select count(*) > 0 from inherit_t1_p1 where gp_segment_id = 2;

begin;
alter table inherit_t1_p1 shrink table to 2;
select count(*) > 0 from inherit_t1_p1 where gp_segment_id = 2;
abort;

select count(*) > 0 from inherit_t1_p1 where gp_segment_id = 2;

alter table inherit_t1_p1 shrink table to 2;

select count(*) > 0 from inherit_t1_p1 where gp_segment_id = 2;

DROP TABLE inherit_t1_p1 CASCADE;


--
-- Cannot shrink a native view and transformed view
--
CREATE TABLE shrink_table1(a int) distributed by (a);
CREATE TABLE shrink_table2(a int) distributed by (a);
CREATE VIEW shrink_view AS select * from shrink_table1;
CREATE rule "_RETURN" AS ON SELECT TO shrink_table2
    DO INSTEAD SELECT * FROM shrink_table1;
ALTER TABLE shrink_table2 shrink TABLE to 2;
ALTER TABLE shrink_view shrink TABLE to 2;
ALTER TABLE shrink_table1 shrink TABLE to 2;
drop table shrink_table1 cascade;

--
-- Test shrinking a table with a domain type as distribution key.
--
create domain myintdomain as int4;

create table shrink_domain_tab(d myintdomain, oldseg int4) distributed by(d);
insert into shrink_domain_tab select generate_series(1,10);

update shrink_domain_tab set oldseg = gp_segment_id;

select gp_segment_id, count(*) from shrink_domain_tab group by gp_segment_id;

alter table shrink_domain_tab shrink table to 2;
select gp_segment_id, count(*) from shrink_domain_tab group by gp_segment_id;
select numsegments from gp_distribution_policy where localoid='shrink_domain_tab'::regclass;
drop table shrink_domain_tab;

-- start_ignore
-- We need to do a cluster expansion which will check if there are partial
-- tables, we need to drop the partial tables to keep the cluster expansion
-- run correctly.
reset search_path;
drop schema test_reshuffle cascade;
-- end_ignore
