set allow_system_table_mods=true;

-- Hash distributed tables
Create table t1_reshuffle(a int, b int, c int);
update gp_distribution_policy  set numsegments=2 where localoid='t1_reshuffle'::regclass;
insert into t1_reshuffle select i,i,0 from generate_series(1,100) I;
Update t1_reshuffle set c = gp_segment_id;
Select count(*) from t1_reshuffle where gp_segment_id=0;
Select count(*) from t1_reshuffle where gp_segment_id=1;
Select count(*) from t1_reshuffle where gp_segment_id=2;
Alter table t1_reshuffle set with (reshuffle);
Select count(*) from t1_reshuffle where gp_segment_id=0;
Select count(*) from t1_reshuffle where gp_segment_id=1;
Select count(*) from t1_reshuffle where gp_segment_id=2;
select numsegments from gp_distribution_policy where localoid='t1_reshuffle'::regclass;
drop table t1_reshuffle;

Create table t1_reshuffle(a int, b int, c int);
update gp_distribution_policy  set numsegments=1 where localoid='t1_reshuffle'::regclass;
insert into t1_reshuffle select i,i,0 from generate_series(1,100) I;
Update t1_reshuffle set c = gp_segment_id;
Select count(*) from t1_reshuffle where gp_segment_id=0;
Select count(*) from t1_reshuffle where gp_segment_id=1;
Select count(*) from t1_reshuffle where gp_segment_id=2;
Alter table t1_reshuffle set with (reshuffle);
Select count(*) from t1_reshuffle where gp_segment_id=0;
Select count(*) from t1_reshuffle where gp_segment_id=1;
Select count(*) from t1_reshuffle where gp_segment_id=2;
select numsegments from gp_distribution_policy where localoid='t1_reshuffle'::regclass;
drop table t1_reshuffle;

Create table t1_reshuffle(a int, b int, c int) distributed by (a,b);
update gp_distribution_policy  set numsegments=2 where localoid='t1_reshuffle'::regclass;
insert into t1_reshuffle select i,i,0 from generate_series(1,100) I;
Update t1_reshuffle set c = gp_segment_id;
Select count(*) from t1_reshuffle where gp_segment_id=0;
Select count(*) from t1_reshuffle where gp_segment_id=1;
Select count(*) from t1_reshuffle where gp_segment_id=2;
Alter table t1_reshuffle set with (reshuffle);
Select count(*) from t1_reshuffle where gp_segment_id=0;
Select count(*) from t1_reshuffle where gp_segment_id=1;
Select count(*) from t1_reshuffle where gp_segment_id=2;
select numsegments from gp_distribution_policy where localoid='t1_reshuffle'::regclass;
drop table t1_reshuffle;

Create table t1_reshuffle(a int, b int, c int) distributed by (a,b);
update gp_distribution_policy  set numsegments=1 where localoid='t1_reshuffle'::regclass;
insert into t1_reshuffle select i,i,0 from generate_series(1,100) I;
Update t1_reshuffle set c = gp_segment_id;
Select count(*) from t1_reshuffle where gp_segment_id=0;
Select count(*) from t1_reshuffle where gp_segment_id=1;
Select count(*) from t1_reshuffle where gp_segment_id=2;
Alter table t1_reshuffle set with (reshuffle);
Select count(*) from t1_reshuffle where gp_segment_id=0;
Select count(*) from t1_reshuffle where gp_segment_id=1;
Select count(*) from t1_reshuffle where gp_segment_id=2;
select numsegments from gp_distribution_policy where localoid='t1_reshuffle'::regclass;
drop table t1_reshuffle;

Create table t1_reshuffle(a int, b int, c int) distributed by (a) partition by list(b) (partition t1_reshuffle_1 values(1), partition t1_reshuffle_2 values(2), default partition other);
update gp_distribution_policy set numsegments = 1 where localoid='t1_reshuffle_1_prt_t1_reshuffle_1'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='t1_reshuffle_1_prt_t1_reshuffle_2'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='t1_reshuffle_1_prt_other'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='t1_reshuffle'::regclass;
insert into t1_reshuffle select i,i,0 from generate_series(1,100) I;
Alter table t1_reshuffle set with (reshuffle);
Select count(*) from t1_reshuffle;
Select count(*) > 0 from t1_reshuffle where gp_segment_id=1;
Select count(*) > 0 from t1_reshuffle where gp_segment_id=2;
drop table t1_reshuffle;

Create table t1_reshuffle(a int, b int, c int) distributed by (a) partition by list(b) (partition t1_reshuffle_1 values(1), partition t1_reshuffle_2 values(2), default partition other);
update gp_distribution_policy set numsegments = 2 where localoid='t1_reshuffle_1_prt_t1_reshuffle_1'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='t1_reshuffle_1_prt_t1_reshuffle_2'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='t1_reshuffle_1_prt_other'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='t1_reshuffle'::regclass;
insert into t1_reshuffle select i,i,0 from generate_series(1,100) I;
Alter table t1_reshuffle set with (reshuffle);
Select count(*) from t1_reshuffle;
Select count(*) > 0 from t1_reshuffle where gp_segment_id=2;
drop table t1_reshuffle;

Create table t1_reshuffle(a int, b int, c int) distributed by (a,b) partition by list(b) (partition t1_reshuffle_1 values(1), partition t1_reshuffle_2 values(2), default partition other);
update gp_distribution_policy set numsegments = 1 where localoid='t1_reshuffle_1_prt_t1_reshuffle_1'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='t1_reshuffle_1_prt_t1_reshuffle_2'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='t1_reshuffle_1_prt_other'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='t1_reshuffle'::regclass;
insert into t1_reshuffle select i,i,0 from generate_series(1,100) I;
Alter table t1_reshuffle set with (reshuffle);
Select count(*) from t1_reshuffle;
Select count(*) > 0 from t1_reshuffle where gp_segment_id=1;
Select count(*) > 0 from t1_reshuffle where gp_segment_id=2;
drop table t1_reshuffle;

Create table t1_reshuffle(a int, b int, c int) distributed by (a,b) partition by list(b) (partition t1_reshuffle_1 values(1), partition t1_reshuffle_2 values(2), default partition other);
update gp_distribution_policy set numsegments = 2 where localoid='t1_reshuffle_1_prt_t1_reshuffle_1'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='t1_reshuffle_1_prt_t1_reshuffle_2'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='t1_reshuffle_1_prt_other'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='t1_reshuffle'::regclass;
insert into t1_reshuffle select i,i,0 from generate_series(1,100) I;
Alter table t1_reshuffle set with (reshuffle);
Select count(*) from t1_reshuffle;
Select count(*) > 0 from t1_reshuffle where gp_segment_id=2;
drop table t1_reshuffle;


-- Random distributed tables
Create table r1_reshuffle(a int, b int, c int) distributed randomly;
update gp_distribution_policy  set numsegments=1 where localoid='r1_reshuffle'::regclass;
insert into r1_reshuffle select i,i,0 from generate_series(1,100) I;
Update r1_reshuffle set c = gp_segment_id;
Select count(*) from r1_reshuffle;
Alter table r1_reshuffle set with (reshuffle);
Select count(*) from r1_reshuffle;
Select count(*) > 0 from r1_reshuffle where gp_segment_id=2;
drop table r1_reshuffle;

Create table r1_reshuffle(a int, b int, c int) distributed randomly;
update gp_distribution_policy  set numsegments=2 where localoid='r1_reshuffle'::regclass;
insert into r1_reshuffle select i,i,0 from generate_series(1,100) I;
Update r1_reshuffle set c = gp_segment_id;
Select count(*) from r1_reshuffle;
Alter table r1_reshuffle set with (reshuffle);
Select count(*) from r1_reshuffle;
Select count(*) > 0 from r1_reshuffle where gp_segment_id=2;
drop table r1_reshuffle;

Create table r1_reshuffle(a int, b int, c int) distributed randomly partition by list(b) (partition r1_reshuffle_1 values(1), partition r1_reshuffle_2 values(2), default partition other);
update gp_distribution_policy set numsegments = 1 where localoid='r1_reshuffle_1_prt_r1_reshuffle_1'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='r1_reshuffle_1_prt_r1_reshuffle_2'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='r1_reshuffle_1_prt_other'::regclass;
update gp_distribution_policy set numsegments = 1 where localoid='r1_reshuffle'::regclass;
insert into r1_reshuffle select i,i,0 from generate_series(1,100) I;
Alter table r1_reshuffle set with (reshuffle);
Select count(*) from r1_reshuffle;
Select count(*) > 0 from r1_reshuffle where gp_segment_id=1;
Select count(*) > 0 from r1_reshuffle where gp_segment_id=2;
drop table r1_reshuffle;

Create table r1_reshuffle(a int, b int, c int) distributed randomly partition by list(b) (partition r1_reshuffle_1 values(1), partition r1_reshuffle_2 values(2), default partition other);
update gp_distribution_policy set numsegments = 2 where localoid='r1_reshuffle_1_prt_r1_reshuffle_1'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='r1_reshuffle_1_prt_r1_reshuffle_2'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='r1_reshuffle_1_prt_other'::regclass;
update gp_distribution_policy set numsegments = 2 where localoid='r1_reshuffle'::regclass;
insert into r1_reshuffle select i,i,0 from generate_series(1,100) I;
Alter table r1_reshuffle set with (reshuffle);
Select count(*) from r1_reshuffle;
Select count(*) > 0 from r1_reshuffle where gp_segment_id=1;
Select count(*) > 0 from r1_reshuffle where gp_segment_id=2;
drop table r1_reshuffle;

-- Replicated tables
Create table r1_reshuffle(a int, b int, c int) distributed replicated;
update gp_distribution_policy  set numsegments=1 where localoid='r1_reshuffle'::regclass;
insert into r1_reshuffle select i,i,0 from generate_series(1,100) I;
Select count(*) from r1_reshuffle;
Alter table r1_reshuffle set with (reshuffle);
Select count(*) from r1_reshuffle;
drop table r1_reshuffle;

Create table r1_reshuffle(a int, b int, c int) distributed replicated;
update gp_distribution_policy  set numsegments=2 where localoid='r1_reshuffle'::regclass;
insert into r1_reshuffle select i,i,0 from generate_series(1,100) I;
Select count(*) from r1_reshuffle;
Alter table r1_reshuffle set with (reshuffle);
Select count(*) from r1_reshuffle;
drop table r1_reshuffle;

-- table with update triggers on distributed key column
CREATE OR REPLACE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS '
BEGIN
	RAISE NOTICE ''trigger_func(%) called: action = %, when = %, level = %'', TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;';

CREATE TABLE table_with_update_trigger(a int, b int, c int);
update gp_distribution_policy set numsegments=2 where localoid='table_with_update_trigger'::regclass;
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
-- reshuffle should success and not hiting any triggers.
Alter table table_with_update_trigger set with (reshuffle);
select gp_segment_id, count(*) from table_with_update_trigger group by 1 order by 1;
