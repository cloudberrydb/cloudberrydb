-- Test effective distribution policy after different variants of CREATE TABLE


-- Make sure random default distribution works for CTAS
SET gp_create_table_random_default_distribution=on;
DROP TABLE IF EXISTS distpol;
create table distpol as select random(), 1 as a, 2 as b;

select attrnums from gp_distribution_policy where localoid = 'distpol'::regclass;


-- Test RANDOM default distribution with AS clause containing a SELECT block
DROP TABLE IF EXISTS hobbies_r;
CREATE TABLE hobbies_r (
  name text,
  person text
);

DROP TABLE IF EXISTS bar;
CREATE TABLE bar AS SELECT * FROM hobbies_r;

select attrnums from gp_distribution_policy where localoid='bar'::regclass;

-- Test RANDOM distribution with ON COMMIT option
begin;

create temp table r3_1 on commit preserve rows as select 10 as a1, 20 as b1, 30 as c1, 40 as d1;
select attrnums from gp_distribution_policy where localoid='r3_1'::regclass;

create temp table r3_2 on commit delete rows as select 10 as a2, 20 as b2, 30 as c2, 40 as d2;
select attrnums from gp_distribution_policy where localoid='r3_2'::regclass;

create temp table r3_3 on commit drop as select 10 as a3, 20 as b3, 30 as c3, 40 as d3;
select attrnums from gp_distribution_policy where localoid='r3_3'::regclass;

end;

RESET gp_create_table_random_default_distribution;

-- Test that distribution policy is not inherited and it is RANDOM in CREATE TABLE with default distribution set to random
SET gp_create_table_random_default_distribution=on;
DROP TABLE IF EXISTS person CASCADE;
CREATE TABLE person (
  name      text,
  age       int4,
  location  point
) DISTRIBUTED BY (name);

CREATE TABLE staff_member (
  salary    int4,
  manager   name
) INHERITS (person) WITH OIDS;
select attrnums from gp_distribution_policy where localoid = 'staff_member'::regclass;

CREATE TABLE student (
  gpa      float8
) INHERITS (person);
select attrnums from gp_distribution_policy where localoid = 'student'::regclass;

CREATE TABLE stud_emp (
  percent  int4
) INHERITS (staff_member, student);
select attrnums from gp_distribution_policy where localoid = 'stud_emp'::regclass;

RESET gp_create_table_random_default_distribution;

-- Test that LIKE clause does not affect default distribution
SET gp_create_table_random_default_distribution=on;
set client_min_messages='warning';
DROP TABLE IF EXISTS person CASCADE;
reset client_min_messages;
CREATE TABLE person (
  name      text,
  age       int4,
  location  point
) DISTRIBUTED BY (name);
select attrnums from gp_distribution_policy where localoid = 'person'::regclass;

DROP TABLE IF EXISTS person_copy;
CREATE TABLE person_copy (LIKE person);
select attrnums from gp_distribution_policy where localoid = 'person_copy'::regclass;

RESET gp_create_table_random_default_distribution;
