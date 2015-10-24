drop table if exists test;
create table test (a integer);
insert into test select a from generate_series(1,100) a;

set log_min_messages=debug5;
select count(*) from test;

set debug_print_slice_table=on;
select count(*) from test;

--FOR UPDATE/FOR SHARE
select * from test order by a limit 1 for update;

select * from test order by a limit 1 for share;

--returning clause
insert into test values (1) returning *;


drop table if exists test;
create table test (a integer);
insert into test select a from generate_series(1,5) a;

set log_min_messages=debug5;
select * from test limit null;
select * from test limit 0;

