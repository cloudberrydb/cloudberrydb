drop table if exists test_exec;
create table test_exec (a integer);
insert into test_exec select a from generate_series(1,100) a;

set log_min_messages=debug5;
select count(*) from test_exec;

set debug_print_slice_table=on;
select count(*) from test_exec;

--FOR UPDATE/FOR SHARE
select * from test_exec order by a limit 1 for update;

select * from test_exec order by a limit 1 for share;

--returning clause
insert into test_exec values (1) returning *;


drop table if exists test_exec;
create table test_exec (a integer);
insert into test_exec select a from generate_series(1,5) a;

set log_min_messages=debug5;
select * from test_exec limit null;
select * from test_exec limit 0;

