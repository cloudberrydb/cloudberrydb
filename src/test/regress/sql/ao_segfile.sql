create schema ao_segfile;
set search_path to ao_segfile;
set gp_appendonly_insert_files = 4;

-- ao table
create table ao_copy (a int) using ao_row;
select segfilecount from pg_appendonly where relid = 'ao_copy'::regclass;
set gp_appendonly_insert_files_tuples_range = 1;
-- ensure 4 files on 3 segments
COPY ao_copy from stdin;
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
\.

analyze ao_copy;
select count(*) from ao_copy;
select segfilecount from pg_appendonly where relid = 'ao_copy'::regclass;
reset gp_appendonly_insert_files_tuples_range;

-- aocs table
create table aocs_copy (a int) using ao_column;
select segfilecount from pg_appendonly where relid = 'aocs_copy'::regclass;
set gp_appendonly_insert_files_tuples_range = 1;
-- ensure 4 files on 3 segments
COPY aocs_copy from stdin;
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
\.

analyze aocs_copy;
select count(*) from aocs_copy;
select segfilecount from pg_appendonly where relid = 'aocs_copy'::regclass;
reset gp_appendonly_insert_files_tuples_range;
reset gp_appendonly_insert_files;

-- start_ignore
drop schema ao_segfile cascade;
-- end_ignore
