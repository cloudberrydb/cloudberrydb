-- start_ignore
-- LAST MODIFIED:
--     2009-07-17 mgilkey
--         Added "order" directive.  Because this specifies columns by
--         position, not name, it requires that the columns come out in a
--         known order, so I changed the "SELECT" clauses to specify the
--         columns individually rather than use "SELECT *".
--         I also changed the "explain analyze ..." to specify column names, 
--         too, so that we would be analyzing exactly the same statement as 
--         we are executing.
-- end_ignore
drop table if exists test;
create table test (a integer, b integer);
insert into test select a, a%25 from generate_series(1,100) a;

create index test_b on test (b);

set enable_seqscan=off;
set enable_bitmapscan=off;
set enable_indexscan=on;

\echo -- start_ignore
select a, b from test where b=10 order by b desc;
\echo -- end_ignore

\echo -- order 2
select a, b from test where b=10 order by b desc;
drop table if exists test;
create table test (a integer, b integer);

insert into test select a%10, a%25 from generate_series(1,100) a;

create index t_ab on test (a,b);

set enable_bitmapscan=off;
set enable_seqscan=off;
set enable_indexscan=on;

select * from test where (a,b) < (0,10);
