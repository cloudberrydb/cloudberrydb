--
-- Test correlated subquery in subplan with motion chooses correct scan type
--
-- Given I have two distributed tables
create table choose_seqscan_t1(id1 int,id2 int);
create table choose_seqscan_t2(id1 int,id2 int);
-- and they have some data
insert into choose_seqscan_t1 select i+1,i from generate_series(1,50)i;
insert into choose_seqscan_t2 select i+1,i from generate_series(1,50)i;
-- and one of the tables has an index on a column which is not the distribution column
create index bidx2 on choose_seqscan_t2(id2);
-- and the statistics reflect the newly inserted data
analyze choose_seqscan_t1; analyze choose_seqscan_t2;
-- making an indexscan cheaper with this GUC is only necessary with this small dataset
-- if you insert more data, you can still ensure an indexscan is considered
set random_page_cost=1;
-- and I query the table with the index from inside a subquery which will be pulled up inside of a subquery that will stay a subplan
select (select id1 from (select * from choose_seqscan_t2) foo where id2=choose_seqscan_t1.id2) from choose_seqscan_t1 order by id1;
explain select (select id1 from (select * from choose_seqscan_t2) foo where id2=choose_seqscan_t1.id2) from choose_seqscan_t1;
-- then, a sequential scan is chosen because I need a motion to move choose_seqscan_t2

-- start_ignore
drop table if exists choose_seqscan_t1;
drop table if exists choose_seqscan_t2;
-- end_ignore

-- Given I have one replicated table
create table choose_indexscan_t1(id1 int, id2 int);
create table choose_indexscan_t2(id1 int, id2 int) distributed replicated;
-- and it has data
insert into choose_indexscan_t1 select i+1, i from generate_series(1,20)i;
insert into choose_indexscan_t2 select i+1, i from generate_series(1,100)i;
-- and the replicated table has an index on a column which is not the distribution key
create index choose_indexscan_t2_idx on choose_indexscan_t2(id2);
-- and the statistics reflect the newly inserted data
analyze choose_indexscan_t1; analyze choose_indexscan_t2;
-- making an indexscan cheaper with this GUC is only necessary with this small dataset
-- if you insert more data, you can still ensure an indexscan is considered
set random_page_cost=1;
-- and I query the table with the index from inside a subquery which will be pulled up inside of a subquery that will stay a subplan
select (select id1 from (select * from choose_indexscan_t2) foo where id2=choose_indexscan_t1.id2) from choose_indexscan_t1 order by id1;
explain select (select id1 from (select * from choose_indexscan_t2) foo where id2=choose_indexscan_t1.id2) from choose_indexscan_t1;
-- then an indexscan is chosen because it is correct to do this on a replicated table since no motion is required

-- start_ignore
drop table if exists choose_indexscan_t1;
drop table if exists choose_indexscan_t2;
-- end_ignore
