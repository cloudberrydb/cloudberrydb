-- 
-- Table: r
-- 

drop table r;
create table r(a int, b int);

insert into r values (1 , 1);
insert into r values (2 , 1);
insert into r values (3 , 1);
insert into r values (4 , 1);
insert into r values (5 , 1);
insert into r values (5 , 1);
insert into r values (6 , 1);
insert into r values (6 , 3);
insert into r values (6 , 2);
insert into r values (7 , 1);
insert into r values (8 , 1);
insert into r values (9 , 2);
insert into r values (9 , 1);

-- 
-- Table: s
-- 

drop table s;
create table s(c int, d int, e int);

insert into s values (1 , 1 , 1);  
insert into s values (1 , 3 , 1);  
insert into s values (2 , 2 , 1);  
insert into s values (3 , 3 , 1);  
insert into s values (3 , 2 , 2);  
insert into s values (3 , 1 , 3);  
insert into s values (5 , 2 , 1);  
insert into s values (5 , 1 , 2);  
insert into s values (6 , 2 , 3);  
insert into s values (7 , 1 , 1);  
insert into s values (7 , 3 , 2);  
insert into s values (9 , 1 , 3);  
insert into s values (10 , 3 , 1);  
insert into s values (11 , 3 , 2);

analyze r;
analyze s;

--
-- Enable the optimizer
--

set gp_optimizer=on;

-- 
-- Display Table Contents
-- 

select * from r;
select * from s;

-- 
-- Query 1
-- 

select count(*) from r;

-- 
-- Query 2
-- 

select b, count(*), min(a), max(a), sum(a), min(c), max(c), sum(c) from r, s where r.b = s.d group by b order by b;

-- 
-- Query 3
-- 

select a, count(*), min(b), max(b), sum(b), min(d), max(d), sum(d) from r, s where r.a = s.c group by a order by a;

-- 
-- Query 4
-- 

select count(distinct a), sum(distinct a) from r;

-- 
-- Query 5
-- 

select count(distinct b), sum(distinct b) from r;

-- 
-- Query 6
-- 

select count(distinct b), sum(distinct b) from r group by a;

-- 
-- Query 7
-- 

select count(distinct a), sum(distinct a) from r group by b;

-- 
-- Query 8
-- 

select count(distinct c), sum(distinct c) from s group by d,e;

-- 
-- Query 9
-- 

select count(distinct a), sum(distinct a) as t from r order by t;

-- 
-- Query 10
-- 

select count(distinct b), sum(distinct b) as t from r group by a order by a,t ;

-- 
-- Query 11
-- 

select count(distinct a), sum(distinct a) as t from r group by b order by b, t;

-- 
-- Query 12
-- 

select count(distinct c), sum(distinct c) from s group by d,e order by e;

-- 
-- Query 13
-- 

select count(distinct c+d) from s;

-- 
-- Query 14
-- 

select count(distinct c+d) from s group by e;

-- 
-- Query 15
-- 

select count(distinct a), sum(distinct b) from r;

-- 
-- Query 16
-- 

select count(distinct c), sum(distinct d) from s;

-- 
-- Query 17
-- 

select count(distinct c), sum(distinct d) from s group by e;
