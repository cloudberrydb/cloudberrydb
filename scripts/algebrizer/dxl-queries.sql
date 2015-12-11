drop table r;
create table r(a int, b int);

drop table s;
create table s(c int, d int);

drop table r1;
create table r1(a int, b int);

select gpoptutils.DumpQueryDXL('select * from r');
select gpoptutils.DumpQueryDXL('select * from r where a > 10 and b > 100');
select gpoptutils.DumpQueryDXL('select 1');
select gpoptutils.DumpQueryDXL('select 1 where random() > 2');
select gpoptutils.DumpQueryDXL('select r.a+1 from r');
select gpoptutils.DumpQueryDXL('select * from r, s where r.a = s.d');
select gpoptutils.DumpQueryDXL('select * from r,s, r1 where r.a = s.c and r1.a = r.a and r1.b = s.d');
select gpoptutils.DumpQueryDXL('select * from r left join s on (r.a = s.c)');
select gpoptutils.DumpQueryDXL('select r1.b+r.b+s.d from r inner join r1 on (r.a = r1.a) left join s on (r.a = s.c)');
select gpoptutils.DumpQueryDXL('select * from r, (select s.c as c, s.d as d, s.c+s.d as e from s) as t where t.c = r.a and t.e = r.b');
select gpoptutils.DumpQueryDXL('select * from r where a = (select max(a) from r)');
select gpoptutils.DumpQueryDXL('select * from r where a = any (select b from r) and b = all (select a from r)');
select gpoptutils.DumpQueryDXL('select 1, count(*) + sum(b) as t from r group by a order by t');
select gpoptutils.DumpQueryDXL('select sum(distinct a) from t group by b having count(distinct a) > 10 order by b');

drop table foo;
create table foo(a int, b int, c int, d int, e int);
select gpoptutils.DumpQueryDXL('select r1.b+r.b+s.d from r inner join r1 on (r.a = r1.a) left join s on (r.a = s.c) left join foo on (s.d = foo.b)');
select gpoptutils.DumpQueryDXL('select max(b) from r');
select gpoptutils.DumpQueryDXL('select a, count(distinct b) from r group by a');
select gpoptutils.DumpQueryDXL('select a+b as c, count(*) from r group by c');
select gpoptutils.DumpQueryDXL('select * from r where a = b');
select gpoptutils.DumpQueryDXL('select * from r where a = 5');
select gpoptutils.DumpQueryDXL('select a, b, count(*) from r group by a, b');

--- do not support the rollup/cube
--- select gpoptutils.DumpQueryDXL('select b, c, d, count(*) from foo GROUP BY ROLLUP (b, c, (b,c), (b,c,d))');
--- select gpoptutils.DumpQueryDXL('select a+d as g, a+c as h, count(distinct a) * random() from foo GROUP BY CUBE(g,h)');

select gpoptutils.DumpQueryDXL('select * from r INNER JOIN s ON (s.c = r.a)');
select gpoptutils.DumpQueryDXL('select * from r where a > b + 2');
select gpoptutils.DumpQueryDXL('select * from r where a = (select max(a) from r)');
select gpoptutils.DumpQueryDXL('select * from r where a = any (select b from r) and b = all (select a from r)');
select gpoptutils.DumpQueryDXL('select * from R where exists (select b from R) and not exists (select a from R)');
select gpoptutils.DumpQueryDXL('select * from r where a in (1,2)');

select gpoptutils.DumpQueryDXL('select a, count(*) as c from r group by a order by c');
select gpoptutils.DumpQueryDXL('select * from r order by a limit 100 offset 10');
select gpoptutils.DumpQueryDXL('select * from (select s.c as c, s.d as d, s.c+s.d as e from s ) as t');
select gpoptutils.DumpQueryDXL('select count(distinct(b)) from r group by a');
select gpoptutils.DumpQueryDXL('select * from R where exists (select b from R) and not exists (select a from R)');
select gpoptutils.DumpQueryDXL('select * from R R1 where exists (select * from R R2 where R1.a=R2.a)');
select gpoptutils.DumpQueryDXL('select * from R where exists (select * from R)');
