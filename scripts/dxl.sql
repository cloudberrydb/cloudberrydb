drop table r;
create table r();
alter table r add column a int;
alter table r add column b int;
insert into r select generate_series(1,100), generate_series(1,100);

drop table s;
create table s();
alter table s add column c int;
alter table s add column d int;
insert into s select generate_series(1,100), generate_series(1,100);

select gpoptutils.DumpPlanToFile('select * from r', 'dxl-q1.plstmt');
select gpoptutils.DumpPlanToFile('select * from r,s where r.a=s.c', 'dxl-q2.plstmt');
select gpoptutils.DumpPlanToFile('select * from r,s where r.a<s.c+1 or r.a>s.c', 'dxl-q4.plstmt');
select gpoptutils.DumpPlanToFile('select a, b, sum(d) from r,s group by a,b', 'dxl-q5.plstmt');
select gpoptutils.DumpPlanToFile('select r.a+1 from r', 'dxl-q6.plstmt');
select gpoptutils.DumpPlanToFile('select 1', 'dxl-q7.plstmt');
select gpoptutils.DumpPlanToFile('select * from r,s where r.a<s.c or (r.b<s.d and r.b>s.c)', 'dxl-q14.plstmt');
select gpoptutils.DumpPlanToFile('select case when r.a<s.c then r.a<s.c else r.a<s.c end from r,s', 'dxl-q15.plstmt');
select gpoptutils.DumpPlanToFile('select * from r limit 100', 'dxl-q16.plstmt');
select gpoptutils.DumpPlanToFile('select * from r limit 100 offset 10', 'dxl-q17.plstmt');
select gpoptutils.DumpPlanToFile('select random() from r', 'dxl-q20.plstmt');
select gpoptutils.DumpPlanToFile('select * from r where r.a > random()', 'dxl-q21.plstmt');
select gpoptutils.DumpPlanToFile('select pow(r.b,r.a) from r', 'dxl-q22.plstmt');
select gpoptutils.DumpPlanToFile('select 0.001 * r.a from r', 'dxl-q23.plstmt');
select gpoptutils.DumpPlanToFile('select case (r.a * random()) when 1 then true else false end from r', 'dxl-q24.plstmt');
select gpoptutils.DumpPlanToFile('select r.b, count(*) from r group by r.b', 'dxl-q25.plstmt');
select gpoptutils.DumpPlanToFile('select sum(r.a)/100 from r', 'dxl-q26.plstmt');
select gpoptutils.DumpPlanToFile('select max(r.a) from r', 'dxl-q27.plstmt');
select gpoptutils.DumpPlanToFile('select * from r limit random()', 'dxl-q28.plstmt');
select gpoptutils.DumpPlanToFile('select r.a, sum(r.b) x from r group by r.a order by x', 'dxl-q29.plstmt');

select gpoptutils.RestorePlanFromFile('dxl-q28.plstmt');
select gpoptutils.RestorePlanFromFile('dxl-q21.plstmt');

select gpoptutils.DumpQueryToFile('select * from r', 'dxl-q1.query');
select gpoptutils.DumpQueryToFile('select * from r,s where r.a=s.c', 'dxl-q2.query');
select gpoptutils.DumpQueryToFile('select * from r,s where r.a<s.c+1 or r.a>s.c', 'dxl-q4.query');
select gpoptutils.DumpQueryToFile('select a, b, sum(d) from r,s group by a,b', 'dxl-q5.query');
select gpoptutils.DumpQueryToFile('select r.a+1 from r', 'dxl-q6.query');
select gpoptutils.DumpQueryToFile('select 1', 'dxl-q7.query');
select gpoptutils.DumpQueryToFile('select * from r,s where r.a<s.c or (r.b<s.d and r.b>s.c)', 'dxl-q14.query');
select gpoptutils.DumpQueryToFile('select case when r.a<s.c then r.a<s.c else r.a<s.c end from r,s', 'dxl-q15.query');
select gpoptutils.DumpQueryToFile('select * from r limit 100', 'dxl-q16.query');
select gpoptutils.DumpQueryToFile('select * from r limit 100 offset 10', 'dxl-q17.query');
select gpoptutils.DumpQueryToFile('select random() from r', 'dxl-q20.query');
select gpoptutils.DumpQueryToFile('select * from r where r.a > random()', 'dxl-q21.query');
select gpoptutils.DumpQueryToFile('select pow(r.b,r.a) from r', 'dxl-q22.query');
select gpoptutils.DumpQueryToFile('select 0.001 * r.a from r', 'dxl-q23.query');
select gpoptutils.DumpQueryToFile('select case (r.a * random()) when 1 then true else false end from r', 'dxl-q24.query');
select gpoptutils.DumpQueryToFile('select r.b, count(*) from r group by r.b', 'dxl-q25.query');
select gpoptutils.DumpQueryToFile('select sum(r.a)/100 from r', 'dxl-q26.query');
select gpoptutils.DumpQueryToFile('select max(r.a) from r', 'dxl-q27.query');
select gpoptutils.DumpQueryToFile('select * from r limit random()', 'dxl-q28.query');
select gpoptutils.DumpQueryToFile('select r.a, sum(r.b) x from r group by r.a order by x', 'dxl-q29.query');
select gpoptutils.DumpPlanToFile('select r.b, count(*) from r group by r.b', 'dxl-q25.plstmt');
select gpoptutils.DumpPlanToFile('select sum(r.a)/100 from r', 'dxl-q26.plstmt');
select gpoptutils.DumpPlanToFile('select max(r.a) from r', 'dxl-q27.plstmt');
select gpoptutils.DumpPlanToFile('select * from r limit random()', 'dxl-q28.plstmt');
select gpoptutils.DumpPlanToFile('select r.a, sum(r.b) x from r group by r.a order by x', 'dxl-q29.plstmt');
select gpoptutils.RestorePlanFromFile('dxl-q28.plstmt');
select gpoptutils.RestorePlanFromFile('dxl-q21.plstmt');
select gpoptutils.DumpPlanToFile('select * from r where (a = b) is unknown', 'dxl-q33.plstmt');
select gpoptutils.RestoreQueryFromFile('dxl-q28.query');
select gpoptutils.RestoreQueryFromFile('dxl-q21.query');

drop table t1;
CREATE TABLE t1();
alter table t1 add column c1 int; 
alter table t1 add column c2 varchar;

select gpoptutils.DumpPlanToFile('select * from t1 where t1.c2 is null', 'dxl-q30.plstmt');
select gpoptutils.DumpPlanToFile('select * from t1 where t1.c1 in (10, 20, 30, 40)', 'dxl-q32.plstmt' );

select gpoptutils.DumpQueryToFile('select * from t1 where t1.c2 is null', 'dxl-q30.query');
select gpoptutils.DumpQueryToFile('select * from t1 where t1.c1 in (10, 20, 30, 40)', 'dxl-q32.query' );

drop table foo;
create table foo();
alter table foo add column x text;
alter table foo add column y int;
insert into foo select 'Darwin.i386-m32.debug/CGroupStateMachineTest.o ./src/unittest/gpopt/operators/.obj.Darwin.i386-m32.debug/CExpressionTest.o ./src/unittest/gpopt/metadata/.obj.Darwin.i386-m32.debug/CTableDescriptorTest.o ./src/unittest/gpopt/metadata/.obj.Darwin.i386-m32.debug/CNameTest.o ./src/unittest/gpopt/metadata/.obj.Darwin.i386-m32.debug/CColumnDescriptorTest.o ./src/unittest/gpopt/engine/.obj.Darwin.i386-m32.debug/CEngineTest.o ./src/unittest/gpopt/base/.obj.Darwin.i386-m32.debug/CStateMachineTest.o ./src/unittest/gpopt/base/.obj.Darwin.i386-m32.debug/CColumnFactoryTest.o ./src/unittest/gpopt/base/.obj.Darwin.i386-m32.debug/CColRefSetTest.o ./src/unittest/gpopt/base/.obj.Darwin.i386-m32.debug/CColRefSetIterTest.o ./src/unittest/gpdb/.obj.Darwin.i386-m32.debug/CStubsTest.o ./src/unittest/dxl/.obj.Darwin.i386-m32.debug/CXMLSerializerTest.o ./src/unittest/dxl/.obj.Darwin.i386-m32.debug/CPlStmtTranslatorTest.o ./src/unittest/dxl/.obj.Darwin.i386-m32.debug/CParseHandlerTest.o ./src/unittest/dxl/.obj.Darwin.i386-m32.debug/CParseHandlerManagerTest.o ./src/unittest/dxl/.obj.Darwin.i386-m32.debug/CDXLTranslatorTest.o ./src/unittest/dxl/.obj.Darwin.i386-m32.debug/CDXLMemoryManagerTest.o ./src/unittest/.obj.Darwin.i386-m32.debug/CUnittest.o ./src/startup/.obj.Darwin.i386-m32.debug/main.o -o .obj.Darwin.i386-m32.debug/gpo  -L../libgpos/.obj.Darwin.i386-m32.debug  -L../libgpopt/.obj.Darwin.i386-m32.debug', generate_series(1, 100);

select gpoptutils.DumpPlanToFile('select x::char(20) from foo', 'dxl-q31.plstmt');
select gpoptutils.DumpQueryToFile('select x::char(20) from foo', 'dxl-q31.query');

alter table s add column e int;

select gpoptutils.DumpPlanToFile('select * from s where s.c<s.d+s.e', 'dxl-q8.plstmt');
select gpoptutils.DumpPlanToFile('select * from r where r.a<r.b+5 or r.a>10', 'dxl-q9.plstmt');
select gpoptutils.DumpPlanToFile('select * from r where r.a<r.b+5 or (r.a>10 and r.b>20)', 'dxl-q10.plstmt');
select gpoptutils.DumpPlanToFile('select * from r where r.a<r.b+5 and r.a>10 and r.b>20', 'dxl-q11.plstmt');
select gpoptutils.DumpPlanToFile('select case when (r.a <= 10) then 1 when (r.a > 10 AND r.a <100) then 2 else 3 end from r', 'dxl-q12.plstmt');
select gpoptutils.DumpPlanToFile('select r.a + r.b from r', 'dxl-q13.plstmt');

select gpoptutils.DumpQueryToFile('select * from s where s.c<s.d+s.e', 'dxl-q8.query');
select gpoptutils.DumpQueryToFile('select * from r where r.a<r.b+5 or r.a>10', 'dxl-q9.query');
select gpoptutils.DumpQueryToFile('select * from r where r.a<r.b+5 or (r.a>10 and r.b>20)', 'dxl-q10.query');
select gpoptutils.DumpQueryToFile('select * from r where r.a<r.b+5 and r.a>10 and r.b>20', 'dxl-q11.query');
select gpoptutils.DumpQueryToFile('select case when (r.a <= 10) then 1 when (r.a > 10 AND r.a <100) then 2 else 3 end from r', 'dxl-q12.query');
select gpoptutils.DumpQueryToFile('select r.a + r.b from r', 'dxl-q13.query');

drop table r;
create table r(a int not null, b int not null);

drop table s;
create table s(
c int not null,
d int not null);

select gpoptutils.DumpPlanToFile('select * from r, s where r.a=s.c', 'dxl-q3.plstmt');

select gpoptutils.DumpQueryToFile('select * from r, s where r.a=s.c', 'dxl-q3.query');

drop table t;
create table t();
alter table t add column a CHAR(25) NOT NULL;
alter table t add column b DECIMAL(15,2);

select gpoptutils.DumpPlanToFile('select * from t where t.a = \'FRANCE\' ', 'dxl-q18.plstmt');
select gpoptutils.DumpPlanToFile('select * from t where t.b > 10.34', 'dxl-q19.plstmt');

select gpoptutils.DumpQueryToFile('select * from t where t.a = \'FRANCE\' ', 'dxl-q18.query');
select gpoptutils.DumpQueryToFile('select * from t where t.b > 10.34', 'dxl-q19.query');


drop table r;
drop table s;
create table r(a int, b int);
create table s(c int, d int);
select gpoptutils.DumpPlanToFile('select * from r full outer join s on (a=c)', 'dxl-q19');
select gpoptutils.DumpPlanToFile('select distinct a from r', 'dxl-q40.plstmt');
select gpoptutils.DumpPlanToFile('select * from r r1, r r2', 'dxl-q41.plstmt');

drop table p;
create table p(a int, b int) partition by range(a) (start (1) end(5) every(1), default partition extra);
select gpoptutils.DumpPlanToFile('select * from p where a=1', 'dxl-q39.plstmt');

drop table t1;
create table t1();
alter table t1 add column a int;
alter table t1 add column b int;

drop table t2;
create table t2();
alter table t2 add column a int;
alter table t2 add column b int;

drop table t3;
create table t3();
alter table t3 add column a int;
alter table t3 add column b int;

select gpoptutils.DumpPlanToFile('select * from t1 where t1.a < (select min(t2.a) from t2) order by t1.a', 'dxl-q34.plstmt');
select gpoptutils.DumpPlanToFile('select * from t1 where t1.a < (select min(t2.a) from t2) and t1.b < (select min(t3.b) from t3) order by t1.a', 'dxl-q35.plstmt');
select gpoptutils.DumpPlanToFile('select * from t1 where t1.a < (select min(t2.a) from t2 where t2.b < (select min(t3.b) from t3)) order by t1.a', 'dxl-q36.plstmt');


select gpoptutils.DumpPlanToFile('select * from r full outer join s on (a=c)','dxl-q43.plstmt');

create table p (a int, b int, c int) partition by range(c) (start(1) end(5) every(1), default partition extra);
insert into p select generate_series(1,1000), generate_series(1,1000) * 3, generate_series(1,1000)%6;
select gpoptutils.DumpPlanToFile('select * from p where c < 3','dxl-q44.plstmt');
select gpoptutils.DumpPlanToFile('select * from r where a in (1,2) and (a=b) is true','dxl-q45.plstmt');

select gpoptutils.DumpPlanToFile('select distinct a from r','dxl-q46.plstmt');
select gpoptutils.DumpPlanToFile('select * from r r1, r r2','dxl-q47.plstmt');
