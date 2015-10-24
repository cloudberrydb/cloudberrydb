--start_ignore
create language plpgsql;
drop table foo cascade;
drop table tenk1 cascade;
--end_ignore
CREATE TABLE employee(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO employee values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO employee values (2,'Robert',14420,'01/02/2007','Vacouver');

SELECT * FROM employee;

BEGIN work;

INSERT INTO employee(ID,name) values (106,'Hall');

COMMIT work;

SELECT * FROM employee;

DROP table employee;

---create table in begin and commit transaction---

BEGIN work;
CREATE TABLE test111(id integer,name text);
COMMIT work;

BEGIN work;
DROP TABLE test111;
COMMIT work;


CREATE TABLE Manager(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO Manager values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO Manager values (2,'Robert',14420,'01/02/2007','Vacouver');
INSERT INTO Manager values (3,'swapna',23456,'09/02/2007','Parbhani');
INSERT INTO Manager values (4,'prachi',34567,'07/03/2007','Mumbai');


SELECT * FROM Manager;

BEGIN work;

UPDATE Manager SET name = NULL WHERE id=2;

COMMIT work;

SELECT * FROM Manager;

DROP table Manager;

--- Rolling back a Transaction ---
CREATE TABLE Manager1(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO Manager1 values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO Manager1 values (2,'Robert',14420,'01/02/2007','Vacouver');
INSERT INTO Manager1 values (3,'swapna',23456,'09/02/2007','Parbhani');
INSERT INTO Manager1 values (4,'prachi',34567,'07/03/2007','Mumbai');
INSERT INTO Manager1 values (5,'Pranita',65555,'08/04/2007','Mumbai');
INSERT INTO Manager1 values (6,'Manisha',74444,'07/02/2007','Canada');
INSERT INTO Manager1 values (7,'Anagha',49080,'09/01/2007','Nanded');


SELECT * FROM Manager1;

BEGIN ;
SELECT * FROM Manager1 where id = 1;

UPDATE Manager1 SET name = 'Atharva' WHERE id=1;
SELECT * FROM Manager1 where id = 1;
ROLLBACK;

SELECT * FROM Manager1 WHERE id = 1;

DROP table Manager1;

--- Rolling back a Transaction ---
CREATE TABLE Manager11(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO Manager11 values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO Manager11 values (2,'Robert',14420,'01/02/2007','Vacouver');
INSERT INTO Manager11 values (3,'swapna',23456,'09/02/2007','Parbhani');
INSERT INTO Manager11 values (4,'prachi',34567,'07/03/2007','Mumbai');
INSERT INTO Manager11 values (5,'Pranita',65555,'08/04/2007','Mumbai');
INSERT INTO Manager11 values (6,'Manisha',74444,'07/02/2007','Canada');
INSERT INTO Manager11 values (7,'Anagha',49080,'09/01/2007','Nanded');
INSERT INTO Manager11 values (8,'Varad' ,32100,'01/12/2007','Pune');
INSERT INTO Manager11 values (9,'Vedant',43210,'10/02/2007','Aurangabad');


SELECT * FROM Manager11;

BEGIN ;
DELETE FROM Manager11;

SELECT * FROM Manager11;
ROLLBACK ;

SELECT * FROM Manager11;

DROP table Manager11;

--- Rolling back a Transaction ---
CREATE TABLE Manager10(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO Manager10 values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO Manager10 values (2,'Robert',14420,'01/02/2007','Vacouver');
INSERT INTO Manager10 values (3,'swapna',23456,'09/02/2007','Parbhani');
INSERT INTO Manager10 values (4,'prachi',34567,'07/03/2007','Mumbai');
INSERT INTO Manager10 values (5,'Pranita',65555,'08/04/2007','Mumbai');
INSERT INTO Manager10 values (6,'Manisha',74444,'07/02/2007','Canada');
INSERT INTO Manager10 values (7,'Anagha',49080,'09/01/2007','Nanded');
INSERT INTO Manager10 values (8,'Varad' ,32100,'01/12/2007','Pune');
INSERT INTO Manager10 values (9,'Vedant',43210,'10/02/2007','Aurangabad');


SELECT * FROM Manager10;

BEGIN ;
UPDATE Manager10 SET salary = salary - 100 WHERE name = 'Varad';

SAVEPOINT my_savepoint;

UPDATE Manager10 SET salary = salary + 100 WHERE name = 'Vedant';

ROLLBACK TO my_savepoint;

UPDATE Manager10 SET salary = salary + 100 WHERE name = 'Anagha';

COMMIT;

SELECT * FROM Manager10;

DROP table Manager10;

CREATE TABLE "book" ( id integer NOT NULL,title text NOT NULL,author_id integer,subject_id integer,constraint "book_id_pkey" Primary Key ("id"));

INSERT INTO book values (7808, 'java',4156,9);
INSERT INTO book values (4513, 'javascript',1866,15);
INSERT INTO book values (7809, 'python',4956,9);
INSERT INTO book values (4514, 'C',19866,1);
INSERT INTO book values (7810, 'C++',4336,5);
INSERT INTO book values (4515, 'Perl cookbook',189066,8);
INSERT INTO book values (7811, 'POstgreSql',41156,2);
INSERT INTO book values (4516, 'Oracle',1866,6);

SELECT * FROM book;
BEGIN work;

DELETE FROM book;

ABORT work;

SELECT * FROM book;

DROP TABLE book;


BEGIN;
CREATE TABLE Atable (a integer);
INSERT INTO Atable VALUES (10);
SHOW TRANSACTION ISOLATION LEVEL;

DROP TABLE Atable;
COMMIT;
CREATE TABLE Manager(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO Manager values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO Manager values (2,'Robert',14420,'01/02/2007','Vacouver');
INSERT INTO Manager values (3,'swapna',23456,'09/02/2007','Parbhani');
INSERT INTO Manager values (4,'prachi',34567,'07/03/2007','Mumbai');


SELECT * FROM Manager;

BEGIN work;

SET TRANSACTION ISOLATION LEVEL  SERIALIZABLE;

SHOW TRANSACTION ISOLATION LEVEL;

UPDATE Manager SET name = NULL WHERE id=2;

COMMIT work;

SHOW TRANSACTION ISOLATION LEVEL;

SELECT * FROM Manager;

DROP table Manager;

CREATE TABLE Manager(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO Manager values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO Manager values (2,'Robert',14420,'01/02/2007','Vacouver');
INSERT INTO Manager values (3,'swapna',23456,'09/02/2007','Parbhani');
INSERT INTO Manager values (4,'prachi',34567,'07/03/2007','Mumbai');


SELECT * FROM Manager;

BEGIN work;

SET SESSION CHARACTERISTICS AS  TRANSACTION ISOLATION LEVEL  SERIALIZABLE;

SHOW TRANSACTION ISOLATION LEVEL;

UPDATE Manager SET name = NULL WHERE id=2;

COMMIT work;

SHOW TRANSACTION ISOLATION LEVEL;

SELECT * FROM Manager;

DROP table Manager;

CREATE TABLE Manager(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO Manager values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO Manager values (2,'Robert',14420,'01/02/2007','Vacouver');
INSERT INTO Manager values (3,'swapna',23456,'09/02/2007','Parbhani');
INSERT INTO Manager values (4,'prachi',34567,'07/03/2007','Mumbai');


SELECT * FROM Manager;

BEGIN work;

SHOW TRANSACTION ISOLATION LEVEL;

SET CONSTRAINTS ALL IMMEDIATE;

UPDATE Manager SET name = NULL WHERE id=2;

COMMIT work;

SHOW TRANSACTION ISOLATION LEVEL;

SELECT * FROM Manager;

DROP table Manager;

CREATE TABLE employee(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO employee values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO employee values (2,'Robert',14420,'01/02/2007','Vacouver');

SELECT * FROM employee;

START TRANSACTION ;

INSERT INTO employee(ID,name) values (106,'Hall');

COMMIT work;

SELECT * FROM employee;

DROP table employee;

CREATE TABLE employee(ID int,name varchar(10),salary real,start_date date,city varchar(10),region char(1));

INSERT INTO employee values (1,'jason',40420,'02/01/2007','New York');
INSERT INTO employee values (2,'Robert',14420,'01/02/2007','Vacouver');

SELECT * FROM employee;

START TRANSACTION SERIALIZABLE;

INSERT INTO employee(ID,name) values (106,'Hall');

COMMIT work;

SELECT * FROM employee;

DROP table employee;

create table tabcd (c1 text) distributed randomly;
insert into tabcd values ('a'), ('b'), ('c'), ('d');

create table t1234 (c1 integer) distributed randomly;
insert into t1234 values (1),(2),(3),(4);

create table tabcd_orig as select * from tabcd distributed randomly;
create table t1234_orig as select * from t1234 distributed randomly;

DROP FUNCTION IF EXISTS transaction_test_cursor_nit();
CREATE FUNCTION transaction_test_cursor_nit() RETURNS void AS '
DECLARE
ref_abcd refcursor;
ref_1234_1 refcursor;
ref_1234_2 refcursor;
abcd_var varchar;
t_1234_var_1 int;
t1234_var_2 int;
i int;
j int;
arr_1234 int [4];
arr_abcd varchar [4];
BEGIN
arr_1234[1]:=1;arr_1234[2]:=2;arr_1234[3]:=3;arr_1234[4]:=4;
open ref_1234_1 FOR SELECT c1 FROM t1234 order by 1;
BEGIN
j:=1;
open ref_abcd FOR SELECT c1 FROM tabcd order by 1;
fetch ref_abcd into abcd_var;
while abcd_var is not null loop
arr_abcd[j]:=abcd_var;
BEGIN
open ref_1234_2 FOR SELECT c1 FROM t1234 order by 1;
fetch ref_1234_2 into t1234_var_2;
i:=1;
while t1234_var_2 is not null loop
update tabcd set c1=c1||t1234_var_2 where c1=arr_abcd[j];
arr_abcd[j]:=arr_abcd[j]||t1234_var_2;
arr_1234[i]:=arr_1234[i]+10;
i:=i+1;
fetch ref_1234_2 into t1234_var_2;
end loop;
close ref_1234_2;
END;
fetch ref_abcd into abcd_var;
j:=j+1;
end loop;
close ref_abcd;
fetch ref_1234_1 into t_1234_var_1;
while t_1234_var_1 is not null loop
update t1234 set c1=arr_1234[t_1234_var_1] where c1=t_1234_var_1;
fetch ref_1234_1 into t_1234_var_1;
end loop;
END;
close ref_1234_1;
END;
' LANGUAGE plpgsql MODIFIES SQL DATA;


BEGIN;
TRUNCATE tabcd;
TRUNCATE t1234;
INSERT INTO tabcd SELECT * from tabcd_orig;
INSERT INTO t1234 SELECT * from t1234_orig;
SELECT transaction_test_cursor_nit();
SELECT * from tabcd order by 1;
SELECT * from t1234 order by 1;

SELECT transaction_test_cursor_nit();
ABORT;

BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
TRUNCATE tabcd;
TRUNCATE t1234;
INSERT INTO tabcd SELECT * from tabcd_orig;
INSERT INTO t1234 SELECT * from t1234_orig;
SELECT transaction_test_cursor_nit();
SELECT * from tabcd order by 1;
SELECT * from t1234 order by 1;

SELECT transaction_test_cursor_nit();
ABORT;

DROP TABLE tabcd;
DROP TABLE t1234;
DROP TABLE tabcd_orig;
DROP TABLE t1234_orig;
CREATE SCHEMA prerds;

CREATE OR REPLACE FUNCTION fin_hie_acc_grp1()  returns integer as $$
declare 
c integer :=99;

begin
delete from  prerds.ramana_fin_setleaf_bt ;
Truncate Table  prerds.FINANCE_HIE_ACC_GRP ;
--LEVEL 1
insert into prerds.ramana_fin_hir  
select SUBSETNAME, SETNAME from  stgdb.stg_setnode where
SETCLASS = '0109' and SUBCLASS = 'PCOA' and setname = 'ACC_GRP';
 
Insert into prerds.FINANCE_HIE_ACC_GRP  select * from prerds.ramana_fin_hir;
 

Loop
--NEXT LEVEL

insert into prerds.ramana_fin_hir1  
select   distinct  r.subsetname, r.Setname   
from  stgdb.stg_setnode r,  prerds.ramana_fin_hir  rr
where 
r.SETCLASS = '0109'
and r.SUBCLASS = 'PCOA'
and r.SETNAME= rr.lv1 ;

Insert into prerds.FINANCE_HIE_ACC_GRP  select * from prerds.ramana_fin_hir1 ;


Insert into prerds.FINANCE_HIE_ACC_GRP1 
select   distinct  r.valfrom, r.Setname  
from  stgdb.stg_setleaf r,  prerds.ramana_fin_hir  rr
where 
r.SETCLASS = '0109'
and r.SUBCLASS = 'PCOA' and r.valoption =  'EQ'
and r.SETNAME= rr.lv1 and r.Setname in 
(select r.lv1 from prerds.ramana_fin_hir r left outer join 
prerds.ramana_fin_hir1 rr on r.lv1 = rr.lv2 where rr.lv1 is null);


Insert into prerds.ramana_fin_setleaf_bt 
select   distinct  r.Setname, r.valoption, r.valfrom, r.valto 
from  stgdb.stg_setleaf r,  prerds.ramana_fin_hir  rr
where 
r.SETCLASS = '0109'
and r.SUBCLASS = 'PCOA' and r.valoption =  'BT'
and r.SETNAME= rr.lv1 and r.Setname in 
(select r.lv1 from prerds.ramana_fin_hir r left outer join 
prerds.ramana_fin_hir1 rr on r.lv1 = rr.lv2 where rr.lv1 is null);

Exit when (select count(*) from prerds.ramana_fin_hir) = (select count(r.lv1) from prerds.ramana_fin_hir r left outer join 
prerds.ramana_fin_hir1 rr on r.lv1 = rr.lv2 where rr.lv1 is null);


delete from  prerds.ramana_fin_hir;


--NEXT2
insert into prerds.ramana_fin_hir  
select   distinct  r.subsetname,  r.Setname   
from  stgdb.stg_setnode r,  prerds.ramana_fin_hir1  rr
where 
r.SETCLASS = '0109'
and r.SUBCLASS = 'PCOA'
and r.SETNAME= rr.lv1 ;

Insert into prerds.FINANCE_HIE_ACC_GRP  select * from prerds.ramana_fin_hir ;
 
 
Insert into prerds.FINANCE_HIE_ACC_GRP1 
select   distinct  r.valfrom, r.Setname 
from  stgdb.stg_setleaf r,  prerds.ramana_fin_hir1  rr
where 
r.SETCLASS = '0109'
and r.SUBCLASS = 'PCOA' and r.valoption =  'EQ'
and r.SETNAME= rr.lv1 and r.Setname in 
(select r.lv1 from prerds.ramana_fin_hir1 r left outer join 
prerds.ramana_fin_hir rr on r.lv1 = rr.lv2 where rr.lv1 is null);


Insert into prerds.ramana_fin_setleaf_bt 
select   distinct  r.Setname, r.valoption, r.valfrom, r.valto 

from  stgdb.stg_setleaf r,  prerds.ramana_fin_hir1  rr
where 
r.SETCLASS = '0109'
and r.SUBCLASS = 'PCOA' and r.valoption =  'BT'
and r.SETNAME= rr.lv1 and r.Setname in 
(select r.lv1 from prerds.ramana_fin_hir1 r left outer join 
prerds.ramana_fin_hir rr on r.lv1 = rr.lv2 where rr.lv1 is null);


Exit when (select count(*) from prerds.ramana_fin_hir1) = (select count(r.lv1) from prerds.ramana_fin_hir1 r left outer join 
prerds.ramana_fin_hir rr on r.lv1 = rr.lv2 where rr.lv1 is null);



delete from  prerds.ramana_fin_hir1 ;

End Loop;

Insert into prerds.FINANCE_HIE_ACC_GRP1  
Select   KSTAR, setname from stgdb.stg_CSKU r, prerds.ramana_fin_setleaf_bt rr where r.ktopl = 'PCOA' and 
 r.KSTAR BETWEEN rr.valfrom and rr.valto;

delete from  prerds.ramana_fin_hir1 ;
delete from  prerds.ramana_fin_hir ;



/*------------------------------------CHAGING THE ORDER--------------------------------------*/
/*GRANULAR LEVEL*/
delete from prerds.FINANCE_HIE_ACC_GRP_FINAL ;

Insert into prerds.FINANCE_HIE_ACC_GRP_FINAL select * from prerds.FINANCE_HIE_ACC_GRP1;

/*NEXT LEVELS IN LOOP*/
Loop
delete from prerds.FINANCE_HIE_ACC_GRP_TEMP;

Insert into  prerds.FINANCE_HIE_ACC_GRP_TEMP select a.lv1,b.lv2 from prerds.FINANCE_HIE_ACC_GRP1 a, 
prerds.FINANCE_HIE_ACC_GRP  b where a.lv2 = b.lv1;

GET DIAGNOSTICS c = ROW_COUNT;
exit when c = 0;
Insert into prerds.FINANCE_HIE_ACC_GRP_FINAL select * from prerds.FINANCE_HIE_ACC_GRP_TEMP;

delete from prerds.FINANCE_HIE_ACC_GRP1  ;
Insert into prerds.FINANCE_HIE_ACC_GRP1  select * from prerds.FINANCE_HIE_ACC_GRP_TEMP;
end loop;


delete from prerds.fin_hie__desc_temp ;
insert into prerds.fin_hie__desc_temp 
 select   a.lv1,a.lv2, b.hie_desc  from prerds.FINANCE_HIE_ACC_GRP_FINAL a left outer join prerds.ramana_fin_hie_desc b 
 on a.lv2=hie_lvl ;

delete from prerds.FINANCE_HIE_ACC_GRP_FINAL;
insert into prerds.FINANCE_HIE_ACC_GRP_FINAL select * from prerds.fin_hie__desc_temp ;

Return (select count(*) from prerds.FINANCE_HIE_ACC_GRP_FINAL )  ;
end; 

$$ language plpgsql MODIFIES SQL DATA;


CREATE TABLE prerds.ramana_fin_setleaf_bt
(
  setname character varying(24),
  valoption character varying(2),
  valfrom character varying(24),
  valto character varying(24)
);


CREATE TABLE prerds.finance_hie_acc_grp
(
  lv1 character varying,
  lv2 character varying
);

CREATE TABLE prerds.finance_hie_acc_grp_temp
(
  lv1 character varying,
  lv2 character varying
);


CREATE TABLE prerds.ramana_fin_hir
(
  lv1 character varying,
  lv2 character varying
);


CREATE TABLE prerds.ramana_fin_hir1
(
  lv1 character varying,
  lv2 character varying
);


CREATE TABLE prerds.finance_hie_acc_grp1
(
  lv1 character varying,
  lv2 character varying
);



CREATE TABLE prerds.finance_hie_acc_grp_final
(
  lv1 character varying,
  lv2 character varying,
  description character varying
);

insert into prerds.finance_hie_acc_grp1 values ('Test','Test');
insert into prerds.finance_hie_acc_grp1 values ('Test','Test2');
insert into prerds.finance_hie_acc_grp1 values ('Test','Test2');
insert into prerds.finance_hie_acc_grp1 values ('Test','Test3');
insert into prerds.finance_hie_acc_grp1 values ('Test','Test4');
insert into prerds.finance_hie_acc_grp values ('Test','Test2');
insert into prerds.finance_hie_acc_grp values ('Test','Test2');
insert into prerds.finance_hie_acc_grp values ('Test','Test2');
insert into prerds.finance_hie_acc_grp values ('Test','Test2');
insert into prerds.finance_hie_acc_grp values ('Test','Test2');

INSERT INTO prerds.FINANCE_HIE_ACC_GRP_TEMP select a.lv1,b.lv2 from prerds.FINANCE_HIE_ACC_GRP1 a, prerds.FINANCE_HIE_ACC_GRP b where a.lv2 = b.lv1;

drop schema prerds cascade;
drop function  fin_hie_acc_grp1();
CREATE TABLE aggtest_table (a int, b float);
INSERT INTO aggtest_table (a, b) values (777, 777.777);
BEGIN;
SELECT * INTO TABLE xacttest_table FROM aggtest_table;
INSERT INTO xacttest_table (a, b) VALUES (777, 777.777);
END;
SELECT a FROM xacttest_table WHERE a > 100;
BEGIN;
CREATE TABLE disappear (a int4);
TRUNCATE aggtest_table;
SELECT * FROM aggtest_table;
ABORT;
SELECT oid FROM pg_class WHERE relname = 'disappear';
SELECT * FROM aggtest_table;
DROP TABLE aggtest_table;
DROP TABLE xacttest_table;
DROP TABLE disappear;
CREATE TABLE writetest (a int) distributed randomly;
INSERT INTO writetest values (2);
CREATE TEMPORARY TABLE temptest (a int) distributed randomly;
INSERT INTO temptest values (12);
SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;
DROP TABLE writetest; 
INSERT INTO writetest VALUES (1); 
SELECT * FROM writetest; 
TRUNCATE temptest; 
UPDATE temptest SET a = 0 FROM writetest WHERE temptest.a = 1 AND writetest.a = temptest.a; 
PREPARE test AS UPDATE writetest SET a = 0; 
EXECUTE test; 
SELECT * FROM writetest, temptest; 
CREATE TABLE test AS SELECT * FROM writetest; 
START TRANSACTION READ WRITE;
DROP TABLE writetest; 
COMMIT;
DROP TABLE temptest;
SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;
CREATE TABLE foobar_table (a int) distributed randomly;
BEGIN;
CREATE TABLE foo_table (a int);
SAVEPOINT one;
DROP TABLE foo_table;
CREATE TABLE bar_table (a int);
ROLLBACK TO SAVEPOINT one;
RELEASE SAVEPOINT one;
SAVEPOINT two;
CREATE TABLE baz_table (a int);
RELEASE SAVEPOINT two;
drop TABLE foobar_table;
CREATE TABLE barbaz_table (a int);
COMMIT;
SELECT * FROM foo_table;		
SELECT * FROM bar_table;		
SELECT * FROM barbaz_table;	
SELECT * FROM baz_table;		

BEGIN;
INSERT INTO foo_table VALUES (1);
SAVEPOINT one;
INSERT into bar_table VALUES (1);
ROLLBACK TO one;
RELEASE SAVEPOINT one;
SAVEPOINT two;
INSERT into barbaz_table VALUES (1);
RELEASE two;
SAVEPOINT three;
SAVEPOINT four;
INSERT INTO foo_table VALUES (2);
RELEASE SAVEPOINT four;
ROLLBACK TO SAVEPOINT three;
RELEASE SAVEPOINT three;
INSERT INTO foo_table VALUES (3);
COMMIT;
SELECT * FROM foo_table;		
SELECT * FROM barbaz_table;	
DROP TABLE foo_table;
DROP TABLE barbaz_table;
DROP TABLE bar_table;
DROP TABLE baz_table;
CREATE TABLE savepoints (a int) distributed randomly;
BEGIN;
INSERT INTO savepoints VALUES (4);
SAVEPOINT one;
INSERT INTO savepoints VALUES (5);
COMMIT;
SELECT * FROM savepoints;

BEGIN;
INSERT INTO savepoints VALUES (6);
SAVEPOINT one;
INSERT INTO savepoints VALUES (7);
RELEASE SAVEPOINT one;
INSERT INTO savepoints VALUES (8);
COMMIT;
BEGIN;
INSERT INTO savepoints VALUES (9);
SAVEPOINT one;
INSERT INTO savepoints VALUES (10);
ROLLBACK TO SAVEPOINT one;
INSERT INTO savepoints VALUES (11);
COMMIT;
SELECT a FROM savepoints WHERE a in (9, 10, 11);
DROP TABLE savepoints;
BEGIN;
SAVEPOINT one;
SELECT 0/0;
SAVEPOINT two;
RELEASE SAVEPOINT one;
ROLLBACK TO SAVEPOINT one;
SELECT 1;
COMMIT;
SELECT 1;
BEGIN;
CREATE TABLE tenk1 (a int) distributed randomly;
INSERT INTO tenk1 (a) values (1), (2);
DECLARE c CURSOR FOR SELECT * FROM tenk1;
SAVEPOINT one;
FETCH 10 FROM c;
ROLLBACK TO SAVEPOINT one;
FETCH 10 FROM c;
RELEASE SAVEPOINT one;
FETCH 10 FROM c;
CLOSE c;
DECLARE c CURSOR FOR SELECT a/0 FROM tenk1;
SAVEPOINT two;
FETCH 10 FROM c;
ROLLBACK TO SAVEPOINT two;
FETCH 10 FROM c;
ROLLBACK TO SAVEPOINT two;
RELEASE SAVEPOINT two;
FETCH 10 FROM c;
COMMIT;
DROP TABLE tenk1;
CREATE TABLE xacttest_table (a int, b float);
INSERT INTO xacttest_table (a, b) VALUES (777, 777.777);
select * from xacttest_table;
create or replace function max_xacttest() returns double precision language sql READS SQL DATA as
'select max(b) from xacttest_table' stable;

begin;
update xacttest_table set b = max_xacttest() + 10 where b > 0;
select * from xacttest_table;
rollback;

create or replace function max_xacttest() returns double precision language sql READS SQL DATA as
'select max(b) from xacttest_table' volatile;

begin;
update xacttest_table set b = max_xacttest() + 10 where b > 0;
select * from xacttest_table;
rollback;


create or replace function max_xacttest() returns double precision language plpgsql READS SQL DATA as
'begin return max(b) from xacttest_table; end' stable;

begin;
update xacttest_table set b = max_xacttest() + 10 where b > 0;
select * from xacttest_table;
rollback;

create or replace function max_xacttest() returns double precision language plpgsql READS SQL DATA as
'begin return max(b) from xacttest_table; end' volatile;

begin;
update xacttest_table set b = max_xacttest() + 10 where b > 0;
select * from xacttest_table;
rollback;
DROP Function max_xacttest();
DROP TABLE xacttest_table;

BEGIN;
savepoint x;
CREATE TABLE koju (a INT UNIQUE);
INSERT INTO koju VALUES (1);
INSERT INTO koju VALUES (1);
rollback to x;

CREATE TABLE koju (a INT UNIQUE);
INSERT INTO koju VALUES (1);
INSERT INTO koju VALUES (1);
ROLLBACK;

DROP TABLE foo_table;
DROP TABLE baz_table;
DROP TABLE barbaz_table;

