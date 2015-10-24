-- Test: Subqueries 1
CREATE TABLE SUBSELECT_TBL1 (
  							f1 integer,
							f2 integer,
  							f3 float
						);
		
INSERT INTO SUBSELECT_TBL1 VALUES (1, 2, 3); 
INSERT INTO SUBSELECT_TBL1 VALUES (2, 3, 4); 
INSERT INTO SUBSELECT_TBL1 VALUES (3, 4, 5); 
INSERT INTO SUBSELECT_TBL1 VALUES (1, 1, 1); 
INSERT INTO SUBSELECT_TBL1 VALUES (2, 2, 2); 
INSERT INTO SUBSELECT_TBL1 VALUES (3, 3, 3); 
INSERT INTO SUBSELECT_TBL1 VALUES (6, 7, 8); 
INSERT INTO SUBSELECT_TBL1 VALUES (8, 9, NULL); 
SELECT '' AS eight, * FROM SUBSELECT_TBL1 ORDER BY 2,3,4;
                        

-- Test: Subqueries 2
SELECT '' AS two, f1 AS "Constant Select" FROM SUBSELECT_TBL1
 					 WHERE f1 IN (SELECT 1) ORDER BY 2;
                        

-- Test: Subqueries 3
\echo -- order 2
SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL1
					  WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL1) ORDER BY 2;
                        

-- Test: Subqueries 4
\echo -- order 2
SELECT '' AS six, f1 AS "Uncorrelated Field" FROM SUBSELECT_TBL1
				 WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL1 WHERE
				   f2 IN (SELECT f1 FROM SUBSELECT_TBL1)) ORDER BY 2;
                        

-- Test: Subqueries 5
\echo -- order 2,3
SELECT '' AS three, f1, f2
  				FROM SUBSELECT_TBL1
  				WHERE (f1, f2) NOT IN (SELECT f2, CAST(f3 AS int4) FROM SUBSELECT_TBL1
                         	WHERE f3 IS NOT NULL) ORDER BY 2,3;
                        

-- Test: Subqueries 6
SELECT 1 AS one WHERE 1 IN (SELECT 1);
                        

-- Test: Subqueries 7
SELECT 1 AS zero WHERE 1 IN (SELECT 2);
			 

-- Test: Subqueries 7.1
SELECT 1 AS zero WHERE 1 NOT IN (SELECT 1);
                         

-- Test: Subqueries 8
SELECT '' AS six, f1 AS "Correlated Field", f2 AS "Second Field"
                                FROM SUBSELECT_TBL1 upper
                                WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL1 WHERE f1 = upper.f1);
                         

-- Test: Subqueries 12
create table join_tab1 ( i integer, j integer, t text);
INSERT INTO join_tab1 VALUES (1, 4, 'one');
INSERT INTO join_tab1 VALUES (2, 3, 'two');
INSERT INTO join_tab1 VALUES (3, 2, 'three');
INSERT INTO join_tab1 VALUES (4, 1, 'four');
INSERT INTO join_tab1 VALUES (5, 0, 'five');
INSERT INTO join_tab1 VALUES (6, 6, 'six');
INSERT INTO join_tab1  VALUES (7, 7, 'seven');
INSERT INTO join_tab1 VALUES (8, 8, 'eight');
INSERT INTO join_tab1 VALUES (0, NULL, 'zero');
INSERT INTO join_tab1 VALUES (NULL, NULL, 'null');
INSERT INTO join_tab1 VALUES (NULL, 0, 'zero');
select * from join_tab1 order by i, t;				
                         

-- Test: Subqueries 13
create table join_tab2 ( i integer, k integer);
INSERT INTO join_tab2 VALUES (1, -1);
INSERT INTO join_tab2 VALUES (2, 2);
INSERT INTO join_tab2 VALUES (3, -3);
INSERT INTO join_tab2 VALUES (2, 4);
INSERT INTO join_tab2 VALUES (5, -5);
INSERT INTO join_tab2 VALUES (5, -5);
INSERT INTO join_tab2 VALUES (0, NULL);
INSERT INTO join_tab2 VALUES (NULL, NULL);
INSERT INTO join_tab2 VALUES (NULL, 0);
select * from join_tab2; 
                         

-- Test: Subqueries 18
select * from(SELECT '' AS "col", t1.a, t2.e FROM join_tab1 t1 (a, b, c), join_tab2 t2 (d, e) 
				WHERE t1.a = t2.d)as A;

                         

-- Test: Subqueries 24
create table subq_abc(a int);
insert into subq_abc values(1);
insert into subq_abc values(9);
insert into subq_abc values(3);
insert into subq_abc values(6);
select * from subq_abc;
                         

-- Test: Subqueries 38
select null::int >= all ('{1,2,33}');
                         

-- Test: Subqueries 49
create table join_tab4 ( i integer, j integer, t text);
insert into join_tab4 values (1,7,'sunday'); 
insert into join_tab4 values (2,6,'monday');
insert into join_tab4 values (3,5,'tueday');
insert into join_tab4 values (4,4,'wedday');
insert into join_tab4 values (5,3,'thuday');
insert into join_tab4 values (6,2,'friday');
insert into join_tab4 values (7,1,'satday');
select * from join_tab4;
                      

-- Test: Subqueries 50
select i,j,t from (select * from (select i,j,t from join_tab1)as dtab1 
				UNION select * from(select i,j,t from join_tab4) as dtab2 )as mtab; 	
                      

DROP table IF EXISTS join_tab1;
DROP table IF EXISTS join_tab2;
DROP TABLE IF EXISTS SUBSELECT_TBL1;
DROP table IF EXISTS subq_abc;
DROP table IF EXISTS emp_list;
DROP TABLE IF EXISTS subq_test1;
DROP table IF EXISTS join_tab4;
