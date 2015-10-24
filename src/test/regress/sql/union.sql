--
-- UNION (also INTERSECT, EXCEPT)
--

-- Simple UNION constructs

SELECT 1 AS two UNION SELECT 2;

SELECT 1 AS one UNION SELECT 1;

SELECT 1 AS two UNION ALL SELECT 2;

SELECT 1 AS two UNION ALL SELECT 1;

SELECT 1 AS three UNION SELECT 2 UNION SELECT 3;

SELECT 1 AS two UNION SELECT 2 UNION SELECT 2;

SELECT 1 AS three UNION SELECT 2 UNION ALL SELECT 2;

SELECT 1.1 AS two UNION SELECT 2.2;

-- Mixed types

SELECT 1.1 AS two UNION SELECT 2;

SELECT 1 AS two UNION SELECT 2.2;

SELECT 1 AS one UNION SELECT 1.0::float8;

SELECT 1.1 AS two UNION ALL SELECT 2;

SELECT 1.0::float8 AS two UNION ALL SELECT 1;

SELECT 1.1 AS three UNION SELECT 2 UNION SELECT 3;

SELECT 1.1::float8 AS two UNION SELECT 2 UNION SELECT 2.0::float8;

SELECT 1.1 AS three UNION SELECT 2 UNION ALL SELECT 2;

SELECT 1.1 AS two UNION (SELECT 2 UNION ALL SELECT 2);

--
-- Try testing from tables...
--

SELECT f1 AS five FROM FLOAT8_TBL
UNION
SELECT f1 FROM FLOAT8_TBL ORDER BY 1;

SELECT f1 AS ten FROM FLOAT8_TBL
UNION ALL
SELECT f1 FROM FLOAT8_TBL ORDER BY 1;

SELECT f1 AS nine FROM FLOAT8_TBL
UNION
SELECT f1 FROM INT4_TBL ORDER BY 1;

SELECT f1 AS ten FROM FLOAT8_TBL
UNION ALL
SELECT f1 FROM INT4_TBL ORDER BY 1;

SELECT f1 AS five FROM FLOAT8_TBL
  WHERE f1 BETWEEN -1e6 AND 1e6
UNION
SELECT f1 FROM INT4_TBL
  WHERE f1 BETWEEN 0 AND 1000000 ORDER BY 1;

SELECT CAST(f1 AS char(4)) AS three FROM VARCHAR_TBL
UNION
SELECT f1 FROM CHAR_TBL ORDER BY 1;

SELECT f1 AS three FROM VARCHAR_TBL
UNION
SELECT CAST(f1 AS varchar) FROM CHAR_TBL ORDER BY 1;

SELECT f1 AS eight FROM VARCHAR_TBL
UNION ALL
SELECT f1 FROM CHAR_TBL ORDER BY 1;

SELECT f1 AS five FROM TEXT_TBL
UNION
SELECT f1 FROM VARCHAR_TBL
UNION
SELECT TRIM(TRAILING FROM f1) FROM CHAR_TBL ORDER BY 1;

--
-- INTERSECT and EXCEPT
--

SELECT q2 FROM int8_tbl INTERSECT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl INTERSECT ALL SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT ALL SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q2 FROM int8_tbl EXCEPT ALL SELECT DISTINCT q1 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT ALL SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl EXCEPT ALL SELECT DISTINCT q2 FROM int8_tbl ORDER BY 1;

--
-- Mixed types
--

SELECT f1 FROM float8_tbl INTERSECT SELECT f1 FROM int4_tbl ORDER BY 1;

SELECT f1 FROM float8_tbl EXCEPT SELECT f1 FROM int4_tbl ORDER BY 1;

--
-- Operator precedence and (((((extra))))) parentheses
--

SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl INTERSECT (((SELECT q2 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl))) ORDER BY 1;

(((SELECT q1 FROM int8_tbl INTERSECT SELECT q2 FROM int8_tbl))) UNION ALL SELECT q2 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

SELECT q1 FROM int8_tbl UNION ALL (((SELECT q2 FROM int8_tbl EXCEPT SELECT q1 FROM int8_tbl))) ORDER BY 1;

(((SELECT q1 FROM int8_tbl UNION ALL SELECT q2 FROM int8_tbl))) EXCEPT SELECT q1 FROM int8_tbl ORDER BY 1;

--
-- Subqueries with ORDER BY & LIMIT clauses
--

-- In this syntax, ORDER BY/LIMIT apply to the result of the EXCEPT
SELECT q1,q2 FROM int8_tbl EXCEPT SELECT q2,q1 FROM int8_tbl
ORDER BY q2,q1;

-- This should fail, because q2 isn't a name of an EXCEPT output column
SELECT q1 FROM int8_tbl EXCEPT SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1;

-- But this should work:
SELECT q1 FROM int8_tbl EXCEPT (((SELECT q2 FROM int8_tbl ORDER BY q2 LIMIT 1))) ORDER BY 1;

--
-- New syntaxes (7.1) permit new tests
--

(((((select * from int8_tbl))))) ORDER BY 1,2;

create temp table t_union1 (a int, b int);
select distinct a, null as c from t_union1 union select a, b from t_union1;
drop table t_union1;

select null union select distinct null;

select 1 union select distinct null;

select 1 a, NULL b, NULL c UNION SELECT 2, 3, NULL UNION SELECT 3, NULL, 4;

select ARRAY[1, 2, 3] union select distinct null;

select 1 intersect (select 1, 2 union all select 3, 4);
select 1 a, row_number() over (partition by 'a') union all (select 1 a , 2 b);

-- This can preserve domain types, but we keep compatibility for now
-- See MPP-7509
select pg_typeof(a) from (select 'a'::information_schema.sql_identifier a union all
select 'b'::information_schema.sql_identifier)a;

(select * from (
     (select '1' as a union select null)
     union
     (select 1 union select distinct null)
   )s) 
  union
  (select * from (
     (select '1' union select null)
     union
     (select 1 union select distinct null)
  )s2);

-- Yet, we keep behaviors on text-like columns
select pg_typeof(a) from(select 'foo' a union select 'foo'::name)s;
select pg_typeof(a) from(select 1 x, 'foo' a union
    select 1, 'foo' union select 1, 'foo'::name)s;
select pg_typeof(a) from(select 1 x, 'foo' a union
    (select 1, 'foo' union select 1, 'foo'::name))s;

CREATE TABLE union_ctas (a, b) AS SELECT 1, 2 UNION SELECT 1, 1 UNION SELECT 1, 1;
SELECT * FROM union_ctas;
DROP TABLE union_ctas;

-- MPP-21075: push quals below union
CREATE TABLE union_quals1 (a, b) AS SELECT i, i%2 from generate_series(1,10) i;
CREATE TABLE union_quals2 (a, b) AS SELECT i%2, i from generate_series(1,10) i;
SELECT * FROM (SELECT a, b from union_quals1 UNION SELECT b, a from union_quals2) as foo(a,b) where a > b order by a;
SELECT * FROM (SELECT a, max(b) over() from union_quals1 UNION SELECT * from union_quals2) as foo(a,b) where b > 6 order by a,b;

-- MPP-22266: different combinations of set operations and distinct
select * from ((select 1, 'A' from (select distinct 'B') as foo) union (select 1, 'C')) as bar;
select 1 union (select distinct null union select '10');
select 1 union (select 2 from (select distinct null union select 1) as x);
select 1 union (select distinct '10' from (select 1, 3.0 union select distinct 2, null) as foo);
select distinct a from (select 'A' union select 'B') as foo(a);
select distinct a from (select distinct 'A' union select 'B') as foo(a);
select distinct a from (select distinct 'A' union select distinct 'B') as foo(a);
select distinct a from (select  'A' from (select distinct 'C' ) as bar union select distinct 'B') as foo(a);
select distinct a from (select  distinct 'A' from (select distinct 'C' ) as bar union select distinct 'B') as foo(a);
select distinct a from (select  distinct 'A' from (select 'C' from (select distinct 'D') as bar1 ) as bar union select distinct 'B') as foo(a);
