--
-- CREATE_INDEX
-- Create ancillary data structures (i.e. indices)
--

--
-- BTREE
--
CREATE INDEX onek_unique1 ON onek USING btree(unique1 int4_ops);

CREATE INDEX onek_unique2 ON onek USING btree(unique2 int4_ops);

CREATE INDEX onek_hundred ON onek USING btree(hundred int4_ops);

CREATE INDEX onek_stringu1 ON onek USING btree(stringu1 name_ops);

CREATE INDEX tenk1_unique1 ON tenk1 USING btree(unique1 int4_ops);

CREATE INDEX tenk1_unique2 ON tenk1 USING btree(unique2 int4_ops);

CREATE INDEX tenk1_hundred ON tenk1 USING btree(hundred int4_ops);

CREATE INDEX tenk1_thous_tenthous ON tenk1 (thousand, tenthous);

CREATE INDEX tenk2_unique1 ON tenk2 USING btree(unique1 int4_ops);

CREATE INDEX tenk2_unique2 ON tenk2 USING btree(unique2 int4_ops);

CREATE INDEX tenk2_hundred ON tenk2 USING btree(hundred int4_ops);

CREATE INDEX rix ON road USING btree (name text_ops);

CREATE INDEX iix ON ihighway USING btree (name text_ops);

CREATE INDEX six ON shighway USING btree (name text_ops);

-- test comments
COMMENT ON INDEX six_wrong IS 'bad index';
COMMENT ON INDEX six IS 'good index';
COMMENT ON INDEX six IS NULL;

--
-- BTREE ascending/descending cases
--
-- we load int4/text from pure descending data (each key is a new
-- low key) and name/f8 from pure ascending data (each key is a new
-- high key).  we had a bug where new low keys would sometimes be
-- "lost".
--
CREATE INDEX bt_i4_index ON bt_i4_heap USING btree (seqno int4_ops);

CREATE INDEX bt_name_index ON bt_name_heap USING btree (seqno name_ops);

CREATE INDEX bt_txt_index ON bt_txt_heap USING btree (seqno text_ops);

CREATE INDEX bt_f8_index ON bt_f8_heap USING btree (seqno float8_ops);

--
-- BTREE partial indices
--
CREATE INDEX onek2_u1_prtl ON onek2 USING btree(unique1 int4_ops)
	where unique1 < 20 or unique1 > 980;

CREATE INDEX onek2_u2_prtl ON onek2 USING btree(unique2 int4_ops)
	where stringu1 < 'B';

CREATE INDEX onek2_stu1_prtl ON onek2 USING btree(stringu1 name_ops)
	where onek2.stringu1 >= 'J' and onek2.stringu1 < 'K';

--
-- GiST (rtree-equivalent opclasses only)
--
CREATE INDEX grect2ind ON fast_emp4000 USING gist (home_base);

CREATE INDEX gpolygonind ON polygon_tbl USING gist (f1);

CREATE INDEX gcircleind ON circle_tbl USING gist (f1);

CREATE TEMP TABLE gpolygon_tbl AS
    SELECT polygon(home_base) AS f1 FROM slow_emp4000;

CREATE TEMP TABLE gcircle_tbl AS
    SELECT circle(home_base) AS f1 FROM slow_emp4000;

CREATE INDEX ggpolygonind ON gpolygon_tbl USING gist (f1);

CREATE INDEX ggcircleind ON gcircle_tbl USING gist (f1);

SET enable_seqscan = ON;
SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;

SELECT * FROM fast_emp4000
    WHERE home_base @ '(200,200),(2000,1000)'::box
    ORDER BY (home_base[0])[0];

SELECT count(*) FROM fast_emp4000 WHERE home_base && '(1000,1000,0,0)'::box;

SELECT * FROM polygon_tbl WHERE f1 ~ '((1,1),(2,2),(2,1))'::polygon
    ORDER BY (poly_center(f1))[0];

SELECT * FROM circle_tbl WHERE f1 && circle(point(1,-2), 1)
    ORDER BY area(f1);

SELECT count(*) FROM gpolygon_tbl WHERE f1 && '(1000,1000,0,0)'::polygon;

SELECT count(*) FROM gcircle_tbl WHERE f1 && '<(500,500),500>'::circle;

SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;

-- there's no easy way to check that these commands actually use
-- the index, unfortunately.  (EXPLAIN would work, but its output
-- changes too often for me to want to put an EXPLAIN in the test...)
SELECT * FROM fast_emp4000
    WHERE home_base @ '(200,200),(2000,1000)'::box
    ORDER BY (home_base[0])[0];

SELECT count(*) FROM fast_emp4000 WHERE home_base && '(1000,1000,0,0)'::box;

SELECT * FROM polygon_tbl WHERE f1 ~ '((1,1),(2,2),(2,1))'::polygon
    ORDER BY (poly_center(f1))[0];

SELECT * FROM circle_tbl WHERE f1 && circle(point(1,-2), 1)
    ORDER BY area(f1);

SELECT count(*) FROM gpolygon_tbl WHERE f1 && '(1000,1000,0,0)'::polygon;

SELECT count(*) FROM gcircle_tbl WHERE f1 && '<(500,500),500>'::circle;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

--
-- GIN over int[]
--

SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;

CREATE INDEX intarrayidx ON array_index_op_test USING gin (i);

SELECT * FROM array_index_op_test WHERE i @> '{32}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{32}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i @> '{17}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{17}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i @> '{32,17}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i && '{32,17}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i <@ '{38,34,32,89}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE i = '{47,77}' ORDER BY seqno;

CREATE INDEX textarrayidx ON array_index_op_test USING gin (t);

SELECT * FROM array_index_op_test WHERE t @> '{AAAAAAAA72908}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t && '{AAAAAAAA72908}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t @> '{AAAAAAAAAA646}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t && '{AAAAAAAAAA646}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t @> '{AAAAAAAA72908,AAAAAAAAAA646}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t && '{AAAAAAAA72908,AAAAAAAAAA646}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t <@ '{AAAAAAAA72908,AAAAAAAAAAAAAAAAAAA17075,AA88409,AAAAAAAAAAAAAAAAAA36842,AAAAAAA48038,AAAAAAAAAAAAAA10611}' ORDER BY seqno;
SELECT * FROM array_index_op_test WHERE t = '{AAAAAAAAAA646,A87088}' ORDER BY seqno;


RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

--
-- HASH
--
set gp_hash_index = true;
CREATE INDEX hash_i4_index ON hash_i4_heap USING hash (random int4_ops);

CREATE INDEX hash_name_index ON hash_name_heap USING hash (random name_ops);

CREATE INDEX hash_txt_index ON hash_txt_heap USING hash (random text_ops);

CREATE INDEX hash_f8_index ON hash_f8_heap USING hash (random float8_ops);

set gp_hash_index = false;
-- CREATE INDEX hash_ovfl_index ON hash_ovfl_heap USING hash (x int4_ops);


--
-- Test functional index
--
CREATE TABLE func_index_heap (f1 text, f2 text);
CREATE INDEX func_index_index on func_index_heap (textcat(f1,f2));

INSERT INTO func_index_heap VALUES('ABC','DEF');
INSERT INTO func_index_heap VALUES('AB','CDEFG');
INSERT INTO func_index_heap VALUES('QWE','RTY');
-- this should fail because of unique index:
INSERT INTO func_index_heap VALUES('ABCD', 'EF');
-- but this shouldn't:
INSERT INTO func_index_heap VALUES('QWERTY');


--
-- Same test, expressional index
--
DROP TABLE func_index_heap;
CREATE TABLE func_index_heap (f1 text, f2 text);
CREATE  INDEX func_index_index on func_index_heap ((f1 || f2) text_ops);

INSERT INTO func_index_heap VALUES('ABC','DEF');
INSERT INTO func_index_heap VALUES('AB','CDEFG');
INSERT INTO func_index_heap VALUES('QWE','RTY');
-- this should fail because of unique index:
INSERT INTO func_index_heap VALUES('ABCD', 'EF');
-- but this shouldn't:
INSERT INTO func_index_heap VALUES('QWERTY');

--
-- Also try building functional, expressional, and partial indexes on
-- tables that already contain data.
--
create index hash_f8_index_1 on hash_f8_heap(abs(random));
create index hash_f8_index_2 on hash_f8_heap((seqno + 1), random);
create index hash_f8_index_3 on hash_f8_heap(random) where seqno > 1000;

--
-- Try some concurrent index builds
--
-- Unfortunately this only tests about half the code paths because there are
-- no concurrent updates happening to the table at the same time.

CREATE TABLE concur_heap (f1 text, f2 text) distributed by (f1);
-- empty table
CREATE INDEX CONCURRENTLY concur_index1 ON concur_heap(f2,f1);
-- MPP-9772, MPP-9773: re-enable CREATE INDEX CONCURRENTLY (off by default)
set gp_create_index_concurrently=true;
CREATE INDEX CONCURRENTLY concur_index1 ON concur_heap(f2,f1);
INSERT INTO concur_heap VALUES  ('a','b');
INSERT INTO concur_heap VALUES  ('b','b');
-- unique index
CREATE UNIQUE INDEX CONCURRENTLY concur_index2 ON concur_heap(f1);
-- check if constraint is set up properly to be enforced
INSERT INTO concur_heap VALUES ('b','x');
-- check if constraint is enforced properly at build time
--CREATE UNIQUE INDEX CONCURRENTLY concur_index3 ON concur_heap(f2);
-- test that expression indexes and partial indexes work concurrently
CREATE INDEX CONCURRENTLY concur_index4 on concur_heap(f2) WHERE f1='a';
CREATE INDEX CONCURRENTLY concur_index5 on concur_heap(f2) WHERE f1='x';
CREATE INDEX CONCURRENTLY concur_index6 on concur_heap((f2||f1));

-- You can't do a concurrent index build in a transaction
BEGIN;
CREATE INDEX CONCURRENTLY concur_index7 ON concur_heap(f1);
COMMIT;

-- But you can do a regular index build in a transaction
BEGIN;
CREATE INDEX std_index on concur_heap(f2);
COMMIT;

-- check to make sure that the failed indexes were cleaned up properly and the
-- successful indexes are created properly. Notably that they do NOT have the
-- "invalid" flag set.

\d concur_heap

DROP TABLE concur_heap;



SET enable_seqscan = OFF;
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;

create table bm_test (i int, t text);
insert into bm_test select i % 10, (i % 10)::text  from generate_series(1, 100) i;
create index bm_test_idx on bm_test using bitmap (i);
select count(*) from bm_test where i=1;
select count(*) from bm_test where i in(1, 2);
select * from bm_test where i > 10;
reindex index bm_test_idx;
select count(*) from bm_test where i in(1, 2);
drop index bm_test_idx;
create index bm_test_multi_idx on bm_test using bitmap(i, t);
select * from bm_test where i=5 and t='5';
select * from bm_test where i=5 or t='6';
select * from bm_test where i between 1 and 10 and i::text = t;
drop table bm_test;

-- test a bunch of different data types
create table bm_test (i2 int2, i4 int4, i8 int8, f4 float4, f8 float8,
	n numeric(10, 3), t1 varchar(3), t2 char(3), t3 text, a int[2],
	ip inet, b bytea, t timestamp, d date, g bool);

insert into bm_test values(1, 1, 1, 1.0, 1.0, 1000.333, '1', '1', '1',
    array[1, 3], '127.0.0.1', E'\001', '2007-01-01 01:01:01',
    '2007-01-01', 't');

insert into bm_test values(2, 2, 2, 2.0, 2.0, 2000.333, '2', '2', 'foo',
    array[2, 6], '127.0.0.2', E'\002', '2007-01-02 01:01:01',
    '2007-01-02', 'f');

insert into bm_test default values; -- test nulls

create index bm_i2_idx on bm_test using bitmap(i2);
create index bm_i4_idx on bm_test using bitmap(i4);
create index bm_i8_idx on bm_test using bitmap(i8);

create index bm_f4_idx on bm_test using bitmap(f4);
create index bm_f8_idx on bm_test using bitmap(f8);

create index bm_n_idx on bm_test using bitmap(n);

create index bm_t1_idx on bm_test using bitmap(t1);
create index bm_t2_idx on bm_test using bitmap(t2);
create index bm_t3_idx on bm_test using bitmap(t3);

create index bm_a_idx on bm_test using bitmap(a);

create index bm_ip_idx on bm_test using bitmap(ip);

create index bm_b_idx on bm_test using bitmap(b);

create index bm_t_idx on bm_test using bitmap(t);

create index bm_d_idx on bm_test using bitmap(d);

create index bm_g_idx on bm_test using bitmap(g);

create index bm_t3_upper_idx on bm_test using bitmap(upper(t3));
create index bm_n_null_idx on bm_test using bitmap(n) WHERE n ISNULL;
-- Try some cross type stuff
select a.t from bm_test a, bm_test b where a.i2 = b.i2;
select a.t from bm_test a, bm_test b where a.i2 = b.i4;
select a.t from bm_test a, bm_test b where a.i2 = b.i8;
select a.t from bm_test a, bm_test b where b.f4 = a.f8 and a.f8 = '2.0';

-- some range queries
select a.t from bm_test a, bm_test b where a.n < b.n;
select a.t from bm_test a, bm_test b where a.ip < b.ip;

-- or queries
select a.t from bm_test a, bm_test b where a.ip=b.ip OR a.b = b.b;

-- and
select a.t from bm_test a, bm_test b where a.ip=b.ip and a.b = b.b and a.i2=1;

-- subquery
select a.t from bm_test a where d in(select d from bm_test b where a.g=b.g);

-- functional and predicate indexes
select t from bm_test where upper(t3) = 'FOO';
select t from bm_test where n ISNULL;
-- bitmap index builds do not support concurrent building, test for this
create index concurrently should_not_work on bm_test using bitmap(a);
-- test updates
update bm_test set i4 = 3;
-- should return nothing
select * from bm_test where i4 = 1;
-- should return all
select * from bm_test where i4=3;
-- should return one row
select * from bm_test where i2=1;
-- test splitting of words
-- We distribute by k and only insert a single distinct value in that 
-- field so that we can be guaranteed of behaviour. We're not testing
-- the parallel mechanism here so it's fine to harass a single backend
create table bm_test2 (i int, j int, k int) distributed by (k);
create index bm_test2_i_idx on bm_test2 using bitmap(i);
insert into bm_test2 select 1,
case when (i % (16 * 16 + 8)) = 0 then 2  else 1 end, 1
from generate_series(1, 16 * 16 * 16) i;
select count(*) from bm_test2 where i = 1;
select count(*) from bm_test2 where j = 2;
-- break some compressed words
update bm_test2 set i = 2 where j = 2;
select count(*) from bm_test2 where i = 1;
select count(*) from bm_test2 where i = 2;
update bm_test2 set i = 3 where i = 1;
select count(*) from bm_test2 where i = 1;
select count(*) from bm_test2 where i = 2;
select count(*) from bm_test2 where i = 3;
-- now try and break a whole page
-- bitmap words are 16 bits so, with no compression we get about 
-- 16500 words per 32K page. So, what we want to do is, insert
-- 8250 uncompressed words, then a compressed word, then more uncompressed
-- words until the page is full. After this, we can break the compressed word
-- and there by test the word spliting system
create table bm_test3 (i int, j int, k int) distributed by (k);
create index bm_test3_i_idx on bm_test3 using bitmap(i);
insert into bm_test3 select i, 1, 1 from
generate_series(1, 8250 * 8) g, generate_series(1, 2) i;
insert into bm_test3 

select 17, 1, 1 from generate_series(1, 16 * 16) i;
insert into bm_test3 values(17, 2, 1);
insert into bm_test3
select 17, 1, 1 from generate_series(1, 16 * 16) i;

insert into bm_test3 select i, 1, 1 from
generate_series(1, 8250 * 8) g, generate_series(1, 2) i;
select count(*) from bm_test3 where i = 1;
select count(*) from bm_test3 where i = 17;
select count(*) from bm_test3 where i = 17 and j = 2;
update bm_test3 set i = 18 where i = 17 and j = 2;
select count(*) from bm_test3 where i = 1;
select count(*) from bm_test3 where i = 2;
select count(*) from bm_test3 where i = 17;
select count(*) from bm_test3 where i = 18;
drop table bm_test;
drop table bm_test2;
drop table bm_test3;

create table bm_test (i int, j int);
insert into bm_test values (0, 0), (0, 0), (0, 1), (1,0), (1,0), (1,1);
create index bm_test_j on bm_test using bitmap(j);
delete from bm_test where j =1;
vacuum bm_test;
insert into bm_test values (0, 0), (1,0);

set enable_seqscan=off;
set enable_bitmapscan=off;
set optimizer_enable_bitmapscan=off;
-- start_ignore
-- Known_opt_diff: MPP-19808
-- end_ignore
explain select * from bm_test where j = 1;
select * from bm_test where j = 1;
drop table bm_test;
-- MPP-3232
create table bm_test (i int,j int);
insert into bm_test values (1, 1), (1, 2);
create index bm_test_j on bm_test using bitmap(j);
update bm_test set j=20 where j=1;
vacuum bm_test;
drop table bm_test;

-- unique index with null value tests
drop table if exists ijk;
create table ijk(i int, j int, k int) distributed by (i);
insert into ijk values (1, 1, 3);
insert into ijk values (1, 2, 4);
insert into ijk values (1, 3, NULL);
insert into ijk values (1, 3, NULL);
insert into ijk values (1, NULL, NULL);
insert into ijk values (1, NULL, NULL);

-- should fail.
create unique index ijk_i on ijk(i);
create unique index ijk_ij on ijk(i,j);
-- should OK.
create unique index ijk_ijk on ijk(i,j,k);

set gp_enable_mk_sort=on;
drop table if exists ijk;
create table ijk(i int, j int, k int) distributed by (i);
insert into ijk values (1, 1, 3);
insert into ijk values (1, 2, 4);
insert into ijk values (1, 3, NULL);
insert into ijk values (1, 3, NULL);
insert into ijk values (1, NULL, NULL);
insert into ijk values (1, NULL, NULL);

-- should fail.
create unique index ijk_i on ijk(i);
create unique index ijk_ij on ijk(i,j);
-- should OK.
create unique index ijk_ijk on ijk(i,j,k);

set gp_enable_mk_sort=off;
drop table ijk;

---------
-- test bitmaps with NULL and non-NULL values (MPP-8461)
--
create table bmap_test (x int, y int, z int);
insert into bmap_test values (1,NULL,NULL);
insert into bmap_test values (NULL,1,NULL);
insert into bmap_test values (NULL,NULL,1);
insert into bmap_test values (1,NULL,NULL);
insert into bmap_test values (NULL,1,NULL);
insert into bmap_test values (NULL,NULL,1);
insert into bmap_test values (1,NULL,5);
insert into bmap_test values (NULL,1,NULL);
insert into bmap_test values (NULL,NULL,1);
insert into bmap_test select a from generate_series(1,10*1000) as s(a);
create index bmap_test_idx_1 on bmap_test using bitmap (x,y,z);
analyze bmap_test;
select * from bmap_test where x = 1 order by x,y,z;

drop table bmap_test;
