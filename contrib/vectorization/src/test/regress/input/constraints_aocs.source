SET default_table_access_method=ao_column;
--
-- CONSTRAINTS
-- Constraints can be specified with:
--  - DEFAULT clause
--  - CHECK clauses
--  - PRIMARY KEY clauses
--  - UNIQUE clauses
--  - EXCLUDE clauses
--

-- start_matchsubs
-- m/DETAIL:  Failing row contains \(.*\)/
-- s/DETAIL:  Failing row contains \(.*\)/DETAIL:  Failing row contains (#####)/
-- end_matchsubs

--
-- DEFAULT syntax
--

CREATE TABLE DEFAULT_TBL (i int DEFAULT 100,
	x text DEFAULT 'vadim', f float8 DEFAULT 123.456);

INSERT INTO DEFAULT_TBL VALUES (1, 'thomas', 57.0613);
INSERT INTO DEFAULT_TBL VALUES (1, 'bruce');
INSERT INTO DEFAULT_TBL (i, f) VALUES (2, 987.654);
INSERT INTO DEFAULT_TBL (x) VALUES ('marc');
INSERT INTO DEFAULT_TBL VALUES (3, null, 1.0);

SELECT * FROM DEFAULT_TBL;

CREATE SEQUENCE DEFAULT_SEQ CACHE 1;

CREATE TABLE DEFAULTEXPR_TBL (i1 int DEFAULT 100 + (200-199) * 2,
	i2 int DEFAULT nextval('default_seq'));

INSERT INTO DEFAULTEXPR_TBL VALUES (-1, -2);
INSERT INTO DEFAULTEXPR_TBL (i1) VALUES (-3);
INSERT INTO DEFAULTEXPR_TBL (i2) VALUES (-4);
INSERT INTO DEFAULTEXPR_TBL (i2) VALUES (NULL);

SELECT * FROM DEFAULTEXPR_TBL;

-- syntax errors
--  test for extraneous comma
CREATE TABLE error_tbl (i int DEFAULT (100, ));
--  this will fail because gram.y uses b_expr not a_expr for defaults,
--  to avoid a shift/reduce conflict that arises from NOT NULL being
--  part of the column definition syntax:
CREATE TABLE error_tbl (b1 bool DEFAULT 1 IN (1, 2));
--  this should work, however:
CREATE TABLE error_tbl (b1 bool DEFAULT (1 IN (1, 2)));

DROP TABLE error_tbl;

--
-- CHECK syntax
--

CREATE TABLE CHECK_TBL (x int,
	CONSTRAINT CHECK_CON CHECK (x > 3));

INSERT INTO CHECK_TBL VALUES (5);
INSERT INTO CHECK_TBL VALUES (4);
INSERT INTO CHECK_TBL VALUES (3);
INSERT INTO CHECK_TBL VALUES (2);
INSERT INTO CHECK_TBL VALUES (6);
INSERT INTO CHECK_TBL VALUES (1);

SELECT * FROM CHECK_TBL;

CREATE SEQUENCE CHECK_SEQ CACHE 1;

CREATE TABLE CHECK2_TBL (x int, y text, z int,
	CONSTRAINT SEQUENCE_CON
	CHECK (x > 3 and y <> 'check failed' and z < 8));

INSERT INTO CHECK2_TBL VALUES (4, 'check ok', -2);
INSERT INTO CHECK2_TBL VALUES (1, 'x check failed', -2);
INSERT INTO CHECK2_TBL VALUES (5, 'z check failed', 10);
INSERT INTO CHECK2_TBL VALUES (0, 'check failed', -2);
INSERT INTO CHECK2_TBL VALUES (6, 'check failed', 11);
INSERT INTO CHECK2_TBL VALUES (7, 'check ok', 7);

SELECT * from CHECK2_TBL;

--
-- Check constraints on INSERT
--

CREATE SEQUENCE INSERT_SEQ CACHE 1;

CREATE TABLE INSERT_TBL (x INT DEFAULT nextval('insert_seq'),
	y TEXT DEFAULT '-NULL-',
	z INT DEFAULT -1 * (nextval('insert_seq')-1),
	CONSTRAINT INSERT_TBL_CON CHECK (x >= 3 AND y <> 'check failed' AND x < 8),
	CHECK (x + z = 0)) DISTRIBUTED BY(y);

INSERT INTO INSERT_TBL(x,z) VALUES (2, -2);

SELECT * FROM INSERT_TBL;

SELECT 'one' AS one, nextval('insert_seq');

INSERT INTO INSERT_TBL(y) VALUES ('Y');
INSERT INTO INSERT_TBL(y) VALUES ('Y');
INSERT INTO INSERT_TBL(x,z) VALUES (1, -2);
INSERT INTO INSERT_TBL(z,x) VALUES (-7,  7);
INSERT INTO INSERT_TBL VALUES (5, 'check failed', -5);
INSERT INTO INSERT_TBL VALUES (7, '!check failed', -7);
INSERT INTO INSERT_TBL(y) VALUES ('-!NULL-');

SELECT * FROM INSERT_TBL;

-- GPDB: reset the table contents to match upstream.
delete from insert_tbl;
insert into insert_tbl values
  (3, 'Y', -3),
  (7, '-NULL-', -7),
  (7, '!check failed', -7),
  (4, '-!NULL-', -4);

INSERT INTO INSERT_TBL(y,z) VALUES ('check failed', 4);
INSERT INTO INSERT_TBL(x,y) VALUES (5, 'check failed');
-- these fail on GPDB, because we cannot use currval in the z columns' DEFAULT like in upstream.
INSERT INTO INSERT_TBL(x,y) VALUES (5, '!check failed');
INSERT INTO INSERT_TBL(y) VALUES ('-!NULL-');

-- GPDB: fix the table contents to match upstream again, so that the subsequent
-- tests start with the same state as in upstream.
insert into insert_tbl values
  (5, '!check failed', -5),
  (6, '-!NULL-', -6);

SELECT * FROM INSERT_TBL;

SELECT 'seven' AS one, nextval('insert_seq');

INSERT INTO INSERT_TBL(y) VALUES ('Y');

SELECT 'eight' AS one, nextval('insert_seq');

-- According to SQL, it is OK to insert a record that gives rise to NULL
-- constraint-condition results.  Postgres used to reject this, but it
-- was wrong:
INSERT INTO INSERT_TBL VALUES (null, null, null);

SELECT * FROM INSERT_TBL;

--
-- Check constraints on system columns
--

CREATE TABLE SYS_COL_CHECK_TBL (city text, state text, is_capital bool,
                  altitude int,
                  CHECK (NOT (is_capital AND tableoid::regclass::text = 'sys_col_check_tbl')));

INSERT INTO SYS_COL_CHECK_TBL VALUES ('Seattle', 'Washington', false, 100);
INSERT INTO SYS_COL_CHECK_TBL VALUES ('Olympia', 'Washington', true, 100);

SELECT *, tableoid::regclass::text FROM SYS_COL_CHECK_TBL;

DROP TABLE SYS_COL_CHECK_TBL;

--
-- Check constraints on system columns other then TableOid should return error
--
CREATE TABLE SYS_COL_CHECK_TBL (city text, state text, is_capital bool,
                  altitude int,
				  CHECK (NOT (is_capital AND ctid::text = 'sys_col_check_tbl')));

--
-- Check inheritance of defaults and constraints
--

CREATE TABLE INSERT_CHILD (cx INT default 42,
	cy INT CHECK (cy > x))
	INHERITS (INSERT_TBL);

INSERT INTO INSERT_CHILD(x,z,cy) VALUES (7,-7,11);
INSERT INTO INSERT_CHILD(x,z,cy) VALUES (7,-7,6);
INSERT INTO INSERT_CHILD(x,z,cy) VALUES (6,-7,7);
INSERT INTO INSERT_CHILD(x,y,z,cy) VALUES (6,'check failed',-6,7);

SELECT * FROM INSERT_CHILD;

DROP TABLE INSERT_CHILD;

--
-- Check NO INHERIT type of constraints and inheritance
--

CREATE TABLE ATACC1 (TEST INT
	CHECK (TEST > 0) NO INHERIT);

CREATE TABLE ATACC2 (TEST2 INT) INHERITS (ATACC1);
-- check constraint is not there on child
INSERT INTO ATACC2 (TEST) VALUES (-3);
-- check constraint is there on parent
INSERT INTO ATACC1 (TEST) VALUES (-3);
DROP TABLE ATACC1 CASCADE;

CREATE TABLE ATACC1 (TEST INT, TEST2 INT
	CHECK (TEST > 0), CHECK (TEST2 > 10) NO INHERIT);

CREATE TABLE ATACC2 () INHERITS (ATACC1);
-- check constraint is there on child
INSERT INTO ATACC2 (TEST) VALUES (-3);
-- check constraint is there on parent
INSERT INTO ATACC1 (TEST) VALUES (-3);
-- check constraint is not there on child
INSERT INTO ATACC2 (TEST2) VALUES (3);
-- check constraint is there on parent
INSERT INTO ATACC1 (TEST2) VALUES (3);
DROP TABLE ATACC1 CASCADE;

--
-- Check constraints on INSERT INTO
--

DELETE FROM INSERT_TBL;

ALTER SEQUENCE INSERT_SEQ RESTART WITH 4;

CREATE TEMP TABLE tmp (xd INT, yd TEXT, zd INT);

INSERT INTO tmp VALUES (null, 'Y', null);
INSERT INTO tmp VALUES (5, '!check failed', null);
INSERT INTO tmp VALUES (null, 'try again', null);
