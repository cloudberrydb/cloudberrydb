-- start_matchsubs
-- m/variable\.c/
-- s/variable\.c:(\d)+/SOMEFUNC:SOMELINE/
-- end_matchsubs
-------------------------------------- Test CursorLostUpdate for: READ UNCOMMITTED--
--  P4C Cursor Lost Update:
--
--  rc1[x]...w2[x]...w1[x]...c1
------------------------------------
-- setup visi table
DROP TABLE IF EXISTS visi;



CREATE TABLE visi (a int, b int);



INSERT INTO visi VALUES (1,1);



INSERT INTO visi VALUES (2,2);



BEGIN ISOLATION LEVEL READ UNCOMMITTED;



DECLARE curs CURSOR FOR SELECT * FROM visi;



FETCH ALL FROM curs;






SELECT * FROM visi ORDER BY 1,2;


UPDATE visi SET b=4 where b=1;



COMMIT;







SELECT * FROM visi ORDER BY 1,2;


-- start_matchsubs
-- m/variable\.c/
-- s/variable\.c:(\d)+/SOMEFUNC:SOMELINE/
-- end_matchsubs
-------------------------------------- Test CursorLostUpdate for: READ COMMITTED--
--  P4C Cursor Lost Update:
--
--  rc1[x]...w2[x]...w1[x]...c1
------------------------------------
-- setup visi table
DROP TABLE IF EXISTS visi;



CREATE TABLE visi (a int, b int);



INSERT INTO visi VALUES (1,1);



INSERT INTO visi VALUES (2,2);



BEGIN ISOLATION LEVEL READ COMMITTED;



DECLARE curs CURSOR FOR SELECT * FROM visi;



FETCH ALL FROM curs;



BEGIN ISOLATION LEVEL READ COMMITTED;



SELECT * FROM visi ORDER BY 1,2;
UPDATE visi SET b=4 where b=1;

COMMIT;

SELECT * FROM visi ORDER BY 1,2;

-- start_matchsubs
-- m/variable\.c/
-- s/variable\.c:(\d)+/SOMEFUNC:SOMELINE/
-- end_matchsubs
-------------------------------------- Test CursorLostUpdate for: REPEATABLE READ--
--  P4C Cursor Lost Update:
--
--  rc1[x]...w2[x]...w1[x]...c1
------------------------------------
-- setup visi table
DROP TABLE IF EXISTS visi;

CREATE TABLE visi (a int, b int);

INSERT INTO visi VALUES (1,1);

INSERT INTO visi VALUES (2,2);

BEGIN ISOLATION LEVEL REPEATABLE READ;
DECLARE curs CURSOR FOR SELECT * FROM visi;
FETCH ALL FROM curs;
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM visi ORDER BY 1,2;
UPDATE visi SET b=4 where b=1;
COMMIT;

SELECT * FROM visi ORDER BY 1,2;


-- start_matchsubs
-- m/variable\.c/
-- s/variable\.c:(\d)+/SOMEFUNC:SOMELINE/
-- end_matchsubs
-------------------------------------- Test CursorLostUpdate for: SERIALIZABLE--
--  P4C Cursor Lost Update:
--
--  rc1[x]...w2[x]...w1[x]...c1
------------------------------------
-- setup visi table
DROP TABLE IF EXISTS visi;
CREATE TABLE visi (a int, b int);
INSERT INTO visi VALUES (1,1);
INSERT INTO visi VALUES (2,2);
BEGIN ISOLATION LEVEL SERIALIZABLE;
DECLARE curs CURSOR FOR SELECT * FROM visi;
FETCH ALL FROM curs;
SELECT * FROM visi ORDER BY 1,2;
UPDATE visi SET b=4 where b=1;
COMMIT;
SELECT * FROM visi ORDER BY 1,2;



-- start_matchsubs
-- m/variable\.c/
-- s/variable\.c:(\d)+/SOMEFUNC:SOMELINE/
-- end_matchsubs
-------------------------------------- Test FuzzyReads for: READ UNCOMMITTED--
--  P2 (Non-repeatable or Fuzzy Read): Transaction T1
--  reads a data item.  Another transaction T2 then modifies or
--  deletes that data item and commits.  If T1 then attempts to
--  reread the data item, it receives a modified value or discovers
--  that the data item has been deleted.
--
--  r1[x]...w2[x]...(c1 or a1)
------------------------------------

-- setup visi table
DROP TABLE IF EXISTS visi;
CREATE TABLE visi (a int, b int);
INSERT INTO visi VALUES (1,1);
INSERT INTO visi VALUES (2,2);
BEGIN ISOLATION LEVEL READ UNCOMMITTED;
SELECT * FROM visi ORDER BY 1,2;



DELETE FROM visi where a = 1;



COMMIT;





-- start_matchsubs
-- m/variable\.c/
-- s/variable\.c:(\d)+/SOMEFUNC:SOMELINE/
-- end_matchsubs
-------------------------------------- Test FuzzyReads for: READ COMMITTED--
--  P2 (Non-repeatable or Fuzzy Read): Transaction T1
--  reads a data item.  Another transaction T2 then modifies or
--  deletes that data item and commits.  If T1 then attempts to
--  reread the data item, it receives a modified value or discovers
--  that the data item has been deleted.
--
--  r1[x]...w2[x]...(c1 or a1)
------------------------------------

-- setup visi table
DROP TABLE IF EXISTS visi;
CREATE TABLE visi (a int, b int);
INSERT INTO visi VALUES (1,1);
INSERT INTO visi VALUES (2,2);
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT * FROM visi ORDER BY 1,2;
DELETE FROM visi where a = 1;
COMMIT;
SELECT * FROM visi ORDER BY 1,2;



-- start_matchsubs
-- m/variable\.c/
-- s/variable\.c:(\d)+/SOMEFUNC:SOMELINE/
-- end_matchsubs
-------------------------------------- Test FuzzyReads for: REPEATABLE READ--
--  P2 (Non-repeatable or Fuzzy Read): Transaction T1
--  reads a data item.  Another transaction T2 then modifies or
--  deletes that data item and commits.  If T1 then attempts to
--  reread the data item, it receives a modified value or discovers
--  that the data item has been deleted.
--
--  r1[x]...w2[x]...(c1 or a1)
------------------------------------

-- setup visi table
DROP TABLE IF EXISTS visi;
CREATE TABLE visi (a int, b int);
INSERT INTO visi VALUES (1,1);
INSERT INTO visi VALUES (2,2);
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM visi ORDER BY 1,2;
DELETE FROM visi where a = 1;
COMMIT;




-- start_matchsubs
-- m/variable\.c/
-- s/variable\.c:(\d)+/SOMEFUNC:SOMELINE/
-- end_matchsubs
-------------------------------------- Test FuzzyReads for: SERIALIZABLE--
--  P2 (Non-repeatable or Fuzzy Read): Transaction T1
--  reads a data item.  Another transaction T2 then modifies or
--  deletes that data item and commits.  If T1 then attempts to
--  reread the data item, it receives a modified value or discovers
--  that the data item has been deleted.
--
--  r1[x]...w2[x]...(c1 or a1)
------------------------------------

-- setup visi table
DROP TABLE IF EXISTS visi;
CREATE TABLE visi (a int, b int);
INSERT INTO visi VALUES (1,1);
INSERT INTO visi VALUES (2,2);
BEGIN ISOLATION LEVEL SERIALIZABLE;
DELETE FROM visi where a = 1;
COMMIT;
SELECT * FROM visi ORDER BY 1,2;
BEGIN ISOLATION LEVEL SERIALIZABLE;
UPDATE visi set b=3 where b=2;
COMMIT;

