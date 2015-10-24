CREATE DATABASE test;
\c test

-- create a schema we can use
CREATE SCHEMA testschema;

-- try a table with index
CREATE TABLE testschema.foo (i int);
CREATE TABLE testschema.twofields (i int, c char(1));

CREATE INDEX testindex ON testschema.foo (i);

-- test a set of various DML statements
INSERT INTO testschema.foo VALUES(1);
INSERT INTO testschema.foo VALUES(2);
TRUNCATE testschema.foo;

BEGIN;
INSERT INTO testschema.foo VALUES(1);
INSERT INTO testschema.foo VALUES(2);
ROLLBACK;

BEGIN;
INSERT INTO testschema.foo VALUES(1);
INSERT INTO testschema.foo VALUES(2);
UPDATE testschema.foo SET i = 4;
COMMIT;

INSERT INTO testschema.foo VALUES(1);
INSERT INTO testschema.foo VALUES(2);

INSERT INTO testschema.twofields VALUES(1, '1');
INSERT INTO testschema.twofields VALUES(2, '2');

DELETE FROM testschema.foo;
