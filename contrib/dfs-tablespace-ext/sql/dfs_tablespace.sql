-- cleaning
DROP TABLE IF EXISTS tab_test_local;
DROP TABLE IF EXISTS tab_test_oss;
DROP TABLESPACE IF EXISTS oss_test;
DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER test_server;
DROP SERVER IF EXISTS test_server;
DROP FOREIGN DATA WRAPPER IF EXISTS test_fdw;
DROP EXTENSION IF EXISTS dfs_tablespace;

-- create objects
CREATE EXTENSION dfs_tablespace;
CREATE FOREIGN DATA WRAPPER test_fdw;
CREATE SERVER test_server FOREIGN DATA WRAPPER test_fdw OPTIONS(protocol 'qingstor', endpoint 'pek3b.qingstor.com', https 'true', virtual_host 'false');
CREATE USER MAPPING FOR CURRENT_USER SERVER test_server OPTIONS (accesskey 'KGCPPHVCHRMZMFEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');
CREATE TABLESPACE oss_test location '/tbs-49560-0-mgq-multi/tablespace-a' with(server='test_server');

-- show objects
SELECT fdwname, fdwowner, fdwhandler, fdwvalidator, fdwacl, fdwoptions
FROM pg_foreign_data_wrapper
WHERE fdwname = 'test_fdw';
SELECT srvname, srvowner, srvtype, srvversion, srvacl, srvoptions
FROM pg_foreign_server
WHERE srvname = 'test_server';
SELECT spcname, spcowner, spcacl, spcoptions
FROM pg_tablespace
WHERE spcname = 'oss_test';

-- test oss api of ufs
CREATE TABLE tab_test_oss (a int, b int) USING ufsdemo_am TABLESPACE oss_test;
INSERT INTO tab_test_oss VALUES (1, 1);
INSERT INTO tab_test_oss VALUES (1, 1);
SELECT * FROM tab_test_oss;

-- should fail
DROP TABLESPACE oss_test;
DROP TABLE tab_test_oss;

-- test local fs api of ufs
CREATE TABLE tab_test_local (a int, b int) USING ufsdemo_am;
INSERT INTO tab_test_local VALUES (1, 1);
INSERT INTO tab_test_local VALUES (1, 1);
SELECT * FROM tab_test_local;
DROP TABLE tab_test_local;

create type a as (a int);

DROP TABLESPACE oss_test;
DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER test_server;
DROP SERVER test_server;
DROP EXTENSION dfs_tablespace;
DROP FOREIGN DATA WRAPPER test_fdw;
