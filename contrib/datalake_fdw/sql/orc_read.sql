-- create datalake_fdw
-- clean table
DROP EXTERNAL TABLE IF EXISTS oss_read;
DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER foreign_server;
DROP SERVER IF EXISTS foreign_server;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw;

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE FOREIGN DATA WRAPPER datalake_fdw
  HANDLER datalake_fdw_handler
  VALIDATOR datalake_fdw_validator 
  OPTIONS ( mpp_execute 'all segments' );

CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (host 'pek3b.qingstor.com', protocol 'qs', isvirtual 'false',
        ishttps 'false');

CREATE USER MAPPING FOR gpadmin
        SERVER foreign_server
        OPTIONS (user 'gpadmin', accesskey 'KGCPPHVCHRMZMFEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');

SELECT pg_sleep(5);

set vector.enable_vectorization=off;

-- small file
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/oneSmallFile/', enableCache 'false', format 'orc');
select * from oss_read order by a,b;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/twoSmallFile/', enableCache 'false', format 'orc');
select * from oss_read order by a,b;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/moreSmallFile/', enableCache 'false', format 'orc');
select count(*) from oss_read;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/1024_smallFile/', enableCache 'false', format 'orc');
select count(*) from oss_read;

-- big file
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/oneBigFile/', enableCache 'false', format 'orc');
select count(*) from oss_read;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/twoBigFile/', enableCache 'false', format 'orc');
select count(*) from oss_read;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/more_bigFile/', enableCache 'false', format 'orc');
select count(*) from oss_read;

-- 64k stripes
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/strips_64k/', enableCache 'false', format 'orc');
select count(*) from oss_read;

-- empty
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/emptyfloder/', enableCache 'false', format 'orc');
select count(*) from oss_read;

-- complex
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/alltest/', enableCache 'false', format 'orc');
select count(*) from oss_read;

-- numeric
drop table if exists test;
create table test(
col numeric(2,1),
col2 numeric(9,6),
col3 numeric(18,3),
col4 numeric(38,3),
col5 numeric(18,17),
col6 numeric(38,37)
);
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
col numeric(2,1),
col2 numeric(9,6),
col3 numeric(18,3),
col4 numeric(38,3),
col5 numeric(18,17),
col6 numeric(38,37)
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/numeric/', enableCache 'false', format 'orc');
select * from oss_read order by col, col2, col3, col4, col5;
insert into test select * from oss_read;
select * from test order by col, col2, col3, col4, col5;

-- null col
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b text, c int, d text)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/colNull/', enableCache 'false', format 'orc');
select * from oss_read order by a,b,c,d;

-- date
SET datestyle = ISO, MDY;
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a date)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/date/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- timestamp
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a timestamp)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/timestamp/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- big text
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a text)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/bigText/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- bool varchar char
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b bool, c varchar(20), d char(20), e text, f bytea)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/bool/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- int2
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int2)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/int2/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- int2 invalid
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int2)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/int4/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- int4
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int4)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/int4/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- int4 invalid
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int4)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/int8/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- int8
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int8)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/int8/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- float4
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a float4)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/float4/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- float8
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a float8)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/float8/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- timestamp
SET datestyle = 'ISO, DMY';
DROP EXTERNAL TABLE IF EXISTS oss_read;
drop table if exists test;
create table test(a timestamp);
CREATE FOREIGN TABLE oss_read (a timestamp)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/timestamp/', enableCache 'false', format 'orc');
select * from oss_read order by a;
insert into test select a + (5 || 'day')::interval FROM oss_read;
select * from test order by a;

-- time
SET datestyle = 'ISO, DMY';
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a time)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/time/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- timestamp2
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a timestamp)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/timestamp2045/', enableCache 'false', format 'orc');
select * from oss_read order by a;

-- date2
SET datestyle = ISO, MDY;
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a date)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_orc/date2045/', enableCache 'false', format 'orc');
select * from oss_read order by a;
