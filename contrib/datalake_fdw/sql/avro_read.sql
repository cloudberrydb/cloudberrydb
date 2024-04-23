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

-- uncompress test
-- base test
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/oneSmallFile/', enableCache 'false', format 'avro');
select * from oss_read order by a,b;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/twoSmallFile/', enableCache 'false', format 'avro');
select * from oss_read order by a,b;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/1024SmallFile/', enableCache 'false', format 'avro');
select count(*) from oss_read;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/morebigfile/', enableCache 'false', format 'avro');
select count(*) from oss_read;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/empty_floder/', enableCache 'false', format 'avro');
select * from oss_read order by a,b;

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
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/numeric/', enableCache 'false', format 'avro');
select * from oss_read order by col, col2, col3, col4, col5;
insert into test select * from oss_read;
select * from test order by col, col2, col3, col4, col5;

-- null col
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a int, b text, c int, d text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/nullcol/', enableCache 'false', format 'avro');
select * from oss_read order by a, b, c, d;

-- time
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a time
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/time/', enableCache 'false', format 'avro');
select * from oss_read order by a;

-- date
SET datestyle = ISO, MDY;
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a date
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/date/', enableCache 'false', format 'avro');
select * from oss_read order by a;

-- timestamp
SET datestyle = ISO, MDY;
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a timestamp
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/timestamp/', enableCache 'false', format 'avro');
select * from oss_read order by a;

-- big text
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/bigtext/', enableCache 'false', format 'avro');
select count(*) from oss_read;

-- int2 int4 int8 float4 float8
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a int2,
b int4,
c int8,
d float4,
e float8,
f bool,
g varchar(20),
h char(20),
i text,
j bytea
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_uncompress/basetype/', enableCache 'false', format 'avro');
select * from oss_read order by a,b,c,d,e,f,g,h,i,j;

-- snappy test
-- base test
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/oneSmallFile/', enableCache 'false', format 'avro');
select * from oss_read;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/twoSmallFile/', enableCache 'false', format 'avro');
select * from oss_read order by a,b;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/1024SmallFile/', enableCache 'false', format 'avro');
select count(*) from oss_read;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/morebigfile/', enableCache 'false', format 'avro');
select count(*) from oss_read;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (a int, b int)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/empty_floder/', enableCache 'false', format 'avro');
select * from oss_read order by a,b;

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
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/numeric/', enableCache 'false', format 'avro');
select * from oss_read order by col, col2, col3, col4, col5;
insert into test select * from oss_read;
select * from test order by col, col2, col3, col4, col5;

-- null col
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a int, b text, c int, d text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/nullcol/', enableCache 'false', format 'avro');
select * from oss_read order by a, b, c, d;

-- time
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a time
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/time/', enableCache 'false', format 'avro');
select * from oss_read order by a;

-- date
SET datestyle = ISO, MDY;
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a date
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/date/', enableCache 'false', format 'avro');
select * from oss_read order by a;

-- timestamp
SET datestyle = ISO, MDY;
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a timestamp
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/timestamp/', enableCache 'false', format 'avro');
select * from oss_read order by a;

-- big text
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/bigtext/', enableCache 'false', format 'avro');
select count(*) from oss_read;

-- int2 int4 int8 float4 float8
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
a int2,
b int4,
c int8,
d float4,
e float8,
f bool,
g varchar(20),
h char(20),
i text,
j bytea
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/basetype/', enableCache 'false', format 'avro');
select * from oss_read order by a,b,c,d,e,f,g,h,i,j;

-- avro files generated by spark
DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
    a int2,
    b int4,
    c int8,
    d float4,
    e float8,
    f numeric(7,2),
    g numeric(16,6),
    h numeric(25,10),
    i bool,
    j text,
    k bytea,
    l date,
    m timestamp
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/sparkFile/', enableCache 'false', format 'avro');
select * from oss_read order by a;

DROP EXTERNAL TABLE IF EXISTS oss_read;
CREATE FOREIGN TABLE oss_read (
    a int2,
    b int4,
    c int8,
    d float4,
    e float8,
    f numeric(7,2),
    g numeric(16,6),
    h numeric(25,10),
    i bool,
    j text,
    k bytea,
    l date,
    m timestamp
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/avro/avro_snappy/sparkNoNull/', enableCache 'false', format 'avro');
select * from oss_read order by a;



