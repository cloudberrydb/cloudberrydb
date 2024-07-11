-- create datalake_fdw
-- clean table
DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER foreign_server;
DROP SERVER IF EXISTS foreign_server cascade;
DROP FOREIGN DATA WRAPPER IF EXISTS datalake_fdw cascade;

CREATE EXTENSION IF NOT EXISTS datalake_fdw;

CREATE FOREIGN DATA WRAPPER datalake_fdw
  HANDLER datalake_fdw_handler
  VALIDATOR datalake_fdw_validator 
  OPTIONS ( mpp_execute 'all segments' );

CREATE SERVER foreign_server
        FOREIGN DATA WRAPPER datalake_fdw
        OPTIONS (host 'obs.cn-north-4.myhuaweicloud.com', protocol 'huawei', isvirtual 'false',
        ishttps 'false');

CREATE USER MAPPING FOR gpadmin
        SERVER foreign_server
        OPTIONS (user 'gpadmin', accesskey 'J04WCCF5VQP6BAIQUFHP', secretkey 'jGDwttCct2b9b4rEf0hsLD7CeP9WubZuqqz90iQU');

SELECT pg_sleep(5);

set vector.enable_vectorization=off;

set datalake.external_table_limit_segment_num = 0;
set datalake.external_table_new_text = true;
DROP FOREIGN TABLE IF EXISTS read_one_file;
CREATE FOREIGN TABLE read_one_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile/', enableCache 'false', format 'text');
select * from read_one_file order by a;
DROP FOREIGN TABLE IF EXISTS read_one_file;

DROP FOREIGN TABLE IF EXISTS read_one_file2;
CREATE FOREIGN TABLE read_one_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
select * from read_one_file2 order by a;
DROP FOREIGN TABLE IF EXISTS read_one_file2;

DROP FOREIGN TABLE IF EXISTS read_two_file;
CREATE FOREIGN TABLE read_two_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/twofile/', enableCache 'false', format 'text');
SELECT * FROM read_two_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_two_file;

DROP FOREIGN TABLE IF EXISTS read_more_file;
CREATE FOREIGN TABLE read_more_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file/', enableCache 'false', format 'text');
SELECT * FROM read_more_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file;

DROP FOREIGN TABLE IF EXISTS read_more_file2;
CREATE FOREIGN TABLE read_more_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file2/', enableCache 'false', format 'text');
SELECT * FROM read_more_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file2;

DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;
CREATE FOREIGN TABLE read_invalid_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/invalid_path/', enableCache 'false', format 'text');
SELECT * FROM read_invalid_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;

DROP FOREIGN TABLE IF EXISTS read_empty_file_path;
CREATE FOREIGN TABLE read_empty_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/empty_path/', enableCache 'false', format 'text');
SELECT * FROM read_empty_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_empty_file_path;

SET datalake.external_table_limit_segment_num = 1;
set datalake.external_table_new_text = true;
DROP FOREIGN TABLE IF EXISTS read_one_file;
CREATE FOREIGN TABLE read_one_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile/', enableCache 'false', format 'text');
SELECT * FROM read_one_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file;

DROP FOREIGN TABLE IF EXISTS read_one_file2;
CREATE FOREIGN TABLE read_one_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
SELECT * FROM read_one_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file2;

DROP FOREIGN TABLE IF EXISTS read_one_file3;
CREATE FOREIGN TABLE read_one_file3 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
SELECT * FROM read_one_file3 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file3;

DROP FOREIGN TABLE IF EXISTS read_two_file;
CREATE FOREIGN TABLE read_two_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/twofile/', enableCache 'false', format 'text');
SELECT * FROM read_two_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_two_file;

DROP FOREIGN TABLE IF EXISTS read_more_file;
CREATE FOREIGN TABLE read_more_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file/', enableCache 'false', format 'text');
SELECT * FROM read_more_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file;

DROP FOREIGN TABLE IF EXISTS read_more_file2;
CREATE FOREIGN TABLE read_more_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file2/', enableCache 'false', format 'text');
SELECT * FROM read_more_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file2;

DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;
CREATE FOREIGN TABLE read_invalid_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/invalid_path/', enableCache 'false', format 'text');
SELECT * FROM read_invalid_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;

DROP FOREIGN TABLE IF EXISTS read_empty_file_path;
CREATE FOREIGN TABLE read_empty_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/empty_path/', enableCache 'false', format 'text');
SELECT * FROM read_empty_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_empty_file_path;

SET datalake.external_table_limit_segment_num = 2;
set datalake.external_table_new_text = true;
DROP FOREIGN TABLE IF EXISTS read_one_file;
CREATE FOREIGN TABLE read_one_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile/', enableCache 'false', format 'text');
SELECT * FROM read_one_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file;

DROP FOREIGN TABLE IF EXISTS read_one_file2;
CREATE FOREIGN TABLE read_one_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
SELECT * FROM read_one_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file2;

DROP FOREIGN TABLE IF EXISTS read_one_file3;
CREATE FOREIGN TABLE read_one_file3 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
SELECT * FROM read_one_file3 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file3;

DROP FOREIGN TABLE IF EXISTS read_two_file;
CREATE FOREIGN TABLE read_two_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/twofile/', enableCache 'false', format 'text');
SELECT * FROM read_two_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_two_file;

DROP FOREIGN TABLE IF EXISTS read_more_file;
CREATE FOREIGN TABLE read_more_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file/', enableCache 'false', format 'text');
SELECT * FROM read_more_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file;

DROP FOREIGN TABLE IF EXISTS read_more_file2;
CREATE FOREIGN TABLE read_more_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file2/', enableCache 'false', format 'text');
SELECT * FROM read_more_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file2;

DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;
CREATE FOREIGN TABLE read_invalid_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/invalid_path/', enableCache 'false', format 'text');
SELECT * FROM read_invalid_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;

DROP FOREIGN TABLE IF EXISTS read_empty_file_path;
CREATE FOREIGN TABLE read_empty_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/empty_path/', enableCache 'false', format 'text');
SELECT * FROM read_empty_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_empty_file_path;

SET datalake.external_table_limit_segment_num = 3;
set datalake.external_table_new_text = true;
DROP FOREIGN TABLE IF EXISTS read_one_file;
CREATE FOREIGN TABLE read_one_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile/', enableCache 'false', format 'text');
SELECT * FROM read_one_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file;

DROP FOREIGN TABLE IF EXISTS read_one_file2;
CREATE FOREIGN TABLE read_one_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
SELECT * FROM read_one_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file2;

DROP FOREIGN TABLE IF EXISTS read_one_file3;
CREATE FOREIGN TABLE read_one_file3 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
SELECT * FROM read_one_file3 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file3;

DROP FOREIGN TABLE IF EXISTS read_two_file;
CREATE FOREIGN TABLE read_two_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/twofile/', enableCache 'false', format 'text');
SELECT * FROM read_two_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_two_file;

DROP FOREIGN TABLE IF EXISTS read_more_file;
CREATE FOREIGN TABLE read_more_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file/', enableCache 'false', format 'text');
SELECT * FROM read_more_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file;

DROP FOREIGN TABLE IF EXISTS read_more_file2;
CREATE FOREIGN TABLE read_more_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file2/', enableCache 'false', format 'text');
SELECT * FROM read_more_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file2;

DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;
CREATE FOREIGN TABLE read_invalid_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/invalid_path/', enableCache 'false', format 'text');
SELECT * FROM read_invalid_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;

DROP FOREIGN TABLE IF EXISTS read_empty_file_path;
CREATE FOREIGN TABLE read_empty_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/empty_path/', enableCache 'false', format 'text');
SELECT * FROM read_empty_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_empty_file_path;

SET datalake.external_table_limit_segment_num = 500;
set datalake.external_table_new_text = true;
DROP FOREIGN TABLE IF EXISTS read_one_file;
CREATE FOREIGN TABLE read_one_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile/', enableCache 'false', format 'text');
SELECT * FROM read_one_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file;

DROP FOREIGN TABLE IF EXISTS read_one_file2;
CREATE FOREIGN TABLE read_one_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
SELECT * FROM read_one_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file2;

DROP FOREIGN TABLE IF EXISTS read_one_file3;
CREATE FOREIGN TABLE read_one_file3 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/onefile2/', enableCache 'false', format 'text');
SELECT * FROM read_one_file3 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_one_file3;

DROP FOREIGN TABLE IF EXISTS read_two_file;
CREATE FOREIGN TABLE read_two_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/twofile/', enableCache 'false', format 'text');
SELECT * FROM read_two_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_two_file;

DROP FOREIGN TABLE IF EXISTS read_more_file;
CREATE FOREIGN TABLE read_more_file (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file/', enableCache 'false', format 'text');
SELECT * FROM read_more_file ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file;

DROP FOREIGN TABLE IF EXISTS read_more_file2;
CREATE FOREIGN TABLE read_more_file2 (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/more_file2/', enableCache 'false', format 'text');
SELECT * FROM read_more_file2 ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_more_file2;

DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;
CREATE FOREIGN TABLE read_invalid_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/invalid_path/', enableCache 'false', format 'text');
SELECT * FROM read_invalid_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_invalid_file_path;

DROP FOREIGN TABLE IF EXISTS read_empty_file_path;
CREATE FOREIGN TABLE read_empty_file_path (
  a int,
  b text
)
SERVER foreign_server
OPTIONS (filePath '/ossext-ci-test/ext_text/empty_path/', enableCache 'false', format 'text');
SELECT * FROM read_empty_file_path ORDER BY a;
DROP FOREIGN TABLE IF EXISTS read_empty_file_path;
