-- @author prabhd
-- @created 2012-12-05 12:00:00
-- @modified 2012-12-05 12:00:00
-- @tags dml
-- @db_name dmldb
-- @description Mpp-19671
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

SET test_print_direct_dispatch_info = on;
SET gp_autostats_mode=none;

--start_ignore
DROP TABLE IF EXISTS T;
--end_ignore
CREATE TABLE T ( a int , b int ) DISTRIBUTED BY (a);
INSERT INTO T VALUES (1,2);
INSERT INTO T VALUES (1,2);
