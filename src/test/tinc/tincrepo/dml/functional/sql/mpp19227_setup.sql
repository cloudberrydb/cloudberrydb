\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP TABLE IF EXISTS a;
CREATE TABLE a( a int) partition by range(a)(start(1) end(10) every(1),default partition a);
INSERT INTO a SELECT generate_series(1,10);

DROP TABLE IF EXISTS e;
DROP TABLE IF EXISTS f;
CREATE TABLE e( b int,a int) with (appendonly = true) partition by range(a)(start(1) end(10) every(1),default partition def);
CREATE TABLE f(b int, a int) with (appendonly = true, orientation=column) partition by range(a)(start(1) end(10) every(1),default partition def);
INSERT INTO f SELECT i,i FROM generate_series(1,10)i;
INSERT INTO e SELECT i,i FROM generate_series(1,10)i;
