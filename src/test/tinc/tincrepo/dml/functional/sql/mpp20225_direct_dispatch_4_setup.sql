\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS r;
CREATE TABLE r ( a int, b int, c int) distributed by (a);
CREATE TABLE a (a int) distributed by (a);
INSERT INTO a VALUES(1);
