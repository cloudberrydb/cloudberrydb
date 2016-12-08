\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP TABLE IF EXISTS r;
CREATE TABLE r ( a int, b int, x int, y int ) distributed randomly;

DROP TABLE IF EXISTS r_p;
CREATE TABLE r_p ( a int, b int, x int, y int ) distributed randomly
partition by list(a) ( VALUES (0), VALUES (1) );

DROP TABLE IF EXISTS s;
CREATE TABLE s ( c int, d int, e int ) distributed randomly;

INSERT INTO s VALUES
(0,0,0),(0,0,1),(0,1,0),(0,1,1),(1,0,0),(1,0,1),(1,1,0),(1,1,1);

INSERT INTO r VALUES
    (0, 0, 1, 1),
    (0, 1, 2, 2),
    (0, 1, 2, 2),
    (1, 0, 3, 3),
    (1, 0, 3, 3),
    (1, 0, 3, 3),
    (1, 1, 4, 4),
    (1, 1, 4, 4),
    (1, 1, 4, 4),
    (1, 1, 4, 4);

INSERT INTO r_p VALUES
    (0, 0, 1, 1),
    (0, 1, 2, 2),
    (0, 1, 2, 2),
    (1, 0, 3, 3),
    (1, 0, 3, 3),
    (1, 0, 3, 3),
    (1, 1, 4, 4),
    (1, 1, 4, 4),
    (1, 1, 4, 4),
    (1, 1, 4, 4);
