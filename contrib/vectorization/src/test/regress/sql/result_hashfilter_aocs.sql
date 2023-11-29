SET default_table_access_method=ao_column;

DROP TABLE IF EXISTS rep;

DROP TABLE IF EXISTS dis;

CREATE TABLE rep (a int, b int) DISTRIBUTED REPLICATED;

CREATE TABLE dis (a int, b int);

INSERT INTO rep VALUES (1, 1), (2, 2), (3, NULL), (NULL, 4);
INSERT INTO dis VALUES (1, 1), (2, 2);

EXPLAIN SELECT rep.a, rep.b, dis.a, dis.b FROM rep LEFT JOIN dis ON rep.a = dis.a ORDER BY rep.a;

SELECT rep.a, rep.b, dis.a, dis.b FROM rep LEFT JOIN dis ON rep.a = dis.a ORDER BY rep.a;

INSERT INTO rep SELECT generate_series(1, 10000), generate_series(1, 10000);

INSERT INTO dis SELECT generate_series(1, 3000), generate_series(1, 3000);

EXPLAIN SELECT count(*) FROM rep LEFT JOIN dis ON rep.a = dis.a;

SELECT count(*) FROM rep LEFT JOIN dis ON rep.a = dis.a;

DROP TABLE rep;

DROP TABLE dis;