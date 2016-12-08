DROP TABLE IF EXISTS constr_tab;
CREATE TABLE constr_tab ( a int, b int, c int, d int) DISTRIBUTED BY (a);
INSERT INTO constr_tab VALUES(1,5,3,4);
INSERT INTO constr_tab VALUES(1,5,3,4);
