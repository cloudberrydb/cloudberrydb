DROP TABLE IF EXISTS constr_tab;
CREATE TABLE constr_tab ( a int NOT NULL, b int, c int, d int, CHECK (a+b>5)) DISTRIBUTED BY (a);
INSERT INTO constr_tab VALUES(1,5,3,4);
