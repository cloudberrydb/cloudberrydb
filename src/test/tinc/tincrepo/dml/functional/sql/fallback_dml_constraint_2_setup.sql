DROP TABLE IF EXISTS constr_tab;
CREATE TABLE constr_tab ( a int check (a>0) , b int, c int, d int, CHECK (a+b>5)) DISTRIBUTED BY (a);
