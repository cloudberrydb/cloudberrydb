-- @author prabhd  
-- @created 2012-12-05 12:00:00  
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore

DROP TABLE IF EXISTS dml_union_r;
DROP TABLE IF EXISTS dml_union_s;

CREATE TABLE dml_union_r (
        a int CONSTRAINT r_check_a CHECK(a <> -1),
        b int,
        c text,
        d numeric)
DISTRIBUTED BY (a);

CREATE TABLE dml_union_s (
        a int ,
        b int not NULL,
        c text ,
        d numeric default 10.00)
DISTRIBUTED BY (b)
PARTITION BY range(d) (
        start(1) end(1001) every(100),
        default partition def
);



INSERT INTO dml_union_r SELECT generate_series(1,100), generate_series(1,100) * 3,'r', generate_series(1,100) % 6;
INSERT INTO dml_union_r VALUES(NULL,NULL,'text',NULL),(NULL,NULL,'text',NULL),(NULL,NULL,'text',NULL),(NULL,NULL,'text',NULL),(NULL,NULL,'text',NULL);
INSERT INTO dml_union_r VALUES(1,1,'text',1),(2,2,'text',2),(3,3,'text',3),(4,4,'text',4),(5,5,'text',5);
INSERT INTO dml_union_r VALUES(1,1,'text',1),(2,2,'text',2),(3,3,'text',3),(4,4,'text',4),(5,5,'text',5);
INSERT INTO dml_union_r VALUES(1,2,'text',3),(2,3,'text',4),(3,4,'text',5),(4,5,'text',6),(5,6,'text',7);

INSERT INTO dml_union_s SELECT generate_series(1,100), generate_series(1,100) * 3,'s', generate_series(1,100) ;
INSERT INTO dml_union_s VALUES(1,1,'text',1),(2,2,'text',2),(3,3,'text',3),(4,4,'text',4),(5,5,'text',5);
INSERT INTO dml_union_s VALUES(1,1,'text',1),(2,2,'text',2),(3,3,'text',3),(4,4,'text',4),(5,5,'text',5);
INSERT INTO dml_union_s VALUES(1,2,'text',3),(2,3,'text',4),(3,4,'text',5),(4,5,'text',6),(5,6,'text',7);
