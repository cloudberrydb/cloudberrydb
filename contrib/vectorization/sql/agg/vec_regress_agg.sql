
-- drop table
DROP TABLE IF EXISTS hashagg_test;
CREATE TABLE hashagg_test (id1 int4, id2 int4, day date, grp text, v int4) WITH(APPENDONLY=true, ORIENTATION=column);

-- load data
set vector.enable_vectorization=off;

INSERT INTO hashagg_test VALUES (1,1,'1/1/2006','there',1);
INSERT INTO hashagg_test VALUES (1,1,'1/2/2006','there',2);
INSERT INTO hashagg_test VALUES (1,1,'1/3/2006','there',3);
INSERT INTO hashagg_test VALUES (1,1,'1/1/2006','hi',2);
INSERT INTO hashagg_test VALUES (1,1,'1/2/2006','hi',3);
INSERT INTO hashagg_test VALUES (1,1,'1/3/2006','hi',4);

-- queries
set vector.enable_vectorization=on;
select grp,sum(v) from hashagg_test where id1 = 1 and id2 = 1 and day between '1/1/2006' and '1/31/2006' group by grp order by sum(v) desc;

-- drop table
DROP TABLE hashagg_test;

