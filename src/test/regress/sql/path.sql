--
-- PATH
--

--DROP TABLE PATH_TBL;

CREATE TABLE PATH_TBL (s serial, f1 path);

INSERT INTO PATH_TBL(f1) VALUES ('[(1,2),(3,4)]');

INSERT INTO PATH_TBL(f1) VALUES ('((1,2),(3,4))');

INSERT INTO PATH_TBL(f1) VALUES ('[(0,0),(3,0),(4,5),(1,6)]');

INSERT INTO PATH_TBL(f1) VALUES ('((1,2),(3,4))');

INSERT INTO PATH_TBL(f1) VALUES ('1,2 ,3,4');

INSERT INTO PATH_TBL(f1) VALUES ('[1,2,3, 4]');

INSERT INTO PATH_TBL(f1) VALUES ('[11,12,13,14]');

INSERT INTO PATH_TBL(f1) VALUES ('(11,12,13,14)');

-- bad values for parser testing
INSERT INTO PATH_TBL(f1) VALUES ('[(,2),(3,4)]');

INSERT INTO PATH_TBL(f1) VALUES ('[(1,2),(3,4)');

SELECT f1 FROM PATH_TBL ORDER BY s;

SELECT '' AS count, f1 AS open_path FROM PATH_TBL WHERE isopen(f1) ORDER BY s;

SELECT '' AS count, f1 AS closed_path FROM PATH_TBL WHERE isclosed(f1) ORDER BY s;

SELECT '' AS count, pclose(f1) AS closed_path FROM PATH_TBL ORDER BY s;

SELECT '' AS count, popen(f1) AS open_path FROM PATH_TBL ORDER BY s;

