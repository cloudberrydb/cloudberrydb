
-- set vectorization
SET vector.enable_vectorization=off;

CREATE TABLE INT4_TBL(f1 int4) WITH(APPENDONLY=true, ORIENTATION=column);

INSERT INTO INT4_TBL(f1) VALUES ('   0  ');

INSERT INTO INT4_TBL(f1) VALUES ('123456     ');

INSERT INTO INT4_TBL(f1) VALUES ('    -123456');

INSERT INTO INT4_TBL(f1) VALUES ('34.5');

-- largest and smallest values
INSERT INTO INT4_TBL(f1) VALUES ('2147483647');

INSERT INTO INT4_TBL(f1) VALUES ('-2147483647');

-- bad input values -- should give errors
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
INSERT INTO INT4_TBL(f1) VALUES ('asdf');
INSERT INTO INT4_TBL(f1) VALUES ('     ');
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
INSERT INTO INT4_TBL(f1) VALUES ('123       5');
INSERT INTO INT4_TBL(f1) VALUES ('');

-- set vectorization
SET vector.enable_vectorization=on;

SELECT * FROM INT4_TBL;
SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int2 '0';
SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int4 '0';

-- SELECT '' AS five, * FROM INT4_TBL;

-- SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> int2 '0';

-- SELECT '' AS four, i.* FROM INT4_TBL i WHERE i.f1 <> int4 '0';

-- SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = int2 '0';

-- SELECT '' AS one, i.* FROM INT4_TBL i WHERE i.f1 = int4 '0';

-- SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < int2 '0';

-- SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 < int4 '0';

-- SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= int2 '0';

-- SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 <= int4 '0';

-- SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > int2 '0';

-- SELECT '' AS two, i.* FROM INT4_TBL i WHERE i.f1 > int4 '0';

-- SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= int2 '0';

-- SELECT '' AS three, i.* FROM INT4_TBL i WHERE i.f1 >= int4 '0';

-- positive odds
-- SELECT '' AS one, i.* FROM INT4_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- any evens
-- SELECT '' AS three, i.* FROM INT4_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

-- SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i;

-- SELECT '' AS five, i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i
-- WHERE abs(f1) < 1073741824;

-- SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i;

-- SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
-- WHERE abs(f1) < 1073741824;

-- SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i;

-- SELECT '' AS five, i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i
-- WHERE f1 < 2147483646;

-- SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i;

-- SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i
-- WHERE f1 < 2147483646;

-- SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i;

-- SELECT '' AS five, i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i
-- WHERE f1 > -2147483647;

-- SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i;

-- SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i
-- WHERE f1 > -2147483647;

-- SELECT '' AS five, i.f1, i.f1 / int2 '2' AS x FROM INT4_TBL i;

-- SELECT '' AS five, i.f1, i.f1 / int4 '2' AS x FROM INT4_TBL i;

-- drop table
DROP TABLE INT4_TBL;
