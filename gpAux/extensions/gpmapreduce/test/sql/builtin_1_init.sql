CREATE TYPE wordsplit_out AS (key text, value int4);

CREATE FUNCTION wordsplit_1(text) returns setof wordsplit_out as '$libdir/gpmrdemo', 'wordsplit' language C;
CREATE FUNCTION wordsplit_2(IN value text, OUT key text, OUT value int4) returns setof record as '$libdir/gpmrdemo', 'wordsplit' language C;

CREATE FUNCTION myadd(int8, int4) returns int8 as '$libdir/gpmrdemo', 'int4_accum' language C;
CREATE FUNCTION mysum(int8, int8) returns int8 as '$libdir/gpmrdemo', 'int8_add' language C;

