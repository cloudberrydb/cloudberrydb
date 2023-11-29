-- cases provided by clickhouse and simplified them

-- crash down caused by memory context reset

-- SET VEC OFF
SET vector.enable_vectorization=off;

DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (a int, b text) WITH(APPENDONLY=true, ORIENTATION=column) DISTRIBUTED BY(a);

-- create the random string function
create or replace function f_repeat_str(l integer, c integer) returns text AS $BODY$ declare result text; begin select array_to_string(ARRAY(SELECT chr((65 + c)::integer) FROM generate_series(1, l)), '') into result; return result; end; $BODY$ language plpgsql;

INSERT INTO t1 SELECT i % 15, f_repeat_str(8, i % 15) FROM generate_series(1, 50000)i;

-- SET VEC ON
SET vector.enable_vectorization=on;
SET optimizer=off;

-- query
SELECT min(b) FROM t1 WHERE b != '' GROUP BY b LIMIT 15;

-- SET VEC OFF
SET vector.enable_vectorization=off;
SET optimizer=on;

-- drop table

DROP TABLE IF EXISTS t1;

-- drop function

DROP FUNCTION f_repeat_str(length INTEGER, c INTEGER);

-- SET VEC ON
SET vector.enable_vectorization=on;




