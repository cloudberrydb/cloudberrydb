-- dummy types use shell_in/shell_out that will error
-- if typein/typeout is really called

CREATE TYPE pg_ext_aux.int128;

CREATE FUNCTION pg_ext_aux.int128_type_in(cstring)
 RETURNS pg_ext_aux.int128
 AS '$libdir/vectorization', 'vector_int128_in'
 LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_ext_aux.int128_type_out(pg_ext_aux.int128)
 RETURNS cstring
 AS '$libdir/vectorization', 'vector_int128_out'
 LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE pg_ext_aux.int128(
 input = pg_ext_aux.int128_type_in,
 output = pg_ext_aux.int128_type_out,
 internallength = 16);


CREATE TYPE pg_ext_aux.stddev;

CREATE FUNCTION pg_ext_aux.stddev_type_in(cstring)
 RETURNS pg_ext_aux.stddev
 AS '$libdir/vectorization', 'vector_stddev_in'
 LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_ext_aux.stddev_type_out(pg_ext_aux.stddev)
 RETURNS cstring
 AS '$libdir/vectorization', 'vector_stddev_out'
 LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE pg_ext_aux.stddev(
 input = pg_ext_aux.stddev_type_in,
 output = pg_ext_aux.stddev_type_out,
 internallength = 8);

--
-- Dummy type used to convert arrow types
--

CREATE TYPE pg_ext_aux.arrow_avg_int_bytea AS (sum int, count int);

CREATE FUNCTION pg_ext_aux.round(float8, integer) RETURNS numeric as
$$
select pg_catalog.round($1::numeric, $2);
$$ LANGUAGE SQL;
