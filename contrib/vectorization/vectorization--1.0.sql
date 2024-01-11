--
-- Schema to deploy vector objects
--
CREATE SCHEMA IF NOT EXISTS vector;

CREATE TYPE vector.int128; 
CREATE TYPE vector.stddev;
--
-- Dummy type used to convert arrow types
--

CREATE TYPE vector.arrow_avg_int_bytea AS (sum int, count int);

create function round(float8, integer) returns numeric as
$$
select pg_catalog.round($1::numeric, $2);
$$ language sql;