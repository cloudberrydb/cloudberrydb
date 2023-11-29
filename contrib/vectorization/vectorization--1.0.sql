\echo Use "CREATE EXTENSION vectorization" to load this file. \quit
--
-- Schema to deploy vector objects
--
set vector.enable_vectorization = off;
CREATE SCHEMA IF NOT EXISTS vector;

CREATE TYPE vector.int128; 
CREATE TYPE vector.stddev;
--
-- Dummy type used to convert arrow types
--

CREATE TYPE vector.arrow_avg_int_bytea AS (sum int, count int);

set vector.enable_vectorization = on;

create function round(float8, integer) returns float8 as
$$
select pg_catalog.round($1::numeric, $2);
$$ language sql;
