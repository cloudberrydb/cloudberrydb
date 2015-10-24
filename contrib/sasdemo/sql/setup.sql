--
-- Setup external tables to allow:
--    * "reading" from the database to parallel streams
--    * "writing" into the database from parallel streams 



--
-- This table is our example database table
--
DROP TABLE IF EXISTS example CASCADE;
CREATE TABLE example(
   id       varchar(10),
   name     varchar(20),
   value1   float8,
   value2   float8,
   value3   float8,
   value4   float8
) 
WITH (appendonly = true) DISTRIBUTED BY (id);


--
-- Define our binary formatter function
--
CREATE OR REPLACE FUNCTION formatter_export(record) RETURNS bytea 
AS '$libdir/gpformatter.so', 'formatter_export'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION formatter_import() RETURNS record
AS '$libdir/gpformatter.so', 'formatter_import'
LANGUAGE C IMMUTABLE;

--
-- Setup an external table to read from this (e.g, have the database WRITE out)
--
DROP EXTERNAL TABLE IF EXISTS example_out;
CREATE WRITABLE EXTERNAL WEB TABLE example_out(like example)
EXECUTE '$GPHOME/bin/sassender localhost 2000' FORMAT 'CUSTOM' (FORMATTER ‘formatter_export’) DISTRIBUTED BY (id);

--
-- Setup an external table to write into the table (e.g. have the database read in new rows)
--
DROP EXTERNAL TABLE IF EXISTS example_in;
CREATE EXTERNAL WEB TABLE example_in(like example)
EXECUTE '$GPHOME/bin/sasreceiver localhost 2000' ON ALL FORMAT 'CUSTOM' (FORMATTER ‘formatter_import’);


---
--- DATA GENERATION
---
CREATE OR REPLACE FUNCTION dat_gen(c int) RETURNS SETOF example as $$
import random, math
random.seed(1)
alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
for x in range(c):
  id     = ''.join(random.sample(alphabet, int(random.random()*7+3)))
  name   = ''.join(random.sample(alphabet, int(random.random()*12+8)))
  value1 = random.random() * 100 + 20
  value2 = value1 - 10 + random.random()*math.log(value1)*5
  value3 = (value1 * value2) / (value1 + value2)**2
  value4 = math.log(value1) + random.random()*3
  yield {'id': id, 'name':  name, 'value1': value1, 'value2': value2, 'value3': value3, 'value4': value4}
$$ language plpythonu;

-- Add 1K records on each segment
\echo 'loading data...'
INSERT INTO example SELECT (a).* FROM (SELECT dat_gen(1000) a from gp_dist_random('gp_id')) q;
select 'loaded ' || pg_size_pretty(pg_relation_size('example')) || ' of data' as "data loaded";

-- Done with the data generation function
DROP FUNCTION dat_gen(int);
