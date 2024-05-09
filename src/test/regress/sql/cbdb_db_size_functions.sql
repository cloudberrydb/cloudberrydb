DROP TABLE IF EXISTS cbdbheapsizetest;
DROP TABLE IF EXISTS cbdbaosizetest;
DROP TABLE IF EXISTS t_ext;
-- create heap table
CREATE TABLE cbdbheapsizetest(a int);
INSERT INTO cbdbheapsizetest select generate_series(1, 1000);

-- create ao table
CREATE TABLE cbdbaosizetest (a int) WITH (appendonly=true, orientation=row);
insert into cbdbaosizetest select generate_series(1, 100000);

-- create EXTERNAL table
CREATE EXTERNAL TABLE t_ext (a integer) LOCATION ('file://127.0.0.1/tmp/foo') FORMAT 'text';

WITH cbdbrelsize AS (
	SELECT *
	FROM cbdb_relation_size((SELECT array_agg(oid) FROM pg_class WHERE relkind != 'f'))
	), pgrelsize AS (
	SELECT pg_relation_size(oid) as size, relname, oid FROM pg_class
	WHERE relkind != 'f'
)
SELECT pgrelsize.relname, pgrelsize.size, cbdbrelsize.size
FROM pgrelsize FULL JOIN cbdbrelsize
ON pgrelsize.oid = cbdbrelsize.reloid
WHERE pgrelsize.size != cbdbrelsize.size;

SELECT size FROM cbdb_relation_size(array['t_ext'::regclass]);
SELECT * FROM cbdb_relation_size(array[]::oid[], 'main');
SELECT size FROM cbdb_relation_size(array['cbdbheapsizetest'::regclass], 'fsm');
