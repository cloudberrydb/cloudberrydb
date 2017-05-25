-- Test AO XLogging

CREATE OR REPLACE FUNCTION get_ao_eof(tablename TEXT) RETURNS BIGINT[] AS
$$
DECLARE
eofval BIGINT[];
eof_scalar BIGINT;
BEGIN
   SELECT eof INTO eof_scalar FROM gp_toolkit.__gp_aoseg_name(tablename);
   eofval[0] := eof_scalar;
   RETURN eofval;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE generate_ao_xlog_table(a INT, b INT) WITH (APPENDONLY=TRUE);

-- Store the location of xlog in a temporary table so that we can
-- use it to request walsender to start streaming from this point
CREATE TEMP TABLE tmp(startpoint TEXT);
INSERT INTO tmp SELECT pg_current_xlog_location() FROM
gp_dist_random('gp_id') WHERE gp_segment_id = 0;

-- Generate some xlog records for AO
INSERT INTO generate_ao_xlog_table VALUES(1, 10);

-- Verify that AO xlog record was received
SELECT test_xlog_ao((SELECT 'port=' || port FROM gp_segment_configuration
       		     WHERE dbid=2),
		    (SELECT startpoint FROM tmp),
                    (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'),
                    (SELECT oid FROM pg_database
		     WHERE datname = current_database()),
                    (SELECT relfilenode FROM gp_dist_random('pg_class')
		     WHERE relname = 'generate_ao_xlog_table'
		     AND gp_segment_id = 0),
		    (SELECT get_ao_eof('generate_ao_xlog_table')
		     FROM gp_dist_random('gp_id')
		     WHERE gp_segment_id = 0),
		    false) FROM
gp_dist_random('gp_id') WHERE gp_segment_id = 0;

