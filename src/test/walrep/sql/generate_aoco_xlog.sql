-- Test AOCO XLogging

CREATE OR REPLACE FUNCTION get_aoco_eofs(tablename TEXT) RETURNS BIGINT[] AS
$$
DECLARE
eofvals BIGINT[];
i      INT;
result    RECORD;
BEGIN
   i := 0;
   FOR result IN SELECT * FROM gp_toolkit.__gp_aocsseg_name(tablename)
   LOOP
	eofvals[i] := result.eof;
	i := i + 1;
   END LOOP;
   RETURN eofvals;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE generate_aoco_xlog_table(a INT, b INT) WITH (APPENDONLY=TRUE, ORIENTATION=COLUMN);

-- Store the location of xlog in a temporary table so that we can
-- use it to request walsender to start streaming from this point
CREATE TEMP TABLE tmp(startpoint TEXT);
INSERT INTO tmp SELECT pg_current_xlog_location() FROM
gp_dist_random('gp_id') WHERE gp_segment_id = 0;

-- Generate some xlog records for AOCO
INSERT INTO generate_aoco_xlog_table VALUES(1, 10);

-- Verify that AOCO xlog record was received
SELECT test_xlog_ao((SELECT 'port=' || port FROM gp_segment_configuration
                     WHERE dbid=2),
		    (SELECT startpoint FROM tmp),
                    (SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'),
                    (SELECT oid FROM pg_database
		     WHERE datname = current_database()),
	            (SELECT relfilenode FROM gp_dist_random('pg_class')
		     WHERE relname = 'generate_aoco_xlog_table'
		     AND gp_segment_id = 0),
		    (SELECT get_aoco_eofs('generate_aoco_xlog_table')
		     FROM gp_dist_random('gp_id')
		     WHERE gp_segment_id = 0),
		    true) FROM
gp_dist_random('gp_id') WHERE gp_segment_id = 0;

