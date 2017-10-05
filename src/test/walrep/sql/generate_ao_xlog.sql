-- Test AO XLogging

CREATE OR REPLACE FUNCTION get_ao_eof(tablename TEXT) RETURNS BIGINT[] AS
$$
DECLARE
eofval BIGINT[];
eof_scalar BIGINT;
BEGIN
   SELECT eof INTO eof_scalar FROM gp_toolkit.__gp_aoseg_name(tablename);
   eofval[0] := eof_scalar;
   RAISE NOTICE 'eof %', eofval[0];
   RETURN eofval;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_xlog_ao_wrapper(IN tablespace_oid oid, IN database_oid oid,
    	  	   			   IN tablename TEXT, IN startpoints TEXT[])
RETURNS TABLE (insert_cnt INT, truncate_cnt INT) EXECUTE ON ALL SEGMENTS AS
$func$
DECLARE
port TEXT;
relnode INT;
startpoint TEXT;
myseg INT;
BEGIN
	SELECT relfilenode INTO relnode FROM pg_class where relname = tablename;
	myseg := gp_execution_segment();
	startpoint := startpoints[myseg + 1];
	select 'port=' || setting INTO port from pg_settings where name = 'port';
	RAISE NOTICE 'port %, relfilenode % tablename % startpoint % current_xlog_location %',
	port, relnode, tablename, startpoint, pg_current_xlog_location();


	RETURN QUERY
	select insert_count, truncate_count from test_xlog_ao(port,
			startpoint, tablespace_oid, database_oid, relnode,
			(SELECT get_ao_eof(tablename)), false);
END;
$func$ LANGUAGE plpgsql;

drop table generate_ao_xlog_table;
CREATE TABLE generate_ao_xlog_table(a INT, b INT) WITH (APPENDONLY=TRUE);

-- Store the location of xlog in a temporary table so that we can
-- use it to request walsender to start streaming from this point
CREATE TEMP TABLE tmp(dbid int, startpoint TEXT);
INSERT INTO tmp SELECT gp_execution_segment(),pg_current_xlog_location() FROM
gp_dist_random('gp_id');
create TEMP table xlog_startpoint as select array_agg(startpoint) startpoints from (select startpoint from tmp order by dbid) tt;

-- Generate some xlog records for AO
INSERT INTO generate_ao_xlog_table VALUES(1, 10), (8, 10), (3, 10);

-- Verify that AO xlog record was received
SELECT * from test_xlog_ao_wrapper((SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'),
       	      		(SELECT oid FROM pg_database WHERE datname = current_database()),
		     'generate_ao_xlog_table',
		     (SELECT startpoints FROM xlog_startpoint));
