-- negative cases
SELECT test_receive();
SELECT test_send();
SELECT test_disconnect();

-- Test connection
SELECT test_connect('', pg_current_xlog_location());
-- Should report 1 replication
SELECT count(*) FROM pg_stat_replication;
SELECT test_disconnect();
SELECT count(*) FROM pg_stat_replication;

-- Test connection passing hostname
SELECT test_connect('host=localhost', pg_current_xlog_location());
SELECT count(*) FROM pg_stat_replication;
SELECT test_disconnect();
SELECT count(*) FROM pg_stat_replication;

-- create table and store current_xlog_location.
create TEMP table tmp(startpoint text);
CREATE FUNCTION select_tmp() RETURNS text AS $$
select startpoint from tmp;
$$ LANGUAGE SQL;
insert into tmp select pg_current_xlog_location();

-- lets generate some xlogs
create table testwalreceiver(a int);
insert into testwalreceiver select * from generate_series(0, 9);

-- Connect and receive the xlogs, validate everything was received from start to
-- end
SELECT test_connect('', select_tmp());
SELECT test_receive_and_verify(select_tmp(), pg_current_xlog_location());
SELECT test_send();
SELECT test_receive();
SELECT test_disconnect();
