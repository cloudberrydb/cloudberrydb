-- Install a helper function to inject faults, using the fault injection
-- mechanism built into the server.
-- start_matchignore
-- m/ERROR:  extension "gp_inject_fault" already exists/
-- end_matchignore
CREATE EXTENSION gp_inject_fault;
begin;
-- inject fault of type sleep on all primaries
select gp_inject_fault('finish_prepared_after_record_commit_prepared',
       'sleep', '', '', '', 1, 2, 0, dbid) from gp_segment_configuration
       where role = 'p' and content > -1;
-- check fault status
select gp_inject_fault('finish_prepared_after_record_commit_prepared',
       'status', '', '', '', 1, 2, 0, dbid) from gp_segment_configuration
       where role = 'p' and content > -1;
-- commit transaction should trigger the fault
end;
-- fault status should indicate it's triggered
select gp_inject_fault('finish_prepared_after_record_commit_prepared',
       'status', '', '', '', 1, 2, 0, dbid) from gp_segment_configuration
       where role = 'p' and content > -1;
-- reset the fault on all primaries
select gp_inject_fault('finish_prepared_after_record_commit_prepared',
       'reset', '', '', '', 1, 2, 0, dbid) from gp_segment_configuration
       where role = 'p' and content > -1;
-- inject an infinite skip fault into the 'fts_probe' fault to disable FTS probing on qd
select gp_inject_fault_infinite('fts_probe',
       'skip', dbid) from gp_segment_configuration where role = 'p' and content = -1;
-- inject a panic faults on qe when FTS probe is already skipped
select gp_inject_fault('checkpoint',
       'panic', dbid) from gp_segment_configuration where role = 'p' and content = 0;
-- reset the infinite skip fault on qd
select gp_inject_fault_infinite('fts_probe',
       'reset', dbid) from gp_segment_configuration where role = 'p' and content = -1;
-- reset the panic on qe
select gp_inject_fault('checkpoint',
       'reset', dbid) from gp_segment_configuration where role = 'p' and content = 0;
-- inject a panic fault on qd when FTS probe is not skipped
select gp_inject_fault('checkpoint',
       'panic', dbid) from gp_segment_configuration where role = 'p' and content = -1;
-- inject a panic fault on qe when FTS probe is not skipped
select gp_inject_fault('checkpoint',
       'panic', dbid) from gp_segment_configuration where role = 'p' and content = 0;
-- reset the panic fault on qd
select gp_inject_fault('checkpoint',
       'reset', dbid) from gp_segment_configuration where role = 'p' and content = -1;
-- reset the panic fault on qe
select gp_inject_fault('checkpoint',
       'reset', dbid) from gp_segment_configuration where role = 'p' and content = 0;

DROP EXTENSION gp_inject_fault;
