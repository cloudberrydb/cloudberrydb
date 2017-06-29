-- This file is only to help debugging, in case the test fails.  It
-- should be executed in utility mode on the segment that is in
-- changetracking.

-- Flush CT log to disk
checkpoint;

select * from gp_changetracking_log(0) where rel =
(select relfilenode from pg_class where relname = 'resync_bug_table')
and
db = (select oid from pg_database where datname = current_database())
order by xlogloc;
