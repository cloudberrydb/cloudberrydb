-- Create table and insert data while cluster is in-sync.
drop table if exists resync_bug_table;
create table resync_bug_table(a int, b int) distributed by (a);
-- Insert tuples on a single statement.  Insert enough tuples to
-- occupy 5 blocks on that segment.  About 901 tuples having this
-- schema can be accommodated on one block.
insert into resync_bug_table select 1 ,i from generate_series(1,5000)i;
-- Checkpoint isn't really necessary but doesn't harm either.
checkpoint;
