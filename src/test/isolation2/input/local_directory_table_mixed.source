-- Mixed test for local directory table
CREATE TABLESPACE directory_tblspc LOCATION '@testtablespace@';

-- Test create directory table and create directory table
1: BEGIN;
2: BEGIN;
1: CREATE DIRECTORY TABLE dir_table1 TABLESPACE directory_tblspc;
2: CREATE DIRECTORY TABLE dir_table2 TABLESPACE directory_tblspc;
1: COMMIT;
2: COMMIT;

1: \COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation1';
2: \COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation2';
3: \COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation3';
4: \COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation4';

-- Test copy binary from the same directory table in parallel session is allowed
1: BEGIN;
2: BEGIN;
1: \COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation5';
2: \COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation6';
1: COMMIT;
2: COMMIT;

-- Test copy binary from and select directory table
1: BEGIN;
2: BEGIN;
1: \COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation5';
2: SELECT * FROM dir_table2;
1: COMMIT;
2: COMMIT;

1: BEGIN;
2: BEGIN;
1: \COPY BINARY dir_table2 FROM '@abs_srcdir@/data/nation.csv' 'nation6';
2: SELECT * FROM directory_table('dir_table2');
1: COMMIT;
2: COMMIT;

-- Test copy binary from and remove_file()
1: BEGIN;
2: BEGIN;
1: \COPY BINARY dir_table1 FROM '@abs_srcdir@/data/nation.csv' 'nation7';
2: SELECT remove_file('dir_table1', 'nation6');
1: COMMIT;
2: COMIMT;

-- Test select and select
1: BEGIN;
2: BEGIN;
1: SELECT * FROM dir_table1;
2: SELECT * FROM directory_table('dir_table2');
1: SELECT * FROM directory_table('dir_table2');
2: SELECT * FROM dir_table1;
2: COMMIT;
1: COMMIT;

-- Test select and remove_file()
1: BEGIN;
2: BEGIN;
1: SELECT * FROM dir_table1;
2&: SELECT remove_file('dir_table1', 'nation7');
1: COMMIT;
2<:
2: COMMIT;

1: BEGIN;
2: BEGIN;
1: SELECT remove_file('dir_table1', 'nation5');
2&: SELECT * FROM dir_table1;
1: COMMIT;
2<:
2: COMMIT;

1: BEGIN;
2: BEGIN;
1: SELECT remove_file('dir_table2', 'nation6');
2&: SELECT * FROM directory_table('dir_table2');
1: COMMIT;
2<:
2: COMMIT;

1: BEGIN;
2: BEGIN;
1: SELECT * FROM directory_table('dir_table2');
2&: SELECT remove_file('dir_table2', 'nation5');
1: COMMIT;
2<:
2: COMMIT;

1: BEGIN;
2: BEGIN;
1: SELECT * FROM dir_table1;
2: SELECT remove_file('dir_table2', 'nation4');
2: COMMIT;
1: COMMIT;

1: BEGIN;
2: BEGIN;
1: SELECT remove_file('dir_table1', 'nation4');
2: SELECT * FROM dir_table2;
2: COMMIT;
1: COMMIT;

-- Test create directory table and drop directory table
1: BEGIN;
2: BEGIN;
1: CREATE DIRECTORY TABLE dir_table3 TABLESPACE directory_tblspc;
2: DROP DIRECTORY TABLE dir_table2;
2: CREATE DIRECTORY TABLE dir_table2 TABLESPACE directory_tblspc;
1: COMMIT;
2: COMMIT;

-- Test drop directory table and copy from/select/remove
1: BEGIN;
2: BEGIN;
1: \COPY BINARY dir_table3 FROM '@abs_srcdir@/data/nation.csv' 'nation';
2&: DROP DIRECTORY TABLE dir_table3;
1: COMMIT;
2:<
2: COMMIT;

1: BEGIN;
2: BEGIN;
1: SELECT * FROM dir_table2;
2&: DROP DIRECTORY TABLE dir_table2;
1: COMMIT;
2:<
2: COMMIT;

1: BEGIN;
2: BEGIN;
1: SELECT remove_file('dir_table1', 'nation1');
2&: DROP DIRECTORY TABLE dir_table1;
1: COMMIT;
2:<
2: COMMIT;