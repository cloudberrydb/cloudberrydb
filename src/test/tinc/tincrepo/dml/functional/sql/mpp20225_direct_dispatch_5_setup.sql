\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP TABLE IF EXISTS m; 
CREATE TABLE m ();
set allow_system_table_mods='DML';
delete from gp_distribution_policy where localoid='m'::regclass;
reset allow_system_table_mods;
ALTER TABLE m ADD column a integer default null;
ALTER TABLE m ADD column b integer default null;
ALTER TABLE m ADD column c integer default null;
