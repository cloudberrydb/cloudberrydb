drop database if exists gptest_gprestore;
drop database if exists gptest_pgrestore;
drop database if exists testdb1;
\echo -- start_ignore
-------------DISPLAYS ALL DATABASE OBJECTS WITH COMMENTS-

\dd
-----------Display all database available with comments

\l+
--------------------------Display all tables in the connected database-------
\dt+
-----------Display all sequence in the connected database----------
\ds+

--------------Display all view

\dv+
--------Display all operator in connected database
\do+
-------------------------------------------------------------------------


----Display all datatype in connected database
-----------------------------------------------------
\dt+
--------------------------------------------------------


-----------Display all function in connected database

\df+
---------------------------------------------------------------------

--- Querying a system table : pg_language ---

SELECT lanname, lanispl, lanpltrusted, lanacl FROM pg_language;
 
\dx
\da

\dC

------------------------------------------------------


\dc


--------Display tablespace

\db


---------------List Domain

\dD
drop table if exists bar_mpp2825;
create table bar_mpp2825 (a int, b int default 5);
\d
\d bar_mpp2825
drop table if exists bar_mpp2825;
\! createdb mpp8755 -l en_US.utf8
\! createdb mpp8755 -l utf8
show track_activities; -- ON
show track_counts; -- OFF
select datname,application_name,xact_start from pg_stat_activity where application_name='mpp11646';

\! PGAPPNAME='mpp11646' psql gptest -c 'select pg_sleep(3)' > /dev/null 2>&1 &
\! PGAPPNAME='mpp11646' psql template1 -c 'select pg_sleep(3)' > /dev/null 2>&1 &
select pg_sleep(1);

select datname,application_name,xact_start from pg_stat_activity where application_name='mpp11646' order by datname;
\! PGAPPNAME="1234567890123456789012345678901234567890123456789012345678901234567890" psql template1
-- If no guc on, this should fail
\echo '-- Expected failure'
update gp_segment_configuration set role='t' where role='p' and content<>-1;
\echo '-- System catalog is updated using GUC'
update gp_segment_configuration set role='t' where role='p' and content=0;
select count(*) from gp_segment_configuration where role='t' limit 1;

update gp_segment_configuration set role='p' where role='t' and content=0;
select * from gp_segment_configuration where role='t';
\echo '-- System catalog is updated per session level'
show allow_system_table_mods;
set allow_system_table_mods=dml;
update gp_segment_configuration set role='t' where role='p' and content=0;
select count(*) from gp_segment_configuration where role='t' limit 1;

update gp_segment_configuration set role='p' where role='t' and content=0;
select * from gp_segment_configuration where role='t';
\! gpconfig -c allow_system_table_mods -v dml 
\echo 'Default value'
show allow_system_table_mods;

\echo 'Allowed value'
set allow_system_table_mods=dml;
show allow_system_table_mods;
set allow_system_table_mods=DML;
show allow_system_table_mods;
set allow_system_table_mods=none;
show allow_system_table_mods;

\echo 'Incorrect value, expected to fail'
set allow_system_table_mods=dml2;
set allow_system_table_mods=123;
set allow_system_table_mods=abc;
set allow_system_table_mods=true;
set allow_system_table_mods=false;
\echo 'Cannot Set in session, expected to fail'
set allow_system_table_mods="ddl";
set allow_system_table_mods="DDL";
set allow_system_table_mods="all";
set allow_system_table_mods="ALL";
\echo -- end_ignore
