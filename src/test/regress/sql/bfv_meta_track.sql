--
--CBDB GITHUB ISSUE:
--https://github.com/cloudberrydb/cloudberrydb/issues/504 
--

create schema bfv_meta_track;
set search_path to bfv_meta_track;
select count(*) from pg_stat_last_operation join
  pg_namespace on pg_namespace.oid = pg_stat_last_operation.objid
  where pg_namespace.nspname = 'bfv_meta_track';

-- test drop popicy
create table t1(a int);
create policy p1 on t1 using (a % 2 = 0);
select count(*) from pg_stat_last_operation a join pg_policy b on b.oid = a.objid where b.polname = 'p1' and b.polrelid ='t1'::regclass::oid;
drop policy p1 on t1;
select count(*) from pg_stat_last_operation a join pg_policy b on b.oid = a.objid where b.polname = 'p1' and b.polrelid ='t1'::regclass::oid;

--test drop publication
-- start_ignore
create publication pub1;
-- end_ignore
select count(*) from pg_stat_last_operation a join pg_publication b on b.oid = a.objid where b.pubname = 'pub1';
drop publication pub1;
select count(*) from pg_stat_last_operation a join pg_publication b on b.oid = a.objid where b.pubname = 'pub1';

drop schema bfv_meta_track cascade;
-- test drop schema
select count(*) from pg_stat_last_operation join
  pg_namespace on pg_namespace.oid = pg_stat_last_operation.objid
  where pg_namespace.nspname = 'bfv_meta_track';