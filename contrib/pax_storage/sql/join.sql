-- start_ignore
create extension pax;
drop table if exists t1;
-- end_ignore
create table t1(v int) using pax distributed by(v);
insert into t1 select generate_series(1,10);
select * from t1 order by v;
update t1 set v=(select max(v) from t1) where v <= 5;
select * from t1 order by v;
select * from t1 as a join t1 as b on a.v=b.v where a.v<10;
