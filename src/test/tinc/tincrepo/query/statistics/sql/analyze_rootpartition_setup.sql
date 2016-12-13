-- start_ignore
drop table if exists arp_test;
drop table if exists arp_test2;
set gp_create_table_random_default_distribution=off;
-- end_ignore

create table arp_test(a int, b int) partition by range(b) (start(1) end(10) every(3));
insert into arp_test select i, i%9+1 from generate_series(1,1000) i;

create table arp_test2(a int, b int) partition by range(a) (start(1) end(10) every(3));
insert into arp_test2 select i%9+1, i from generate_series(1,1000) i;
