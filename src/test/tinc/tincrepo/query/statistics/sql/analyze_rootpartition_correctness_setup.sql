-- start_ignore
drop table if exists arp_test3;
set gp_create_table_random_default_distribution=off;
-- end_ignore

create table arp_test3(a int, b int) partition by range(a) (start(1) end(10) every(3));
insert into arp_test3 select i%4+1, i from generate_series(1,1000) i;
insert into arp_test3 select 2, i from generate_series(1001,1050) i;
insert into arp_test3 select 3, i from generate_series(1051,1225) i;
insert into arp_test3 select 4, i from generate_series(1226,1600) i;
