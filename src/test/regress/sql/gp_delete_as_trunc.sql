-- Test coverage for MPP-5028.

drop table if exists delete_as_trunc;
create table delete_as_trunc (i int, j int) partition by range(j) (start(1) end(10) every(2), default partition def);
insert into delete_as_trunc select j, j from generate_series(1, 20) j;
select * from delete_as_trunc;
delete from delete_as_trunc where j >= 1 and j < 3;
select * from delete_as_trunc;

drop table if exists delete_as_trunc;

set gp_enable_delete_as_truncate to on;

create table delete_as_trunc (i int, j int) partition by range(j) (start(1) end(10) every(2), default partition def);
insert into delete_as_trunc select j, j from generate_series(1, 20) j;
select * from delete_as_trunc;
delete from delete_as_trunc where j >= 1 and j < 3;
select * from delete_as_trunc;
drop table if exists delete_as_trunc;
