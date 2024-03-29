set pax_enable_filter = on;
create table pax_test.null_test_t(a int, b int, c text) using pax;

insert into pax_test.null_test_t(a) select null from generate_series(1,2)i;
insert into pax_test.null_test_t select 1, i, 'cc_' || i from generate_series(1,2)i;

insert into pax_test.null_test_t select 1, i, null from generate_series(1,2)i;

select * from pax_test.null_test_t t where t.* is null;
select * from pax_test.null_test_t t where t.* is not null;

drop table pax_test.null_test_t;
reset pax_enable_filter;
