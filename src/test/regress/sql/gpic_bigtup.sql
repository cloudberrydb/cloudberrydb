drop table if exists multi1_for_gpic_bigtup;
drop table if exists multi2_for_gpic_bigtup;
drop table if exists multi3_for_gpic_bigtup;

drop table if exists foo_for_gpic_bigtup;
drop table if exists skewed_for_gpic_bigtup;

set gp_interconnect_type=tcp;

create table skewed_for_gpic_bigtup (a int, b int);
insert into skewed_for_gpic_bigtup select 1, i from generate_series(1, 1000) i;

create table foo_for_gpic_bigtup as select *, 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'::text as c from skewed_for_gpic_bigtup;

-- Tests for tuple-serialization.
create table multi1_for_gpic_bigtup (id int, text1 text, text2 text, id2 int);
create table multi2_for_gpic_bigtup (id int, text1 text, text2 text);
create table multi3_for_gpic_bigtup (text1 text, id int, text2 text);

insert into multi1_for_gpic_bigtup values (1, repeat('lfsda;jfewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 5);
insert into multi1_for_gpic_bigtup values (2, repeat('ldsf.fafewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 1941);
insert into multi1_for_gpic_bigtup values (3, repeat('lcx..faaewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 45);
insert into multi1_for_gpic_bigtup values (4, repeat('loewirqul', 10000), repeat('138471473asdf', 2048), 1923);
insert into multi1_for_gpic_bigtup values (5, repeat('lfsda;jfl;dsjfkldsafiwajsal', 10000), repeat('138471473asdf', 2048), 8888);
insert into multi1_for_gpic_bigtup values (6, repeat('lfsda#@$!#@$!@l', 10000), repeat('138471473asdf', 2048), 1);
insert into multi1_for_gpic_bigtup values (7, repeat('lfsd/ew/rewqrfewauir@#%!#$l', 10000), repeat('138471473asdf', 2048), 5);
insert into multi1_for_gpic_bigtup values (8, repeat('lfssa.,/mdfweiourewioajsal', 10000), repeat('138471473asdf', 2048), 35);
insert into multi1_for_gpic_bigtup values (9, repeat('lfsdaxc.,fmxc,msal', 10000), repeat('138471473asdf', 2048), 67);
insert into multi1_for_gpic_bigtup values (10, repeat('lfsda;je;lkjsakl;jrfewsal', 10000), repeat('138471473asdf', 2048), 12);
insert into multi1_for_gpic_bigtup values (11, repeat('lfsda;jpouiewpruqwsal', 10000), repeat('138471473asdf', 2048), 89);
insert into multi1_for_gpic_bigtup values (12, repeat(';jfewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 78);
insert into multi1_for_gpic_bigtup values (13, repeat('lfdfasjf;ljdsafkljsa;jfewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 34);
insert into multi1_for_gpic_bigtup values (14, repeat('a;jfewi;uiwaasfajsal', 10000), repeat('138471473asdf', 2048), 100);
insert into multi1_for_gpic_bigtup values (15, repeat('lfsda;jfewi;uiwafdsasajsal', 10000), repeat('138471473asdf', 2048), 8);
insert into multi1_for_gpic_bigtup values (16, repeat('lfsda;jfewi;uras;klj;saiwajsal', 10000), repeat('138471473asdf', 2048), 2);

-- Really massive rows
insert into multi1_for_gpic_bigtup values (100, repeat('342178040832187091iwajsal', 100000), repeat('1384eqrkljewqklrj71473asdf', 100000), 2);
insert into multi1_for_gpic_bigtup values (101, repeat('blah blah blah blah342178040832187091iwajsal', 100000), repeat('13foo foo_for_gpic_bigtup foo_for_gpic_bigtup foo84eqrkljewqklrj71473asdf', 100000), 2);
insert into multi1_for_gpic_bigtup values (101, repeat('0832187091iwajsal!#$#@!$!$!', 100000), repeat('71473asdf!E$!@#$!@!#', 100000), 2);

insert into multi2_for_gpic_bigtup (id, text1, text2) select m1.id, m1.text2, m1.text1 from multi1_for_gpic_bigtup m1;

insert into multi3_for_gpic_bigtup (text1, id, text2) select m1.text2, m1.id, m1.text1 from multi1_for_gpic_bigtup m1;

-- join the big rows to our other table

select * from multi3_for_gpic_bigtup, foo_for_gpic_bigtup where text1 = c;

select text1,length(text1), c from multi3_for_gpic_bigtup left outer join foo_for_gpic_bigtup on (text1=c) order by text1 limit 10;

drop table if exists multi1_for_gpic_bigtup;
drop table if exists multi2_for_gpic_bigtup;
drop table if exists multi3_for_gpic_bigtup;

drop table if exists foo_for_gpic_bigtup;
drop table if exists skewed_for_gpic_bigtup;

set gp_interconnect_type=udp;

create table skewed_for_gpic_bigtup (a int, b int);
insert into skewed_for_gpic_bigtup select 1, i from generate_series(1, 1000) i;

create table foo_for_gpic_bigtup as select *, 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'::text as c from skewed_for_gpic_bigtup;

-- Tests for tuple-serialization.
create table multi1_for_gpic_bigtup (id int, text1 text, text2 text, id2 int);
create table multi2_for_gpic_bigtup (id int, text1 text, text2 text);
create table multi3_for_gpic_bigtup (text1 text, id int, text2 text);

insert into multi1_for_gpic_bigtup values (1, repeat('lfsda;jfewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 5);
insert into multi1_for_gpic_bigtup values (2, repeat('ldsf.fafewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 1941);
insert into multi1_for_gpic_bigtup values (3, repeat('lcx..faaewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 45);
insert into multi1_for_gpic_bigtup values (4, repeat('loewirqul', 10000), repeat('138471473asdf', 2048), 1923);
insert into multi1_for_gpic_bigtup values (5, repeat('lfsda;jfl;dsjfkldsafiwajsal', 10000), repeat('138471473asdf', 2048), 8888);
insert into multi1_for_gpic_bigtup values (6, repeat('lfsda#@$!#@$!@l', 10000), repeat('138471473asdf', 2048), 1);
insert into multi1_for_gpic_bigtup values (7, repeat('lfsd/ew/rewqrfewauir@#%!#$l', 10000), repeat('138471473asdf', 2048), 5);
insert into multi1_for_gpic_bigtup values (8, repeat('lfssa.,/mdfweiourewioajsal', 10000), repeat('138471473asdf', 2048), 35);
insert into multi1_for_gpic_bigtup values (9, repeat('lfsdaxc.,fmxc,msal', 10000), repeat('138471473asdf', 2048), 67);
insert into multi1_for_gpic_bigtup values (10, repeat('lfsda;je;lkjsakl;jrfewsal', 10000), repeat('138471473asdf', 2048), 12);
insert into multi1_for_gpic_bigtup values (11, repeat('lfsda;jpouiewpruqwsal', 10000), repeat('138471473asdf', 2048), 89);
insert into multi1_for_gpic_bigtup values (12, repeat(';jfewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 78);
insert into multi1_for_gpic_bigtup values (13, repeat('lfdfasjf;ljdsafkljsa;jfewi;uiwajsal', 10000), repeat('138471473asdf', 2048), 34);
insert into multi1_for_gpic_bigtup values (14, repeat('a;jfewi;uiwaasfajsal', 10000), repeat('138471473asdf', 2048), 100);
insert into multi1_for_gpic_bigtup values (15, repeat('lfsda;jfewi;uiwafdsasajsal', 10000), repeat('138471473asdf', 2048), 8);
insert into multi1_for_gpic_bigtup values (16, repeat('lfsda;jfewi;uras;klj;saiwajsal', 10000), repeat('138471473asdf', 2048), 2);

-- Really massive rows
insert into multi1_for_gpic_bigtup values (100, repeat('342178040832187091iwajsal', 100000), repeat('1384eqrkljewqklrj71473asdf', 100000), 2);
insert into multi1_for_gpic_bigtup values (101, repeat('blah blah blah blah342178040832187091iwajsal', 100000), repeat('13foo foo_for_gpic_bigtup foo_for_gpic_bigtup foo84eqrkljewqklrj71473asdf', 100000), 2);
insert into multi1_for_gpic_bigtup values (101, repeat('0832187091iwajsal!#$#@!$!$!', 100000), repeat('71473asdf!E$!@#$!@!#', 100000), 2);

insert into multi2_for_gpic_bigtup (id, text1, text2) select m1.id, m1.text2, m1.text1 from multi1_for_gpic_bigtup m1;

insert into multi3_for_gpic_bigtup (text1, id, text2) select m1.text2, m1.id, m1.text1 from multi1_for_gpic_bigtup m1;

-- join the big rows to our other table

select * from multi3_for_gpic_bigtup, foo_for_gpic_bigtup where text1 = c;

select text1,length(text1), c from multi3_for_gpic_bigtup left outer join foo_for_gpic_bigtup on (text1=c) order by text1 limit 10;