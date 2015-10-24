create schema input;
create schema output;
create table input.qualified(a int, b text) distributed by (a);
insert into input.qualified values(1, 'one');
insert into input.qualified values(2, 'two');
