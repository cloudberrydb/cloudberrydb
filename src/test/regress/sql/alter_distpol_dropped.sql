-- MPP-5928
-- We want to test a whole bunch of different type configurations and whether
-- ATSDB can handle them as dropped types
-- Here's a script to generate all this:

--align="int2 int4 char double"
--length="variable 1 3 4 11 17 19 23 32 196"
--pbv="true false"
--storage="true false"
--
--for a in $align
--do
--	for l in $length
--	do
--		for p in $pbv
--		do
--			for s in $storage
--			do
--				if [ $p == "true" ] && [ $l != "variable" ] && [ $l -gt 8 ];
--				then
--					continue
--				fi
--				if [ $p == "true" ] && [ $l == "variable" ];
--				then
--					continue
--				fi
--				echo "
--drop table alter_distpol_g;
--create type break;
--create function breakin (cstring) returns break as 'textin' language internal;
--create function breakout (break) returns cstring as 'textout' language internal;"
--
--				echo "create type break (input = breakin, output = breakout, internallength = $l, passedbyvalue = $p, alignment = $a);"
--				echo "
--create table alter_distpol_g (i int, j break, k text) with (appendonly = $s);
--insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
--alter table alter_distpol_g drop column j;
--select * from alter_distpol_g order by 1;
--alter table alter_distpol_g set with(reorganize = true) distributed randomly;
--select * from alter_distpol_g order by 1;
--drop type break cascade;
--alter table alter_distpol_g set with(reorganize = true) distributed randomly;
--select * from alter_distpol_g order by 1;"
--			done
--		done
--	done
--done

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = variable, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = variable, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = true, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = true, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = true, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = true, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = true, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = true, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 11, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 11, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 17, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 17, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 19, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 19, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 23, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 23, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 32, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 32, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 196, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 196, passedbyvalue = false, alignment = int2);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = variable, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = variable, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = true, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = true, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = true, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = true, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = true, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = true, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 11, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 11, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 17, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 17, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 19, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 19, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 23, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 23, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 32, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 32, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 196, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 196, passedbyvalue = false, alignment = int4);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = variable, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = variable, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = true, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = true, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = true, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = true, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = true, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = true, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 11, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 11, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 17, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 17, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 19, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 19, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 23, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 23, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 32, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 32, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 196, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 196, passedbyvalue = false, alignment = char);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = variable, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = variable, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = true, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = true, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 1, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = true, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = true, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 3, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = true, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = true, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 4, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 11, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 11, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 17, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 17, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 19, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 19, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 23, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 23, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 32, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 32, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 196, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = true);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
create type break;
create function breakin (cstring) returns break as 'textin' language internal;
create function breakout (break) returns cstring as 'textout' language internal;
create type break (input = breakin, output = breakout, internallength = 196, passedbyvalue = false, alignment = double);

create table alter_distpol_g (i int, j break, k text) with (appendonly = false);
insert into alter_distpol_g (i, k) select i, i from generate_series(1, 10) i;
alter table alter_distpol_g drop column j;
select * from alter_distpol_g order by 1;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;
drop type break cascade;
alter table alter_distpol_g set with(reorganize = true) distributed randomly;
select * from alter_distpol_g order by 1;

drop table alter_distpol_g;
