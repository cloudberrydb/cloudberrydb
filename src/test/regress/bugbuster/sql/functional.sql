\c regression
create language plpgsql;
-- start_ignore
drop table if exists decodeint; 
-- end_ignore

create table decodeint(a int, b int) 
distributed by (a);

insert into decodeint values (0,0);
insert into decodeint values (1,1);
insert into decodeint values (2,2);
insert into decodeint values (3,3);
insert into decodeint values (4,4);
insert into decodeint values (5,5);
insert into decodeint values (6,6);
insert into decodeint values (null,1);
insert into decodeint values (1,1);
insert into decodeint values (2,1);
insert into decodeint values (3,1);
insert into decodeint values (4,1);
insert into decodeint values (5,1);
insert into decodeint values (6,1);

select a, decode(a, 1, 'A', 2, 'B', 3, 'C', 4, 'D', 5, 'E') as decode from decodeint order by a, b;
-- start_ignore
drop table if exists decodeint; 
-- end_ignore

create table decodeint(a int, b int) 
distributed by (a);

insert into decodeint values (0,0);
insert into decodeint values (1,1);
insert into decodeint values (2,2);
insert into decodeint values (3,3);
insert into decodeint values (4,4);
insert into decodeint values (5,5);
insert into decodeint values (6,6);
insert into decodeint values (null,1);
insert into decodeint values (1,1);
insert into decodeint values (2,1);
insert into decodeint values (3,1);
insert into decodeint values (4,1);
insert into decodeint values (5,1);
insert into decodeint values (6,1);

select a, decode(a, 1, 'A', 2, 'B', 3, 'C', 4, 'D', 5, 'E', 'Z') as decode from decodeint order by a, b;

-- start_ignore
drop table if exists decodenum;
-- end_ignore

CREATE TABLE decodenum
(
numcol numeric(6, 3),
distcol int,
ptcol int,
name text
)
distributed by (distcol)
partition by range (ptcol)
(
default partition defaultpart,
start (0) inclusive
end (501) exclusive
every (100)
);

insert into decodenum values(1.1, 100, 0, 'part0');
insert into decodenum values(10.10, 100, 10, 'part1');
insert into decodenum values(10.10, 200, 200, 'part2');
insert into decodenum values(20.22, 200, 200, 'part2');
insert into decodenum values(20.22, 100, 100, 'part1');
insert into decodenum values(300.333, 300, 300, 'part3');
insert into decodenum values(300.333, 300, 100, 'part1');
insert into decodenum values(300.333, 300, 100, 'part1');

select numcol, decode(numcol, 300.333, '300+') as "decode(numcol, 300.333, '300+')" from decodenum order by numcol, distcol;

-- start_ignore
drop table if exists decodenum;
-- end_ignore

CREATE TABLE decodenum
(
numcol numeric(6, 3),
distcol int,
ptcol int,
name text
)
distributed by (distcol)
partition by range (ptcol)
(
default partition defaultpart,
start (0) inclusive
end (501) exclusive
every (100)
);

insert into decodenum values(1.1, 100, 0, 'part0');
insert into decodenum values(2.2, 200, 0, 'part0');
insert into decodenum values(10.10, 100, 10, 'part1');
insert into decodenum values(10.10, 200, 200, 'part2');
insert into decodenum values(20.22, 200, 200, 'part2');
insert into decodenum values(20.22, 100, 100, 'part1');
insert into decodenum values(100.311, 100, 100, 'part3');
insert into decodenum values(100.322, 100, 100, 'part1');
insert into decodenum values(100.333, 200, 200, 'part2');

select numcol, decode(numcol, 10.10, 'Under 100', 20.22, 'Under 100', 100.311, '100+', 100.322, '100+', 100.333, '100+', 'None') from decodenum order by numcol, distcol;
 
-- start_ignore
drop table if exists decodecharao;
-- end_ignore

CREATE TABLE decodecharao (country_code char(2), region text)
WITH (appendonly=true)
DISTRIBUTED BY (region);

insert into decodecharao values('US', 'Americas');
insert into decodecharao values('CA', 'Americas');
insert into decodecharao values('UK', 'Europe');
insert into decodecharao values ('FR', 'France');

select country_code, decode(country_code, 'CA', 'Canada') as decode from decodecharao order by country_code, region;

-- start_ignore
drop table if exists regions;
drop table if exists decodecharao;
-- end_ignore

CREATE TABLE regions(country_code char(2), region text) DISTRIBUTED BY (region);
  
CREATE TABLE decodecharao 
(country_code char(2), country_name varchar(255), region text)
WITH (appendonly=true)
DISTRIBUTED BY (region);

insert into regions(country_code, region) values('JP', 'Asia');
insert into regions(country_code, region) values('US', 'Americas');
insert into regions(country_code, region) values('CA', 'Americas');
insert into regions(country_code, region) values('FR', 'Europe');
insert into regions(country_code, region) values('UK', 'Europe');
insert into regions(country_code, region) values('IT', 'Europe');
 
insert into decodecharao(country_code, country_name, region)
select country_code, 
       decode(country_code, 'AU', 'Australia', 'BR', 'Brazil', 'CA', 'Canada', 'CH', 'China', 'FR', 'France', 'IT', 'Italy', 'JP', 'Japan', 'RU', 'Russia', 'SP', 'Spain', 'US', 'United States', 'United States'),
       region
from regions;

select *
from decodecharao
order by region, country_code;

-- start_ignore
drop table if exists decodevarchar;
-- end_ignore

create table decodevarchar
(
  dayname varchar(25),
  dayid int
) distributed by (dayid);

insert into decodevarchar values('Monday', 1);
insert into decodevarchar values('Tuesday', 2);
insert into decodevarchar values('Wednesday', 3);
insert into decodevarchar values('Thursday', 4);
insert into decodevarchar values('Friday', 5);
insert into decodevarchar values('Saturday', 6);
insert into decodevarchar values('Sunday', 7);

select dayname, 
       decode(dayname, 'Monday', true, 'Tuesday', true, 'Wednesday', true, 'Thursday', true, 'Friday', true, 'Saturday', false, 'Sunday', false) as is_workday 
from decodevarchar order by dayid;
-- start_ignore
drop table if exists decodevarchar;
-- end_ignore

create table decodevarchar
(
  dayname varchar(25),
  dayid int
) distributed by (dayid);

insert into decodevarchar values('Monday', 1);
insert into decodevarchar values('Tuesday', 2);
insert into decodevarchar values('Wednesday', 3);
insert into decodevarchar values('Thursday', 4);
insert into decodevarchar values('Friday', 5);
insert into decodevarchar values('Saturday', 6);
insert into decodevarchar values('Sunday', 7);

select dayname, 
       decode(dayname, 'Monday', true, 'Tuesday', true, 'Wednesday', true, 'Thursday', true, 'Friday', true, false) as is_workday 
from decodevarchar order by dayid;
-- start_ignore
drop table if exists employees;
drop table if exists emp_start_dates;
drop table if exists employees_2011;
-- end_ignore

CREATE TABLE employees
(
   empid integer,
   name text,
   gender char(1),
   PRIMARY KEY (empid, gender)
) distributed by (empid)
partition by list (gender)
(
partition women values ('F'),
partition men values ('M'),
default partition other
);

CREATE TABLE emp_start_dates
(
   empid integer,
   startdate date
) distributed by (startdate);

insert into employees(empid, name, gender) values(100, 'John Smith', 'M');
insert into employees(empid, name, gender) values(101, 'John Deere', 'M');
insert into employees(empid, name, gender) values(102, 'Jane Doe', 'F');
insert into employees(empid, name, gender) values(103, 'Janet Jackson', 'F');
insert into employees(empid, name, gender) values(104, 'Anne Smith', 'F');
insert into employees(empid, name, gender) values(105, 'Ryan Goesling', 'M');
insert into employees(empid, name, gender) values(106, 'George Clooney', 'M');
insert into employees(empid, name, gender) values(107, 'Julia Roberts', 'F');
insert into employees(empid, name, gender) values(108, 'Jennifer Aniston', 'F');
insert into employees(empid, name, gender) values(109, 'Brad Pitt', 'M');
 
insert into emp_start_dates(empid, startdate) values(100, '2011-01-01'::date);
insert into emp_start_dates(empid, startdate) values(101, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(102, '2011-02-28'::date);
insert into emp_start_dates(empid, startdate) values(103, '2009-02-01'::date);
insert into emp_start_dates(empid, startdate) values(104, '2011-03-15'::date);
insert into emp_start_dates(empid, startdate) values(105, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(106, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(107, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(108, '2011-02-15'::date);
insert into emp_start_dates(empid) values(109);

CREATE TABLE employees_2011 
as 
select e.empid as emp_id,
       e.name as emp_name,
       e.gender as gender,
       s.startdate as emp_start_dt,
       decode(s.startdate, '2011-01-01'::date, 1, '2011-01-15'::date, 1, '2011-01-31'::date, 1, '2011-02-01'::date, 2, '2011-02-15'::date, 2, '2011-02-28'::date, 2, '2011-03-01'::date, 3, '2011-03-15'::date, 3, '2011-03-31'::date, 3, '2011-04-01'::date, 4, '2011-04-15'::date, 4, '2011-04-30'::date, 4, '2011-05-01'::date, 5, '2011-05-15'::date, 5, '2011-05-31'::date, 5, '2011-06-01'::date, 6, '2011-06-15'::date, 6, '2011-06-30'::date, 6, 0) as start_mon_2011
from employees e
  join emp_start_dates s on (e.empid = s.empid)
DISTRIBUTED BY (start_mon_2011);


-- start_ignore
drop table if exists decodetimestamptz;
-- end_ignore

create table decodetimestamptz
(
    lastlogin  timestamptz,
    username   varchar(25),
    userid     int,
    decodetxt  text
) WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=1, blocksize=32768) distributed by (userid);

insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-19 10:30:54 PST'::timestamp with time zone, 'user1', 1000);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-15 11:20:14 PST'::timestamp with time zone, 'user2', 1010);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-19 07:25:15 PST'::timestamp with time zone, 'user3', 1020);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-19 08:20:54 PST'::timestamp with time zone, 'user4', 1030);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-15 11:20:14 PST'::timestamp with time zone, 'user5', 1040);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-19 10:30:54 PST'::timestamp with time zone, 'user6', 1050);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-19 10:30:54 PST'::timestamp with time zone, 'user7', 1060);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-15 11:20:14 PST'::timestamp with time zone, 'user8', 1070);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-12-19 10:30:54 PST'::timestamp with time zone, 'user9', 1080);
insert into decodetimestamptz(lastlogin, username, userid) values('2011-01-19 10:30:54 PST'::timestamp with time zone, 'user10', 1090);

select *
from decodetimestamptz
order by userid;

select lastlogin, decode(lastlogin, '2011-12-19 10:30:54 PST'::timestamp with time zone, 'December', '2011-12-15 11:20:14 PST'::timestamp with time zone, 'December', '2011-12-19 07:25:15 PST'::timestamp with time zone, 'December', '2011-12-19 08:20:54 PST'::timestamp with time zone, 'December', '2011-01-19 10:30:54 PST'::timestamp with time zone, 'January', '2011-02-19 10:30:54 PST'::timestamp with time zone, 'February') as decode 
from decodetimestamptz 
order by lastlogin, userid;

-- start_ignore
drop table if exists employees;
drop table if exists emp_start_dates;
-- end_ignore

CREATE TABLE employees
(
   empid integer,
   name text,
   gender char(1),
   PRIMARY KEY (empid, gender)
) distributed by (empid)
partition by list (gender)
(
partition women values ('F'),
partition men values ('M'),
default partition other
);

CREATE TABLE emp_start_dates
(
   empid integer,
   startdate date
) distributed by (startdate);

insert into employees(empid, name, gender) values(100, 'John Smith', 'M');
insert into employees(empid, name, gender) values(101, 'John Deere', 'M');
insert into employees(empid, name, gender) values(102, 'Jane Doe', 'F');
insert into employees(empid, name, gender) values(103, 'Janet Jackson', 'F');
insert into employees(empid, name, gender) values(104, 'Anne Smith', 'F');
insert into employees(empid, name, gender) values(105, 'Ryan Goesling', 'M');
insert into employees(empid, name, gender) values(106, 'George Clooney', 'M');
insert into employees(empid, name, gender) values(107, 'Julia Roberts', 'F');
insert into employees(empid, name, gender) values(108, 'Jennifer Aniston', 'F');
 
insert into emp_start_dates(empid, startdate) values(100, '2011-01-01'::date);
insert into emp_start_dates(empid, startdate) values(101, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(102, '2011-02-28'::date);
insert into emp_start_dates(empid, startdate) values(103, '2009-02-01'::date);
insert into emp_start_dates(empid, startdate) values(104, '2011-03-15'::date);
insert into emp_start_dates(empid, startdate) values(105, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(106, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(107, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(108, '2011-02-15'::date);

select e.empid as emp_id,
       e.name as emp_name,
       e.gender as gender,
       s.startdate as emp_start_dt,       
       decode((s.startdate >= '2011-01-01'::date), true, 'Y', 'N') as started_2011
from employees e
  join emp_start_dates s on (e.empid = s.empid)
order by s.startdate, e.empid asc
-- start_ignore
drop table if exists employees;
drop table if exists emp_start_dates;
-- end_ignore

CREATE TABLE employees
(
   empid integer,
   name text,
   gender char(1),
   PRIMARY KEY (empid, gender)
) distributed by (empid)
partition by list (gender)
(
partition women values ('F'),
partition men values ('M'),
default partition other
);

CREATE TABLE emp_start_dates
(
   empid integer,
   startdate date
) distributed by (startdate);

insert into employees(empid, name, gender) values(100, 'John Smith', 'M');
insert into employees(empid, name, gender) values(101, 'John Deere', 'M');
insert into employees(empid, name, gender) values(102, 'Jane Doe', 'F');
insert into employees(empid, name, gender) values(103, 'Janet Jackson', 'F');
insert into employees(empid, name, gender) values(104, 'Anne Smith', 'F');
insert into employees(empid, name, gender) values(105, 'Ryan Goesling', 'M');
insert into employees(empid, name, gender) values(106, 'George Clooney', 'M');
insert into employees(empid, name, gender) values(107, 'Julia Roberts', 'F');
insert into employees(empid, name, gender) values(108, 'Jennifer Aniston', 'F');
insert into employees(empid, name, gender) values(109, 'Null Startdate', 'M');
 
insert into emp_start_dates(empid, startdate) values(100, '2011-01-01'::date);
insert into emp_start_dates(empid, startdate) values(101, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(102, '2011-02-28'::date);
insert into emp_start_dates(empid, startdate) values(103, '2009-02-01'::date);
insert into emp_start_dates(empid, startdate) values(104, '2011-03-15'::date);
insert into emp_start_dates(empid, startdate) values(105, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(106, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(107, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(108, '2011-02-15'::date);
insert into emp_start_dates(empid, startdate) values(109, null);


-- start_ignore
drop table if exists emp;
drop table if exists dept;
-- end_ignore

CREATE TABLE emp
(
    empid integer PRIMARY KEY,
    deptno integer,
    fname varchar(100),
    lname varchar(200),
    salary numeric(10, 0)
) distributed by (empid);

CREATE TABLE dept
(
    deptno integer,
    name varchar(255)
);

create or replace function random_fname(length integer) returns text as 
$$
begin
  if length < 0 then
    raise exception 'length cannot be less than 0';
  end if;
  return array_to_string(array(select substr('JABICEDOIKEALOMENAPIROSTEII', trunc(random() * 61)::integer + 3, 1) from generate_series(1, length)), '');
end;
$$ language plpgsql CONTAINS SQL;

create or replace function random_lname(length integer) returns text as 
$$
begin
  if length < 0 then
    raise exception 'length cannot be less than 0';
  end if;
  return array_to_string(array(select substr('SMIGRAHITYSAOMBENAPICROSTEVIWHIZ', trunc(random() * 61)::integer + 3, 1) from generate_series(1, length)), '');
end;
$$ language plpgsql CONTAINS SQL;

create or replace function get_random_number(integer, integer) returns integer as 
$$
declare
    start_int ALIAS FOR $1;
    end_int ALIAS FOR $2;
begin
    return trunc(random() * (end_int-start_int) + start_int);
end;
$$ language 'plpgsql' strict NO SQL;

insert into emp(empid)
select generate_series(1000,11000);

update emp
set deptno = '100'
where (empid % 3) = 0;

update emp
set deptno = '200'
where (empid % 8) = 0;

update emp
set deptno = '300'
where (empid % 5) = 0;

update emp
set deptno = '400'
where (empid % 6) = 0;

update emp
set deptno = '500'
where deptno is null;

select deptno, count(empid) 
from emp
group by deptno;

create or replace function set_names()
returns integer as
$$
declare
    emprec record;
    rowcnt integer;
begin
    for emprec in select empid, deptno from emp 
    loop
    	execute 'update emp set fname = (random_fname(get_random_number(3,10))) where empid = ' || emprec.empid;
        execute 'update emp set lname = (random_lname(get_random_number(5,15))) where empid = ' || emprec.empid;
    end loop;
    return 1;
end;
$$ language plpgsql VOLATILE MODIFIES SQL DATA;


insert into dept
values(100, 'Finance');
insert into dept
values(200, 'Sales');
insert into dept
values(300, 'Marketing');
insert into dept
values(400, 'HR');
insert into dept
values(500, 'Engineering');

create or replace function set_salary()
returns integer as
$$
declare
    emprec record;
begin
    for emprec in select e.empid, d.name from emp e, dept d where e.deptno = d.deptno and d.name = 'Sales' order by empid asc
    loop
        execute 'update emp set salary = get_random_number(70000,300000) where empid = ' || emprec.empid;
    end loop;
    for emprec in select e.empid, d.name from emp e, dept d where e.deptno = d.deptno and d.name = 'Finance' order by empid asc
    loop
        execute 'update emp set salary = get_random_number(65000,200000) where empid = ' || emprec.empid;
    end loop;
    for emprec in select e.empid, d.name from emp e, dept d where e.deptno = d.deptno and d.name = 'Marketing' order by empid asc
    loop
        execute 'update emp set salary = get_random_number(85000,200000) where empid = ' || emprec.empid;
    end loop;
    for emprec in select e.empid, d.name from emp e, dept d where e.deptno = d.deptno and d.name = 'HR' order by empid asc
    loop
        execute 'update emp set salary = get_random_number(55000,150000) where empid = ' || emprec.empid;
    end loop;
    for emprec in select e.empid, d.name from emp e, dept d where e.deptno = d.deptno and d.name = 'Engineering' order by empid asc
    loop
        execute 'update emp set salary = get_random_number(65000,200000) where empid = ' || emprec.empid;
    end loop;

    return 1;
end;
$$ language plpgsql VOLATILE MODIFIES SQL DATA;


-- start_ignore
select count(*)
from emp
where fname is null;

select count(*)
from emp
where lname is null;
-- end_ignore

select count(*)
from emp
where salary is null;

select count(distinct empid), count(*)
from emp;

-- start_ignore
drop table if exists employees;
drop table if exists emp_start_dates;
-- end_ignore

CREATE TABLE employees
(
   empid integer,
   name text,
   gender char(1),
   PRIMARY KEY (empid, gender)
) distributed by (empid)
partition by list (gender)
(
partition women values ('F'),
partition men values ('M'),
default partition other
);

CREATE TABLE emp_start_dates
(
   empid integer,
   startdate date
) distributed by (startdate);

insert into employees(empid, name, gender) values(100, 'John Smith', 'M');
insert into employees(empid, name, gender) values(101, 'John Deere', 'M');
insert into employees(empid, name, gender) values(102, 'Jane Doe', 'F');
insert into employees(empid, name, gender) values(103, 'Janet Jackson', 'F');
insert into employees(empid, name, gender) values(104, 'Anne Smith', 'F');
insert into employees(empid, name, gender) values(105, 'Ryan Goesling', 'M');
insert into employees(empid, name, gender) values(106, 'George Clooney', 'M');
insert into employees(empid, name, gender) values(107, 'Julia Roberts', 'F');
insert into employees(empid, name, gender) values(108, 'Jennifer Aniston', 'F');
 
insert into emp_start_dates(empid, startdate) values(100, '2012-01-01'::date);
insert into emp_start_dates(empid, startdate) values(101, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(102, '2008-02-28'::date);
insert into emp_start_dates(empid, startdate) values(103, '2009-02-01'::date);
insert into emp_start_dates(empid, startdate) values(104, '2007-03-15'::date);
insert into emp_start_dates(empid, startdate) values(105, '2006-05-01'::date);
insert into emp_start_dates(empid, startdate) values(106, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(107, '2004-01-15'::date);
insert into emp_start_dates(empid, startdate) values(108, '2000-02-15'::date);

select e.empid as emp_id,
       e.name as emp_name,
       e.gender as gender,
       s.startdate as emp_start_dt,       
       decode(extract(year from s.startdate), 2012, 2012, 2011, 2011, 2010, 2010, 2009, 2009, 2008, 2008, 2007, 2007, 2006, 2006, 2005) as emp_start_yr
from employees e
  join emp_start_dates s on (e.empid = s.empid)
order by s.startdate, e.empid asc
-- start_ignore
drop table if exists office;
-- end_ignore

CREATE TABLE office
(
    locid integer,
    company_name varchar(100),
    address1 text,
    address2 text,
    city text,
    state varchar(3),
    zip varchar(9),
    country text,
    phone text
);

create or replace function get_area_code(phone text) returns text as
$$
declare
begin
  if (strpos(phone, '-') < 1) then
     raise exception 'Format for phone must be xxx-xxx-xxxx';
  end if; 
  return split_part(phone, '-', 1); 
end;
$$ language plpgsql NO SQL;

insert into office(locid, company_name, address1, address2, city, state, zip, country, phone)
values(100, 'Greenplum', '1900 S. Norfolk St.', 'Suite 125', 'San Mateo', 'CA', '94403', 'USA', '650-111-1111');
insert into office(locid, company_name, address1, address2, city, state, zip, country, phone)
values(200, 'Dairy Farm', '100 Meadow Lane', null, 'Stockton', 'CA', '95210', 'USA', '209-222-2000');
insert into office(locid, company_name, address1, address2, city, state, zip, country, phone)
values(300, 'Not Just a Database', '1900 S. Norfolk St.', 'Suite 224', 'San Mateo', 'CA', '94403', 'USA', '650-111-3000');
insert into office(locid, company_name, address1, address2, city, state, zip, country, phone)
values(400, 'EMC Corporation', '176 South Street', null, 'Santa Clara', 'CA', '95123', 'USA', '408-444-4000');
insert into office(locid, company_name, address1, address2, city, state, zip, country, phone)
values(500, 'YTR Homes', '1316 James Ct.', 'Suite 55', 'San Mateo', 'CA', '94401', 'USA', '650-500-5555');
insert into office(locid, company_name, address1, address2, city, state, zip, country, phone)
values(600, 'Taing Consulting', '123 Homestead Ave.', 'Suite 123', 'Santa Clara', 'CA', '95051', 'USA', '408-600-6000');
insert into office(locid, company_name, address1, address2, city, state, zip, country, phone)
values(700, 'Dance Central', '500 Jazz Circle', null, 'San Jose', 'CA', '95210', 'USA', '408-777-7000');

select locid, company_name, phone
from office
order by locid, phone;

select locid, 
       company_name,
       phone,
       decode('408', get_area_code(phone), 'Y') as south_bay,
       decode('650', get_area_code(phone), 'Y', 'N') as peninsula,
       decode('415', get_area_code(phone), 'Y', 'N') as sf
from office
order by locid, phone;

-- start_ignore
drop schema if exists test_functions cascade;
-- end_ignore 
create schema test_functions;
alter function get_area_code(text) set schema test_functions;

select locid,
       company_name,
       phone,
       decode('408', test_functions.get_area_code(phone), 'Y') as south_bay,
       decode('650', test_functions.get_area_code(phone), 'Y', 'N') as peninsula,
       decode('415', test_functions.get_area_code(phone), 'Y', 'N') as sf
from office
order by locid, phone;

-- start_ignore
drop table if exists employees;
drop table if exists emp_start_dates;
-- end_ignore

CREATE TABLE employees
(
   empid integer,
   name text,
   gender char(1),
   PRIMARY KEY (empid, gender)
) distributed by (empid)
partition by list (gender)
(
partition women values ('F'),
partition men values ('M'),
default partition other
);

CREATE TABLE emp_start_dates
(
   empid integer,
   startdate date
) distributed by (startdate);

insert into employees(empid, name, gender) values(100, 'John Smith', 'M');
insert into employees(empid, name, gender) values(101, 'John Deere', 'M');
insert into employees(empid, name, gender) values(102, 'Jane Doe', 'F');
insert into employees(empid, name, gender) values(103, 'Janet Jackson', 'F');
insert into employees(empid, name, gender) values(104, 'Anne Smith', 'F');
insert into employees(empid, name, gender) values(105, 'Ryan Goesling', 'M');
insert into employees(empid, name, gender) values(106, 'George Clooney', 'M');
insert into employees(empid, name, gender) values(107, 'Julia Roberts', 'F');
insert into employees(empid, name, gender) values(108, 'Jennifer Aniston', 'F');
insert into employees(empid, name, gender) values(109, 'John Mayer', 'M');
 
insert into emp_start_dates(empid, startdate) values(100, '2012-01-01'::date);
insert into emp_start_dates(empid, startdate) values(101, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(102, '2008-02-28'::date);
insert into emp_start_dates(empid, startdate) values(103, '2009-02-01'::date);
insert into emp_start_dates(empid, startdate) values(104, '2007-03-15'::date);
insert into emp_start_dates(empid, startdate) values(105, '2006-05-01'::date);
insert into emp_start_dates(empid, startdate) values(106, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(107, '2004-01-15'::date);
insert into emp_start_dates(empid, startdate) values(108, '2000-02-15'::date);
insert into emp_start_dates(empid, startdate) values(109, null);

select e.empid as emp_id,
       e.name as emp_name,
       e.gender as gender,
       s.startdate as emp_start_dt,       
       decode(extract(year from s.startdate), 2012, coalesce(extract(year from s.startdate), '2012'), 2011, coalesce(extract(year from s.startdate), '2011'), 2010, coalesce(extract(year from s.startdate), '2010'), 2009, coalesce(extract(year from s.startdate), '2009'), 2008, coalesce(extract(year from s.startdate), '2008'), 2007, coalesce(extract(year from s.startdate), '2007'), 2006, coalesce(extract(year from s.startdate), '2006'), null, coalesce(extract(year from s.startdate), '0'), 2005) as emp_start_yr
from employees e
  join emp_start_dates s on (e.empid = s.empid)
order by s.startdate, e.empid asc
-- start_ignore
drop table if exists employees;
drop table if exists emp_start_dates;
-- end_ignore

CREATE TABLE employees
(
   empid integer,
   name text,
   gender char(1),
   PRIMARY KEY (empid, gender)
) distributed by (empid)
partition by list (gender)
(
partition women values ('F'),
partition men values ('M'),
default partition other
);

CREATE TABLE emp_start_dates
(
   empid integer,
   startdate date
) distributed by (startdate);

insert into employees(empid, name, gender) values(100, 'John Smith', 'M');
insert into employees(empid, name, gender) values(101, 'John Deere', 'M');
insert into employees(empid, name, gender) values(102, 'Jane Doe', 'F');
insert into employees(empid, name, gender) values(103, 'Janet Jackson', 'F');
insert into employees(empid, name, gender) values(104, 'Anne Smith', 'F');
insert into employees(empid, name, gender) values(105, 'Ryan Goesling', 'M');
insert into employees(empid, name, gender) values(106, 'George Clooney', 'M');
insert into employees(empid, name, gender) values(107, 'Julia Roberts', 'F');
insert into employees(empid, name, gender) values(108, 'Jennifer Aniston', 'F');
insert into employees(empid, name, gender) values(109, 'John Mayer', 'M');
 
insert into emp_start_dates(empid, startdate) values(100, '2012-01-01'::date);
insert into emp_start_dates(empid, startdate) values(101, '2010-01-15'::date);
insert into emp_start_dates(empid, startdate) values(102, '2008-02-28'::date);
insert into emp_start_dates(empid, startdate) values(103, '2009-02-01'::date);
insert into emp_start_dates(empid, startdate) values(104, '2007-03-15'::date);
insert into emp_start_dates(empid, startdate) values(105, '2006-05-01'::date);
insert into emp_start_dates(empid, startdate) values(106, '2011-05-01'::date);
insert into emp_start_dates(empid, startdate) values(107, '2004-01-15'::date);
insert into emp_start_dates(empid, startdate) values(108, '2000-02-15'::date);
insert into emp_start_dates(empid, startdate) values(109, null);

select e.empid as emp_id,
       e.name as emp_name,
       e.gender as gender,
       s.startdate as emp_start_dt,       
       decode(extract(year from s.startdate), 2012, 2012, 2011, 2011, 2010, 2010, 2009, 2009, 2008, 2008, 2007, 2007, 2006, 2006, null, 0, coalesce(extract(year from '2005-01-01'::date), 2005)) as emp_start_yr
from employees e
  join emp_start_dates s on (e.empid = s.empid)
order by s.startdate, e.empid asc;

-- start_ignore
drop table if exists vtable;
-- end_ignore

create table vtable(a int) distributed by (a);

create or replace function test_volatile(in_val integer)
returns integer as
$$
declare
    rowcnt integer;
begin
    execute 'insert into vtable values(' || in_val || ')';
    select count(*) from vtable into rowcnt;
    return rowcnt;
end;
$$ language plpgsql VOLATILE MODIFIES SQL DATA;

select decode(test_volatile(100), 1, 'PASS', 2, 'FAIL', 3, 'FAIL', 4, 'FAIL', 5, 'FAIL', 6, 'FAIL') as decode;

truncate table vtable;
select decode(2, test_volatile(100), 'FAIL', test_volatile(200), 'PASS', test_volatile(300), 'FAIL', 'FAIL') as decode;

select * 
from vtable
order by a;
select decode(null, 1, 'null = 1', 'null != 1');

select decode(1, null, '1 = null', '1 != null');

select decode(null, null, 'null = null', 'null != null');

select decode(10, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, null, 1);

select decode(11, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, null);
select decode(true);

select decode(true, false);

select decode(2);

select decode('2/14/2011'::date);
--start_ignore
drop table if exists genders;
-- end_ignore

CREATE TABLE genders (gender char(1), student_id integer)
WITH (appendonly=true)
DISTRIBUTED BY (student_id);

insert into genders values('M', 11111);
insert into genders values('M', 12222);
insert into genders values('F', 22222);
insert into genders values('F', 33333);
insert into genders values('F', 44444);
insert into genders values('M', 55555);
insert into genders values('F', 55555);
insert into genders values('M', 66666);

select gender, decode(gender, 1, 'Unknown', 'M', 'Male', 'F', 'Female', 'Unknown')
from genders
order by gender,student_id;

select gender,decode(gender, 'N/A', 'Unknown', 'M', 'Male', 'F', 'Female')
from genders
order by gender,student_id;



--start_ignore
drop table if exists genders;
-- end_ignore

CREATE TABLE genders (gender char(1), student_id integer)
WITH (appendonly=true)
DISTRIBUTED BY (student_id);

insert into genders values('M', 11111);
insert into genders values('M', 12222);
insert into genders values('F', 22222);
insert into genders values('F', 33333);
insert into genders values('F', 44444);
insert into genders values('M', 55555);
insert into genders values('F', 55555);
insert into genders values('M', 66666);

select gender, decode(gender, 'Z', 'Unknown', 1)
from genders
order by student_id;

select gender, decode(gender, 'Z', 0,  'M')
from genders
order by student_id;

select gender, decode(gender, 'Z', 0, 'M', 'Male', 'F')
from genders
order by student_id;

select decode('2011-01-05'::date, '2011-01-01'::date, 2011, '2010-12-30'::date, 2010, '2012'); 

select decode('2011-01-05'::date, '2011-01-01'::date, '2011-01-01'::date, '2010-12-30'::date, '2010-01-01'::date, '2012'); 

select decode('2011-01-05'::date, '2011-01-01'::date, '2011-01-01'::date, '2010-12-30'::date, '2010-01-01'::date, 2012); 
-- start_ignore
drop table if exists decodeint;
--end_ignore

create table decodeint(a int, b int)
distributed by (a);

insert into decodeint values (0,0);
insert into decodeint values (1,1);
insert into decodeint values (2,2);
insert into decodeint values (3,3);
insert into decodeint values (4,4);
insert into decodeint values (5,5);
insert into decodeint values (6,6);
insert into decodeint values (null,1);
insert into decodeint values (1,1);
insert into decodeint values (2,1);
insert into decodeint values (3,1);
insert into decodeint values (4,1);
insert into decodeint values (5,1);
insert into decodeint values (6,1);

select a, decode(a, 10, 'J', 11, 'K', 12, 'L', 13, 'M', 14, 'N', 15, 'O', 16, 'P') as decode_nomatch 
from decodeint order by a, b;

select a, decode(a, 10, 'J', 11, 'K', 12, 'L', 13, 'M', 14, 'N', 15, 'O', 16, 'P', 'Z') as decode_nomatch_def 
from decodeint order by a, b;
-- start_ignore
drop table if exists validt;
-- end_ignore

create table decode (a int, b int) distributed by (b);

create database DECODE;

create table testdecode (a int, decode int)
distributed by (a);

create table invalid
(
  empid integer CONSTRAINT decode UNIQUE,
  name text,
  gender char(1)
) distributed by (empid);


create table validt
(
    record_id  integer,
    name text,
    pub_date date
) distributed by (record_id);

create unique index decode ON validt(record_id);

create table invalidpt
(
  empid integer,
  name text,
  gender char(1)
) 
distributed by (empid)
partition by list (gender)
(
   partition decode values ('F'),
   partition men values ('M'),
   default partition other
);

create or replace function sales_region(country_code char(2)) returns text as
$$
begin
  return decode(country_code, 'US', 'Americas', 'CA', 'Americas', 'MX', 'Americas', 
                'CN', 'APJ', 'JP', 'APJ', 'RU', 'APJ', 'UK', 'EMEA', 'FR', 'EMEA', 
               'IL', 'EMEA', 'IT', 'EMEA', 'IE', 'EMEA'); 
end;
$$ language plpgsql CONTAINS SQL;

select 'FR' as country_code, sales_region('FR');

select  'PR' as country_code, sales_region('PR');

drop schema if exists test_functions cascade;
create schema test_functions;
alter function sales_region(char(2)) set schema test_functions;

select 'FR' as country_code, test_functions.sales_region('FR');

select 'PR' as country_code, test_functions.sales_region('PR');

-- start_ignore
drop table if exists locations cascade;
-- end_ignore

create table locations
(
   locid integer,
   bus_name text,
   country_code char(2)
);

insert into locations(locid, bus_name, country_code)
values(1000, 'Test Solutions', 'CN');
insert into locations(locid, bus_name, country_code)
values(1011, 'Taing Consulting', 'US');
insert into locations(locid, bus_name, country_code)
values(2000, 'Parts Plus', 'IT');
insert into locations(locid, bus_name, country_code)
values(1055, 'Computers Unlimited', 'IL');
insert into locations(locid, bus_name, country_code)
values(2005, 'Kangaroos Inc.', 'AU');

select *
from locations
order by locid, country_code;

create or replace view decode_view 
as
select bus_name, 
       country_code,
       decode(country_code, 'US', 'Americas', 'CA', 'Americas', 'MX', 'Americas',
                'CN', 'APJ', 'JP', 'APJ', 'RU', 'APJ', 'UK', 'EMEA', 'FR', 'EMEA',
               'IL', 'EMEA', 'IT', 'EMEA', 'IE', 'EMEA') as region
from locations
order by locid, country_code;

\d+ decode_view;

select *
from decode_view
order by region, country_code;
-- start_ignore
drop table if exists office;
-- end_ignore

create table office 
(
   locid integer,
   bus_name text,
   country_code char(2)
);

insert into office(locid, bus_name, country_code)
values(1000, 'Test Solutions', 'CN');
insert into office(locid, bus_name, country_code)
values(1011, 'Taing Consulting', 'US');
insert into office(locid, bus_name, country_code)
values(2000, 'Parts Plus', 'IT');
insert into office(locid, bus_name, country_code)
values(1055, 'Computers Unlimited', 'IL');
insert into office(locid, bus_name, country_code)
values(2005, 'Kangaroos Inc.', 'AU');

select locid, 
       bus_name, 
       country_code
from office
where  decode(country_code, 'US', 'Americas', 'CA', 'Americas', 'MX', 'Americas',
                'CN', 'APJ', 'JP', 'APJ', 'RU', 'APJ', 'UK', 'EMEA', 'FR', 'EMEA',
               'IL', 'EMEA', 'IT', 'EMEA', 'IE', 'EMEA') = 'EMEA'
order by locid, country_code;

select "decode"('MTIzAAE=', 'base64');

select "decode"(md5('password'),'hex');
-- start_ignore
drop table if exists sales;
-- end_ignore

create table sales
(ytd_sales decimal, 
 currency char(1)
) distributed by (currency);

insert into sales values(2000000, '€');
insert into sales values(10500000.25, '$');
insert into sales values(789100.50, '¥');
insert into sales values(300685, '£');

select *
from sales
order by ytd_sales desc;

select currency, decode(currency, '$', 'USD', '€', 'EUR', '¥', 'JPY', 'USD') from sales order by ytd_sales desc;  
select currency, decode(currency, '$', 'USD', '€', 'EUR', '¥', 'JPY', '£', 'GBP', 'USD') from sales order by ytd_sales desc;  
