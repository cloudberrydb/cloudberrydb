CREATE DOMAIN country_code char(2) NOT NULL;
ALTER DOMAIN country_code DROP NOT NULL;



DROP DOMAIN country_code;

CREATE FUNCTION addlab1111(integer,integer) RETURNS integer
AS 'select $1 + $2;'
LANGUAGE SQL 
IMMUTABLE
CONTAINS SQL
RETURNS NULL ON NULL INPUT;

ALTER FUNCTION addlab1111(integer,integer) RENAME TO addition111;


DROP FUNCTION addition111(integer,integer);

CREATE USER jonathan11 WITH PASSWORD 'abc1';
CREATE USER jonathan12 WITH PASSWORD 'abc2';

ALTER USER jonathan11 RENAME TO jona11;
ALTER USER jonathan12 RENAME TO jona12;

DROP USER jona11;
DROP USER jona12;

CREATE USER jonathan12 WITH PASSWORD 'abc2';

ALTER USER jonathan11 RENAME TO jona11;
ALTER USER jonathan12 RENAME TO jona12;

CREATE GROUP marketing WITH USER jona11,jona12;

ALTER GROUP marketing RENAME TO market;

DROP GROUP market;
DROP USER jona11;
DROP USER jona12;

select unnest('{}'::text[]);

drop table mpp_r6756 cascade; --ignore
drop table mpp_s6756 cascade; --ignore

create table mpp_r6756 ( a int, b int, x int, y int ) 
    distributed randomly
    partition by list(a) (
        values (0), 
        values (1)
        );

create table mpp_s6756 ( c int, d int, e int ) 
    distributed randomly;

insert into mpp_s6756 values 
    (0,0,0),(0,0,1),(0,1,0),(0,1,1),(1,0,0),(1,0,1),(1,1,0),(1,1,1);

insert into mpp_r6756 values
    (0, 0, 1, 1),
    (0, 1, 2, 2),
    (0, 1, 2, 2),
    (1, 0, 3, 3),
    (1, 0, 3, 3),
    (1, 0, 3, 3),
    (1, 1, 4, 4),
    (1, 1, 4, 4),
    (1, 1, 4, 4),
    (1, 1, 4, 4);

-- start_equiv
select a, b, count(distinct x), count(distinct y)
from mpp_r6756 r, mpp_s6756 c, mpp_s6756 d, mpp_s6756 e
where r.a = c.c and r.a = d.d and r.a = e.e
group by a, b
union all
select a, null, count(distinct x), count(distinct y)
from mpp_r6756 r, mpp_s6756 c, mpp_s6756 d, mpp_s6756 e
where r.a = c.c and r.a = d.d and r.a = e.e
group by a
union all
select null, null, count(distinct x), count(distinct y)
from mpp_r6756 r, mpp_s6756 c, mpp_s6756 d, mpp_s6756 e
where r.a = c.c and r.a = d.d and r.a = e.e;

select a, b, count(distinct x), count(distinct y)
from mpp_r6756 r, mpp_s6756 c, mpp_s6756 d, mpp_s6756 e
where r.a = c.c and r.a = d.d and r.a = e.e
group by rollup (a,b);
--end_equiv

drop table mpp_r6756 cascade; --ignore
drop table mpp_s6756 cascade; --ignore

--start_ignore
drop table if exists copytest;
--end_ignore
