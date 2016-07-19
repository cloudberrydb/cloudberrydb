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
