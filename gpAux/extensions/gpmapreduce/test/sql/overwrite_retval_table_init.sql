create table simple(m int,n int) distributed randomly;
insert into simple values (1,10);
insert into simple values (2,20);
insert into simple values (2,21);
insert into simple values (2,22);
insert into simple values (3,30);
insert into simple values (4,40);
insert into simple values (5,50);
insert into simple values (5,50);
insert into simple values (10,100);
insert into simple values (2,21);

create or replace function tran (value int, value int, out ret integer) returns integer language 'C' as '$libdir/gpmrdemo', 'tran';

CREATE OR REPLACE FUNCTION retcomposite(IN integer)
RETURNS TABLE(x integer, y integer, z integer)
AS '$libdir/gpmrdemo', 'retcomposite'
LANGUAGE C IMMUTABLE STRICT;
