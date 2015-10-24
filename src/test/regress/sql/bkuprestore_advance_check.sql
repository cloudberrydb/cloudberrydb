\c regressbkuptestadv
select * from t1;
select * from v1;
insert into t1 values (2, 1, 3);
insert into t1 values (1, 5, 10);
insert into t1 values (100, NULL, 3);
insert into t1 values (20, 20, 30);
insert into t1 values (21, 22, 23);
select * from t1;
select * from v1;

select * from oids;
select oid from oids where 1 = 0;

select * from idxt;
insert into idxt (1, 1);
insert into idxt (1, 1);

select C3ToBit('OK');
select C3ToBit(NULL);
select 'ABC'::char(3)::bit(3);

select incr(100);

create function decr (i int) returns int as
$$ 
BEGIN
	return i-1;
END
$$ language plpgsql NO SQL;

select decr(100);

select * from tt1;
insert into tt1 values (10, 10, '1234');
insert into tt1 values (11, 11, '1234A');

select * from tt2;

select * from rt;
delete from rt;
select * from rt;
insert into rt values (100, 100);
select * from rt;

select nextval('seq');
select nextval('seq');

\c regressrestoretestadv1
select * from t1;
select * from v1;
insert into t1 values (2, 1, 3);
insert into t1 values (1, 5, 10);
insert into t1 values (100, NULL, 3);
insert into t1 values (20, 20, 30);
insert into t1 values (21, 22, 23);
select * from t1;
select * from v1;

select * from oids;
select oid from oids where 1 = 0;

select * from idxt;
insert into idxt (1, 1);
insert into idxt (1, 1);

select C3ToBit('OK');
select C3ToBit(NULL);
select 'ABC'::char(3)::bit(3);

select incr(100);

create function decr (i int) returns int as
$$ 
BEGIN
	return i-1;
END
$$ language plpgsql NO SQL

select decr(100);

select * from tt1;
insert into tt1 values (10, 10, '1234');
insert into tt1 values (11, 11, '1234A');

select * from tt2;

select * from rt;
delete from rt;
select * from rt;
insert into rt values (100, 100);
select * from rt;

select nextval('seq');
select nextval('seq');

\c regressrestoretestadv2
select * from t1;
select * from v1;
insert into t1 values (2, 1, 3);
insert into t1 values (1, 5, 10);
insert into t1 values (100, NULL, 3);
insert into t1 values (20, 20, 30);
insert into t1 values (21, 22, 23);
select * from t1;
select * from v1;

select * from oids;
select oid from oids where 1 = 0;

select * from idxt;
insert into idxt (1, 1);
insert into idxt (1, 1);

select C3ToBit('OK');
select C3ToBit(NULL);
select 'ABC'::char(3)::bit(3);

select incr(100);

create function decr (i int) returns int as
$$ 
BEGIN
	return i-1;
END
$$ language plpgsql NO SQL;

select decr(100);

select * from tt1;
insert into tt1 values (10, 10, '1234');
insert into tt1 values (11, 11, '1234A');

select * from tt2;

select * from rt;
delete from rt;
select * from rt;
insert into rt values (100, 100);
select * from rt;

select nextval('seq');
select nextval('seq');

\c regressbkuptestmisc
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
select relpages, reltuples from pg_class where relname = 't3';

\c regressrestoretestmisc1
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
select relpages, reltuples from pg_class where relname = 't3';

\c regressrestoretestmisc1
select * from t1;
select * from t2;
select * from t3;
select * from t4;
select * from t5;
select relpages, reltuples from pg_class where relname = 't3';

