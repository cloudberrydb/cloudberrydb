CREATE TABLE outer_table (
    flex_value_id numeric(15,0) ,
    language character varying(4) ,
    last_update_date date ,
    last_updated_by numeric(15,0) ,
    creation_date date ,
    created_by numeric(15,0) ,
    last_update_login numeric(10,0) ,
    description character varying(240) ,
    source_lang character varying(4) ,
    flex_value_meaning character varying(150) ,
    legalhold character varying(5) ,
    legaldescription character varying(255) ,
    dataowner character varying(255) ,
    purgedate date 
)
WITH (appendonly=true, orientation=column, compresstype=none) DISTRIBUTED BY (flex_value_id);


set allow_system_table_mods="DML";
UPDATE pg_class
SET
	relpages = 1::int,
	reltuples = 0.0::real
WHERE relname = 'outer_table' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'ofa_applsys');
UPDATE pg_class
SET
	relpages = 64::int,
	reltuples = 61988.0::real
WHERE relname = 'outer_table' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'applsys');

create table inner_table
	(
	flex_value_set_id numeric(10,0) ,
    flex_value_id numeric(15,0) ,
    flex_value character varying(150) ,
	flex_partition int
) 
WITH (appendonly=true, orientation=column, compresstype=none) distributed by (flex_value_set_id ,flex_value_id) partition by range(flex_partition) ( start(0) end(10) every (2), default partition other);

CREATE INDEX inner_table_idx ON inner_table USING bitmap (flex_value_id);
UPDATE pg_class
SET
	relpages = 64::int,
	reltuples = 30994.0::real
WHERE relname = 'inner_table' AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'applsys');


insert into outer_table values(12345,'ZHS');
insert into outer_table values(012345,'ZHS');
insert into inner_table values(54321,12345, 'TOM', 5);
insert into inner_table values(654321,123456, 'JERRY', 20);

CREATE TABLE foo(fid numeric(10,0)) distributed by (fid);
insert into foo VALUES(54321);

set optimizer=on;
set optimizer_segments=64;
set optimizer_enable_bitmapscan=on; 
select disable_xform('CXformInnerJoin2HashJoin');
select disable_xform('CXformInnerJoin2IndexGetApply');

SELECT fid FROM
(SELECT b1.flex_value_set_id, b1.flex_value_id
FROM
inner_table b1,
outer_table c1
WHERE
b1.flex_value_id = c1.flex_value_id 
and c1.language = 'ZHS') bar,
foo
WHERE foo.fid = bar.flex_value_set_id;
