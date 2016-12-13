-- @author gcaragea
-- @created 2015-05-08 18:00:00
-- @tags ORCA bfv
-- @gpopt 1.577
-- @description bfv for MPP-25537

--start_ignore
DROP TABLE IF EXISTS mpp25537_facttable1;
DROP TABLE IF EXISTS mpp25537_dimdate;
DROP TABLE IF EXISTS mpp25537_dimtabl1;
--end_ignore

CREATE TABLE mpp25537_facttable1 (
col1 integer,
wk_id smallint,
id integer
)
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
partition by range (wk_id) (
start (1::smallint) END (20::smallint) inclusive every (1),
default partition dflt
)
;

insert into mpp25537_facttable1 select col1, col1, col1 from (select generate_series(1,20) col1)a;

CREATE TABLE mpp25537_dimdate (
wk_id smallint,
col2 date
)
;

insert into mpp25537_dimdate select col1, current_date - col1 from (select generate_series(1,20,2) col1)a;

CREATE TABLE mpp25537_dimtabl1 (
id integer,
col2 integer
)
;

insert into mpp25537_dimtabl1 select col1, col1 from (select generate_series(1,20,3) col1)a;

CREATE INDEX idx_mpp25537_facttable1 on mpp25537_facttable1 (id); 

set optimizer_analyze_root_partition to on;
set optimizer to on;

ANALYZE mpp25537_facttable1;
ANALYZE mpp25537_dimdate;
ANALYZE mpp25537_dimtabl1;

SELECT count(*) 
FROM mpp25537_facttable1 ft, mpp25537_dimdate dt, mpp25537_dimtabl1 dt1
WHERE ft.wk_id = dt.wk_id
AND ft.id = dt1.id;
