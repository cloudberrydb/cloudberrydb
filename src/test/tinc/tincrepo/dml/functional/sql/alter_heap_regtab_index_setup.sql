-- @author prabhd  
-- @created 2012-12-05 12:00:00  
-- @modified 2012-12-05 12:00:00 
-- @tags dml 
-- @db_name dmldb
\echo --start_ignore
set gp_enable_column_oriented_table=on;
\echo --end_ignore
DROP TABLE IF EXISTS dml_tab;
CREATE TABLE dml_tab (
        i int,
        x text,
        c char,
        v varchar,
        d date,
        n numeric,
        t timestamp without time zone,
        tz time with time zone
) distributed by (c);
INSERT INTO dml_tab VALUES(generate_series(1,10),'dml_tab','a','alter operations','01-01-2013',1,'01-01-2013 10:10:00', '01-01-2013 10:10:54+02');

