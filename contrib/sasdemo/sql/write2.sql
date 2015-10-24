--
-- Here we show an example creating a new table from an external definition 
-- using a subset of the external table definition columns.
--
CREATE TEMPORARY TABLE example_staging AS 
  SELECT id, value1 FROM example_in
DISTRIBUTED BY (id) ;

-- Verify that we loaded the data, then drop the table, real world we might do 
-- something different.
SELECT * FROM example_staging;
DROP TABLE example_staging;

