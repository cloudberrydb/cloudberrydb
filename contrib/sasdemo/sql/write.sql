--
-- Example script that shows example of a "write" query from an external source
--
-- In this example data is moving from the external source into the database 
-- in parallel using the external table definitions from setup.sql.  From the
-- database's perspective this operation is a "read" from an external source,
-- as such it is modeled as a select statement from the external source;


DROP TABLE IF EXISTS example_2;
CREATE TABLE example_2 AS SELECT * FROM example_in DISTRIBUTED BY (id);

