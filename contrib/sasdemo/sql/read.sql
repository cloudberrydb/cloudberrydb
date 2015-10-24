--
-- Example script that shows example of a "read" query from an external source
--
-- In this example data is moving from the database out to the external source
-- in parallel using the external table definitions from setup.sql.  From the
-- database's perspective this operation is a "write" to an external source,
-- as such it is modeled as an insert into an external writable table.

INSERT INTO example_out SELECT * FROM example;


