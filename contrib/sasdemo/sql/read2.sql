-- Like read.sql, but this file inserts only a subset of the columns

-- It is not necessary to read all of the columns
--    * non accessed columns will be passed as null
INSERT INTO example_out(id, value1)  SELECT id, value1 FROM example;