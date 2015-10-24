
-- Both of these queries should return zero rows,
-- eg, the original table should have the exact same contents as the one
-- we created via the binary export/import process
SELECT * FROM example
EXCEPT ALL
SELECT * FROM example_2;

SELECT * FROM example_2
EXCEPT ALL
SELECT * FROM example;

