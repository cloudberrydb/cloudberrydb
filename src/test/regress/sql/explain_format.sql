-- start_matchsubs
-- m/\(actual time=\d+.\d+..\d+.\d+ rows=\d+ loops=\d+\)/
-- s/\(actual time=\d+.\d+..\d+.\d+ rows=\d+ loops=\d+\)/(actual time=10.302..10.302 rows=0 loops=1)/
-- m/\(slice\d+\)    Executor memory: (\d+)\w bytes\./
-- s/\(slice\d+\)    Executor memory: (\d+)\w bytes\./\(slice\)    Executor memory: (#####)K bytes./
-- m/\(slice\d+\)    Executor memory: (\d+)\w bytes avg x \d+ workers, \d+\w bytes max \(seg\d+\)\./
-- s/\(slice\d+\)    Executor memory: (\d+)\w bytes avg x \d+ workers, \d+\w bytes max \(seg\d+\)\./\(slice\)    Executor memory: ####K bytes avg x #### workers, ####K bytes max (seg0)./
-- m/Total runtime: \d+\.\d+ ms/
-- s/Total runtime: \d+\.\d+ ms/Total runtime: ##.### ms/
-- m/cost=\d+\.\d+\.\.\d+\.\d+ rows=\d+ width=\d+/
-- s/\(cost=\d+\.\d+\.\.\d+\.\d+ rows=\d+ width=\d+\)/(cost=##.###..##.### rows=### width=###)/
-- end_matchsubs
--
-- DEFAULT syntax
CREATE TABLE apples(id int PRIMARY KEY, type text);
CREATE TABLE locations(id int PRIMARY KEY, address text);
CREATE TABLE boxes(id int PRIMARY KEY, apple_id int REFERENCES apples(id), location_id int REFERENCES locations(id));

--- Check Explain Text output
EXPLAIN SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;

--- Check Explain Analyze Text output
-- explain_processing_off
EXPLAIN (ANALYZE) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;
-- explain_processing_on

