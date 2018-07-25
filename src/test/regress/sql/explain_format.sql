-- start_matchsubs
-- m/\(actual time=\d+\.\d+..\d+\.\d+ rows=\d+ loops=\d+\)/
-- s/\(actual time=\d+\.\d+..\d+\.\d+ rows=\d+ loops=\d+\)/(actual time=##.###..##.### rows=# loops=#)/
-- m/\(slice\d+\)    Executor memory: (\d+)\w bytes\./
-- s/\(slice\d+\)    Executor memory: (\d+)\w bytes\./\(slice\)    Executor memory: (#####)K bytes./
-- m/\(slice\d+\)    Executor memory: (\d+)\w bytes avg x \d+ workers, \d+\w bytes max \(seg\d+\)\./
-- s/\(slice\d+\)    Executor memory: (\d+)\w bytes avg x \d+ workers, \d+\w bytes max \(seg\d+\)\./\(slice\)    Executor memory: ####K bytes avg x #### workers, ####K bytes max (seg#)./
-- m/Work_mem: \d+\w bytes max\./
-- s/Work_mem: \d+\w bytes max\. */Work_mem: ###K bytes max./
-- m/Total runtime: \d+\.\d+ ms/
-- s/Total runtime: \d+\.\d+ ms/Total runtime: ##.### ms/
-- m/cost=\d+\.\d+\.\.\d+\.\d+ rows=\d+ width=\d+/
-- s/\(cost=\d+\.\d+\.\.\d+\.\d+ rows=\d+ width=\d+\)/(cost=##.###..##.### rows=### width=###)/
-- end_matchsubs
--
-- DEFAULT syntax
CREATE TABLE apples(id int PRIMARY KEY, type text);
INSERT INTO apples(id) SELECT generate_series(1, 100000);
CREATE TABLE locations(id int PRIMARY KEY, address text);
CREATE TABLE boxes(id int PRIMARY KEY, apple_id int REFERENCES apples(id), location_id int REFERENCES locations(id));

--- Check Explain Text format output
-- explain_processing_off
EXPLAIN SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;
-- explain_processing_on

--- Check Explain Analyze Text output that include the slices information
-- explain_processing_off
EXPLAIN (ANALYZE) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;
-- explain_processing_on

-- YAML Required replaces for costs and time changes
-- start_matchsubs
-- m/ Loops: \d+ /
-- s/ Loops: \d+\s+/ Loops: # /
-- m/ Cost: \d+\.\d+ /
-- s/ Cost: \d+\.\d+\s+/ Cost: ###.## /
-- m/ Rows: \d+ /
-- s/ Rows: \d+\s+/ Rows: ##### /
-- m/ Plan Width: \d+ /
-- s/ Plan Width: \d+\s+/ Plan Width: ## /
-- m/ Time: \d+\.\d+ /
-- s/ Time: \d+\.\d+\s+/ Time: ##.### /
-- m/Total Runtime: \d+\.\d+/
-- s/Total Runtime: \d+\.\d+/Total Runtime: ##.###/
-- m/Segments: \d+\s+/
-- s/Segments: \d+\s+/Segments: #/
-- m/PQO version \d+\.\d+\.\d+"\s+/
-- s/PQO version \d+\.\d+\.\d+"\s+/PQO version ##.##.##"/
-- m/Slice: [0-3]\s+/
-- s/Slice: [0-3]\s+/Slice: # /
-- m/Executor Memory: \d+\s+/
-- s/Executor Memory: \d+\s+/Executor Memory: ### /
-- m/Maximum Memory Used: \d+\s+/
-- s/Maximum Memory Used: \d+\s+/Maximum Memory Used: ### /
-- m/Work Maximum Memory: \d+\s+/
-- s/Work Maximum Memory: \d+\s+/Work Maximum Memory: ### /
-- end_matchsubs
-- Check Explain YAML output
EXPLAIN (FORMAT YAML) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;

--- Check Explain Analyze YAML output that include the slices information
-- explain_processing_off
EXPLAIN (ANALYZE, FORMAT YAML) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;
-- explain_processing_on

-- JSON Required replaces for costs and time changes
-- start_matchsubs
-- m/ Loops": \d+,?\s+/
-- s/ Loops": \d+,?\s+/ Loops": #, /
-- m/ Cost": \d+\.\d+, /
-- s/ Cost": \d+\.\d+,\s+/ Cost": ###.##, /
-- m/ Rows": \d+, /
-- s/ Rows": \d+,\s+/ Rows": #####, /
-- m/"Plan Width": \d+,? /
-- s/"Plan Width": \d+,?\s+/"Plan Width": ##, /
-- m/ Time": \d+\.\d+, /
-- s/ Time": \d+\.\d+,\s+/ Time": ##.###, /
-- m/Total Runtime": \d+\.\d+,?\s+/
-- s/Total Runtime": \d+\.\d+,?\s+/Total Runtime": ##.###,/
-- m/"Memory used": \d+,?\s+/
-- s/"Memory used": \d+,?\s+/"Memory used": ####,/
-- m/"Segments": \d+,\s+/
-- s/"Segments": \d+,\s+/"Segments": #,/
-- m/"Slice": [0-3],\s+/
-- s/"Slice": [0-3],\s+/"Slice": #, /
-- m/Executor Memory": \d+,?\s+/
-- s/Executor Memory": \d+,?\s+/Executor Memory": ### /
-- m/"Maximum Memory Used": \d+,?\s+/
-- s/"Maximum Memory Used": \d+,?\s+/"Maximum Memory Used": ###, /
-- m/"Work Maximum Memory": \d+,?\s+/
-- s/"Work Maximum Memory": \d+,?\s+/"Work Maximum Memory": ### /
-- m/"Workers": \d+,\s+/
-- s/"Workers": \d+,\s+/"Workers": ##, /
-- end_matchsubs
-- explain_processing_off
-- Check Explain JSON output
EXPLAIN (FORMAT JSON) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;
-- explain_processing_on

--- Check Explain Analyze JSON output that include the slices information
-- explain_processing_off
EXPLAIN (ANALYZE, FORMAT JSON) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;
-- explain_processing_on

-- XML Required replaces for costs and time changes
-- start_matchsubs
-- m/Cost>\d+\.\d+<\/[^>]+\-Cost>\s+/
-- s/Cost>\d+\.\d+<\/([^>]+)\-Cost>\s+/Cost>###.##<\/$1-Cost>/
-- m/Plan-Rows>\d+<\/Plan-Rows>\s+/
-- s/Plan-Rows>\d+<\/Plan-Rows>\s+/Plan-Rows>###<\/Plan-Rows>/
-- m/Plan-Width>\d+<\/Plan-Width>\s+/
-- s/Plan-Width>\d+<\/Plan-Width>\s+/Plan-Width>###<\/Plan-Width>/
-- m/Segments>\d+<\/Segments>\s+/
-- s/Segments>\d+<\/Segments>\s+/Segments>##<\/Segments>/
-- m/Actual-Rows>\d+<\/Actual-Rows>\s+/
-- s/Actual-Rows>\d+<\/Actual-Rows>\s+/Actual-Rows>###<\/Actual-Rows>/
-- m/Actual-Loops>\d+<\/Actual-Loops>\s+/
-- s/Actual-Loops>\d+<\/Actual-Loops>\s+/Actual-Loops>###<\/Actual-Loops>/
-- m/-Time>\d+\.\d+<\/[^>]+\-Time>\s*/
-- s/-Time>\d+\.\d+<\/([^>]+)\-Time>\s*/-Time>##.###<\/$1-Time>/
-- m/Total-Runtime>\d+\.\d+<\/Total-Runtime>\s+/
-- s/Total-Runtime>\d+\.\d+<\/Total-Runtime>\s+/Total-Runtime>##.###<\/Total-Runtime>/
-- m/Memory-used>\d+<\/Memory-used>\s+/
-- s/Memory-used>\d+<\/Memory-used>\s+/Memory-used>###<\/Memory-used>/
-- m/PQO version \d+\.\d+\.\d+/
-- s/PQO version \d+\.\d+\.\d+/PQO version ##.##.##/
-- m/<Slice>[0-3]<\/Slice>/
-- s/<Slice>[0-3]<\/Slice>/<Slice>#<\/Slice> /
-- m/Executor-Memory>\d+<\//
-- s/Executor-Memory>\d+<\/([^>]+)>\s+/Executor-Memory>###<\/$1> /
-- m/<Maximum-Memory-Used>\d+<\/Maximum-Memory-Used>\s+/
-- s/<Maximum-Memory-Used>\d+<\/Maximum-Memory-Used>\s+/<Maximum-Memory-Used>###<\/Maximum-Memory-Used> /
-- m/<Work-Maximum-Memory>\d+<\/Work-Maximum-Memory>\s+/
-- s/<Work-Maximum-Memory>\d+<\/Work-Maximum-Memory>\s+/<Work-Maximum-Memory>###<\/Work-Maximum-Memory> /
-- m/<Workers>\d+<\/Workers>\s+/
-- s/<Workers>\d+<\/Workers>\s+/<Workers>###<\/Workers> /
-- end_matchsubs
-- explain_processing_off
-- Check Explain XML output
EXPLAIN (FORMAT XML) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;
-- explain_processing_on

-- explain_processing_off
-- Check Explain Analyze XML output
EXPLAIN (ANALYZE, FORMAT XML) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN locations ON locations.id = boxes.location_id;
-- explain_processing_on
