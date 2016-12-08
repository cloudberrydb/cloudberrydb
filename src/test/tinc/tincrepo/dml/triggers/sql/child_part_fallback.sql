--start_ignore
SET client_min_messages='log';
INSERT INTO dml_trigger_table VALUES('TEST',10);
SET client_min_messages='notice';
--end_ignore

SELECT * FROM dml_trigger_table order by 2;

\!grep Planner %MYD%/output/child_part_fallback_orca.out
