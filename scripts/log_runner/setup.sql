BEGIN;

--
-- table that stores results
--

drop table if exists dxl_tests;
create table dxl_tests(id text, gpdb text, algebrizerresult text, planfreezerresult text, optimizerresult text);


--
-- This function takes in a query id and query text. It performs the following steps:
-- 1. Transforms query text to Query structure
-- 2. Transform Query structure to DXL structure
-- 3. Transform DXL structure to Query
-- 4. Transform DXL back to DXL (algebrizer)
-- 5. Transform DXL to Query structure
-- 6. Call the planner on the Query structure and produce a plan
-- 7. Execute the plan and capture the number of rows returned
--

create or replace FUNCTION run_algebrizer_test(query_id text, query text) RETURNS void as $$
declare
	result text;
	restore_query text;
BEGIN
	-- insert entry for the given query if it doesn't exist already 
	insert into dxl_tests select query_id as id where not exists(select 1 from dxl_tests where id=query_id);
	
	BEGIN

	restore_query := 'select gpoptutils.RestoreQueryDXL(gpoptutils.DumpQueryDXL(' || quote_literal(query) || '))';

	execute restore_query into result;
	
	EXCEPTION
		WHEN OTHERS
		THEN
		result := substring(SQLERRM from 90 for 100);
	END;
	update dxl_tests set algebrizerresult = result where id=query_id;
	return;
END;
$$ LANGUAGE plpgsql;


--
-- This function takes in a query id and query text. It performs the following steps:
-- Exercise the new optimizer

create or replace FUNCTION run_optimize_test(query_id text, query text) RETURNS void as $$
declare
	result text;
	restore_query text;
BEGIN
	-- insert entry for the given query if it doesn't exist already 
	insert into dxl_tests select query_id as id where not exists(select 1 from dxl_tests where id=query_id);
	
	BEGIN

	restore_query := 'select gpoptutils.RestorePlanDXL(gpoptutils.Optimize(' || quote_literal(query) || '))';

	execute restore_query into result;
	
	EXCEPTION
		WHEN OTHERS
		THEN
		result := substring(SQLERRM from 90 for 100);
	END;
	update dxl_tests set optimizerresult = result where id=query_id;
	return;
END;
$$ LANGUAGE plpgsql;


--
-- This function takes in a query id and query text. It performs the following steps:
-- 1. Transforms query text to Query structure
-- 2. Call the planner on the Query structure and produce a Plan
-- 3. Transform Plan structure to DXL structure
-- 4. Transform DXL structure to DXL
-- 5. Transform DXL back to DXL (plan)
-- 6. Transform DXL to Plan structure
-- 7. Execute the plan and capture the number of rows returned
--

create or replace FUNCTION run_planfreezer_test(query_id text, query text) RETURNS void as $$
declare
	result text;
	restore_query text;
BEGIN
	-- insert entry for the given query if it doesn't exist already 
	insert into dxl_tests select query_id as id where not exists(select 1 from dxl_tests where id=query_id);
	
	BEGIN

	restore_query := 'select gpoptutils.RestorePlanDXL(gpoptutils.DumpPlanDXL(' || quote_literal(query) || '))';

	raise NOTICE 'restore_query: %', restore_query;

	execute restore_query into result;

	raise NOTICE 'result: %', result;

	
	EXCEPTION
		WHEN OTHERS
		THEN
		result := substring(SQLERRM from 90 for 100);
	END;
	update dxl_tests set planfreezerresult = result where id=query_id;
	return;
END;
$$ LANGUAGE plpgsql;



--
-- This function takes in a query id and query text. It performs the following steps:
-- 1. Transforms query text to Query structure
-- 2. Call the planner on the Query structure and produce a Plan
-- 3. Execute the plan and capture the number of rows returned
--
create or replace FUNCTION run_gpdb_query(query_id text, query text) RETURNS void as $$
declare
	num_rows integer;
BEGIN
	-- insert entry for the given query if it doesn't exist already 
	insert into dxl_tests select query_id as id where not exists(select 1 from dxl_tests where id=query_id);
	
	execute query;
	
	GET DIAGNOSTICS num_rows = ROW_COUNT;
	
	update dxl_tests set gpdb = 'processed ' || num_rows || ' rows' where id=query_id;
	return;
END;
$$ LANGUAGE plpgsql;



--
-- extract statements from a log file
--

create or replace function create_external_table(logfilepath text, logtable text) returns void as $$
declare
	cmd text;
begin
	execute 'drop external web table if exists logs';
	cmd := 'CREATE EXTERNAL WEB TABLE ' 
	|| logtable
	|| '('
	|| 'logtime timestamp with time zone,'
	|| 'loguser text,'
	|| 'logdatabase text,'
    	|| 'logpid text,'
    	|| 'logthread text,'
    	|| 'loghost text,'
    	|| 'logport text,'
    	|| 'logsessiontime timestamp with time zone,'
    	|| 'logtransaction int,'
    	|| 'logsession text,'
    	|| 'logcmdcount text,'
    	|| 'logsegment text,'
    	|| 'logslice text,'
    	|| 'logdistxact text,'
    	|| 'loglocalxact text,'
    	|| 'logsubxact text,'
    	|| 'logseverity text,'
    	|| 'logstate text,'
    	|| 'logmessage text,'
    	|| 'logdetail text,'
    	|| 'loghint text,'
    	|| 'logquery text,'
    	|| 'logquerypos int,'
    	|| 'logcontext text,'
    	|| 'logdebug text,'
    	|| 'logcursorpos int,'
    	|| 'logfunction text,'
    	|| 'logfile text,'
    	|| 'logline int,'
    	|| 'logstack text'
	|| ')'
	|| 'EXECUTE E''cat ' 
	|| logfilepath 
	|| ''' ON MASTER FORMAT ''CSV'' (DELIMITER AS '','' NULL AS '''' QUOTE AS ''"'')';

	execute cmd;
end;
$$ language plpgsql;


--
-- This view tallies the results from the tests
--
drop view if exists dxl_scoreboard;

create view dxl_scoreboard (allqueries, algebrizerpass, planfreezerpass, optimizerpass) as
       select (select count(*) from dxl_tests),
       (select count(*) from dxl_tests where algebrizerresult=gpdb),
       (select count(*) from dxl_tests where planfreezerresult=gpdb),
       (select count(*) from dxl_tests where optimizerresult=gpdb);



COMMIT;

