select
count(*) as num_queries, Pass, Result, Description
from 
(select 

(case when (gpdb = planfreezerresult and gpdb like 'processed%') then 'PASSED'
	  else 'NOT PASSED' end) Pass, 
	  
(case when (gpdb = planfreezerresult and gpdb like 'processed%') then 'PASSED'
	  when (gpdb <> planfreezerresult and planfreezerresult like 'processed%') then 'wrong-number-of-result-produced'
	  else 'error-during-planfreezing' end) Result, 
(case when (gpdb = planfreezerresult and planfreezerresult like 'processed%') then 'PASSED'
	  when (gpdb <> planfreezerresult and planfreezerresult like 'processed%') then 'wrong-number-of-result-produced' 
	  when (planfreezerresult like '%ARRAYREF%') then 'ARRAYREF'
	  when (planfreezerresult like '%Distinct Clause%') then 'Distinct Clause'
	  when (planfreezerresult like '%ARRAY :%') then 'ARRAY'	  	  
  	  when (planfreezerresult like '%WINDOW%') then 'WINDOW'	  	  
  	  when (planfreezerresult like '%LogicalSetOperation%') then 'LogicalSetOperation'
  	  when (planfreezerresult like '%NULLIFEXPR%') then 'NULLIFEXPR'
   	  when (planfreezerresult like '%GROUPINGFUNC%') then 'GROUPINGFUNC'  	  
   	  when (planfreezerresult like '%GROUPINGCLAUSE%') then 'GROUPINGCLAUSE'    	  
   	  when (planfreezerresult like '%COALESCE%') then 'GROUPINGCLAUSE'
   	  when (planfreezerresult like '%LogicalConstTable%') then 'LogicalConstTable'   	  
   	  when (planfreezerresult like '%GROUPID%') then 'GROUPID'   	  
  	  when (planfreezerresult like '%LogicalConstTable%') then 'LogicalConstTable'
   	  when (planfreezerresult like '%Xerces parse exception%') then 'Xerces parse exception'
   	  when (planfreezerresult like '%FIELDSELECT%') then 'FIELDSELECT'
   	  when (planfreezerresult like '%ROWCOMPARE%') then 'ROWCOMPARE'
  	  when (planfreezerresult like '%AGGREF%') then 'AGGREF'
   	  when (planfreezerresult like '%INDEXSCAN%') then 'INDEXSCAN' 
  	  when (planfreezerresult like '%FUNCTIONSCAN%') then 'FUNCTIONSCAN'
   	  when (planfreezerresult like '%SETOP%') then 'SETOP'
   	  when (planfreezerresult like '%BITMAPHEAPSCAN%') then 'BITMAPHEAPSCAN'
   	  when (planfreezerresult like '%VALUESSCAN%') then 'VALUESSCAN'
  	  when (planfreezerresult like '%SUBPLAN%') then 'SUBPLAN' 
  	  when (planfreezerresult like '%ROW%') then 'ROW' 
  	  when (planfreezerresult like '%GROUPING%') then 'GROUPING'
  	  when (planfreezerresult like '%CASETESTEXPR%') then 'CASETESTEXPR' 
   	  when (planfreezerresult like '%REPEAT%') then 'Error in REPEAT' 
	  else planfreezerresult end) Description
from dxl_tests
) d
group by ROLLUP (Pass, (Result, Description))
order by num_queries desc;