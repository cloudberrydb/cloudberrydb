select
count(*) as num_queries, Pass, Result, Description
from 
(select 

(case when (gpdb = algebrizerresult and gpdb like 'processed%') then 'PASSED'
	  else 'NOT PASSED' end) Pass, 
	  
(case when (gpdb = algebrizerresult and gpdb like 'processed%') then 'PASSED'
	  when (gpdb <> algebrizerresult and algebrizerresult like 'processed%') then 'wrong-number-of-result-produced'
	  else 'error-during-algebrization' end) Result, 
(case when (gpdb = algebrizerresult and algebrizerresult like 'processed%') then 'PASSED'
	  when (gpdb <> algebrizerresult and algebrizerresult like 'processed%') then 'wrong-number-of-result-produced' 
	  when (algebrizerresult like '%ARRAYREF%') then 'ARRAYREF'
	  when (algebrizerresult like '%Distinct Clause%') then 'Distinct Clause'
	  when (algebrizerresult like '%ARRAY :%') then 'ARRAY'	  	  
  	  when (algebrizerresult like '%WindowOperations%') then 'WindowOperations'	  	  
  	  when (algebrizerresult like '%LogicalSetOperation%') then 'LogicalSetOperation'
  	  when (algebrizerresult like '%NULLIFEXPR%') then 'NULLIFEXPR'
   	  when (algebrizerresult like '%GROUPINGFUNC%') then 'GROUPINGFUNC'  	  
   	  when (algebrizerresult like '%GROUPINGCLAUSE%') then 'GROUPINGCLAUSE'    	  
   	  when (algebrizerresult like '%COALESCE%') then 'GROUPINGCLAUSE'
   	  when (algebrizerresult like '%LogicalConstTable%') then 'LogicalConstTable'   	  
   	  when (algebrizerresult like '%GROUPID%') then 'GROUPID'   	  
  	  when (algebrizerresult like '%LogicalConstTable%') then 'LogicalConstTable'
   	  when (algebrizerresult like '%Xerces parse exception%') then 'Xerces parse exception'
   	  when (algebrizerresult like '%FIELDSELECT%') then 'FIELDSELECT'
   	  when (algebrizerresult like '%ROWCOMPARE%') then 'ROWCOMPARE'
  	  when (algebrizerresult like '%AGGREF%') then 'AGGREF'
   	  when (algebrizerresult like '%CMappingElement.cpp%') then 'Error in CMappingElement.cpp' 
   	  when (algebrizerresult like '%CTranslatorRelcacheToDXL.cpp%') then 'Error in CTranslatorRelcacheToDXL.cpp' 
   	  when (algebrizerresult like '%CTranslatorScalarToDXL.cpp%') then 'Error in CTranslatorScalarToDXL.cpp' 
   	  when (algebrizerresult like '%CTranslatorQueryToDXL.cpp%') then 'Error in CTranslatorQueryToDXL.cpp'
	  else algebrizerresult end) Description
from dxl_tests
) d
group by ROLLUP (Pass, (Result, Description))
order by num_queries desc;