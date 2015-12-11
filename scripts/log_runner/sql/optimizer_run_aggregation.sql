select
count(*) as num_queries, Pass, Result, Description
from 
(select 

(case when (gpdb = optimizerresult and gpdb like 'processed%') then 'PASSED'
	  else 'NOT PASSED' end) Pass, 
	  
(case when (gpdb = optimizerresult and gpdb like 'processed%') then 'PASSED'
	  when (gpdb <> optimizerresult and optimizerresult like 'processed%') then 'wrong-number-of-result-produced'
	  else 'error' end) Result, 
(case when (gpdb = optimizerresult and optimizerresult like 'processed%') then 'PASSED'
	  when (gpdb <> optimizerresult and optimizerresult like 'processed%') then 'wrong-number-of-result-produced' 
	  when (optimizerresult like '%ARRAYREF%') then 'ARRAYREF'
	  when (optimizerresult like '%Distinct Clause%') then 'Distinct Clause'
	  when (optimizerresult like '%ARRAY :%') then 'ARRAY'	  	  
  	  when (optimizerresult like '%WindowOperations%') then 'WindowOperations'	  	  
  	  when (optimizerresult like '%LogicalSetOperation%') then 'LogicalSetOperation'
  	  when (optimizerresult like '%NULLIFEXPR%') then 'NULLIFEXPR'
   	  when (optimizerresult like '%GROUPINGFUNC%') then 'GROUPINGFUNC'  	  
   	  when (optimizerresult like '%GROUPINGCLAUSE%') then 'GROUPINGCLAUSE'    	  
   	  when (optimizerresult like '%COALESCE%') then 'GROUPINGCLAUSE'
   	  when (optimizerresult like '%LogicalConstTable%') then 'LogicalConstTable'   	  
   	  when (optimizerresult like '%GROUPID%') then 'GROUPID'   	  
  	  when (optimizerresult like '%LogicalConstTable%') then 'LogicalConstTable'
   	  when (optimizerresult like '%Xerces parse exception%') then 'Xerces parse exception'
   	  when (optimizerresult like '%FIELDSELECT%') then 'FIELDSELECT'
   	  when (optimizerresult like '%ROWCOMPARE%') then 'ROWCOMPARE'
  	  when (optimizerresult like '%AGGREF%') then 'AGGREF'
  	  when (optimizerresult like '%IsTrue%') then 'IsTrue'
  	  when (optimizerresult like '%IsNotTrue%') then 'IsNotTrue'  	  
  	  when (optimizerresult like '%IsNotFalse%') then 'IsNotFalse'
  	  when (optimizerresult like '%IsDistinctFrom%') then 'IsDistinctFrom'  	  
  	  when (optimizerresult like '%SubqueryExists%') then 'SubqueryExists'  	     	  
   	  when (optimizerresult like '%CMappingElement.cpp%') then 'Error in CMappingElement.cpp' 
   	  when (optimizerresult like '%CTranslatorRelcacheToDXL.cpp%') then 'Error in CTranslatorRelcacheToDXL.cpp' 
   	  when (optimizerresult like '%CTranslatorScalarToDXL.cpp%') then 'Error in CTranslatorScalarToDXL.cpp' 
   	  when (optimizerresult like '%CTranslatorQueryToDXL.cpp%') then 'Error in CTranslatorQueryToDXL.cpp'
	  else optimizerresult end) Description
from dxl_tests
) d
group by ROLLUP (Pass, (Result, Description))
order by num_queries desc;