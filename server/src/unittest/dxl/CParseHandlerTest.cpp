//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerTest.cpp
//
//	@doc:
//		Tests parsing DXL documents into DXL trees.
//---------------------------------------------------------------------------
#include "gpos/error/CException.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/error/CMessage.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/exception.h"
#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/eval/CConstExprEvaluatorDefault.h"

#include "unittest/base.h"
#include "unittest/dxl/CParseHandlerTest.h"
#include "unittest/dxl/statistics/CCardinalityTestUtils.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/md/CMDRequest.h"


// MD request file
const CHAR *
CParseHandlerTest::m_szMDRequestFile = "../data/dxl/parse_tests/MDRequest.xml";

// files for testing parsing of different DXL nodes
const CHAR *
CParseHandlerTest::m_rgszPlanDXLFileNames[] =
	{
		"../data/dxl/parse_tests/q1-TS.xml",
		"../data/dxl/parse_tests/q2-HJ.xml",
		"../data/dxl/parse_tests/q3-HJ2.xml",
		"../data/dxl/parse_tests/q4-NLJ-GM.xml",
		"../data/dxl/parse_tests/q5-HJ-RM.xml",
		"../data/dxl/parse_tests/q6-opexpr.xml",
		"../data/dxl/parse_tests/q7-boolexpr.xml",
		"../data/dxl/parse_tests/q8-boolexpr-not.xml",
		"../data/dxl/parse_tests/q9-constval.xml",
		"../data/dxl/parse_tests/q10-Case.xml",
		"../data/dxl/parse_tests/q11-Limit.xml",
		"../data/dxl/parse_tests/q12-Limit-NoOffset.xml",
		"../data/dxl/parse_tests/q13-AGG-TS.xml",
		"../data/dxl/parse_tests/q14-FuncExpr-NoArg.xml",
		"../data/dxl/parse_tests/q15-FuncExpr-WithArgs.xml",
		"../data/dxl/parse_tests/q16-FuncExpr-WithNestedFuncExpr.xml",
		"../data/dxl/parse_tests/q17-AggRef.xml",
		"../data/dxl/parse_tests/q18-Sort-TS.xml",
		"../data/dxl/parse_tests/q19-DistinctFrom.xml",
		"../data/dxl/parse_tests/q20-DistinctFrom-HJ.xml",
		"../data/dxl/parse_tests/q21-SubqueryScan.xml",
		"../data/dxl/parse_tests/q22-Result.xml",
		"../data/dxl/parse_tests/q23-MJ.xml",
		"../data/dxl/parse_tests/q25-AppendPartTable.xml",
		"../data/dxl/parse_tests/q28-Materialize.xml",
		"../data/dxl/parse_tests/q38-DynamicScan-PartitionSelector.xml",
		"../data/dxl/parse_tests/q39-Array.xml",
		"../data/dxl/parse_tests/q37-IndexScan.xml",
		"../data/dxl/parse_tests/q40-SubPlan.xml",
		"../data/dxl/parse_tests/q42-TableValuedFunction.xml",
		"../data/dxl/parse_tests/q44-Window.xml",
		"../data/dxl/parse_tests/q45-WindowWithFraming.xml",
		"../data/dxl/parse_tests/q48-IndexOnlyScan.xml",
		"../data/dxl/parse_tests/q49-Coalesce.xml",
		"../data/dxl/parse_tests/q57-DMLDelete.xml",
		"../data/dxl/parse_tests/q58-DMLInsert.xml",
		"../data/dxl/parse_tests/q60-DMLUpdate.xml",
		"../data/dxl/parse_tests/q61-PlanWithStats.xml",
		"../data/dxl/parse_tests/q62-CTEPlan.xml",
		"../data/dxl/parse_tests/q64-ExternalScan.xml",
		"../data/dxl/parse_tests/q67-PhysicalCTAS.xml",
		"../data/dxl/parse_tests/q70-BitmapIndexProbe.xml",
		"../data/dxl/parse_tests/q71-DynamicBitmapTableScan.xml",
		"../data/dxl/parse_tests/q72-BitmapBoolOp.xml",
		"../data/dxl/parse_tests/q74-DirectDispatchInfo.xml",
		"../data/dxl/parse_tests/q76-ValuesScan.xml",
 	};

// files for tests involving dxl representation of queries
const CHAR *
CParseHandlerTest::m_rgszQueryDXLFileNames[] =
	{
//		"../data/dxl/parse_tests/q28-LogicalGroupBy.xml",
		"../data/dxl/parse_tests/q41-SetOp.xml",
		"../data/dxl/parse_tests/q30-LogicalOrderBy.xml",
		"../data/dxl/parse_tests/q31-LogicalLimit.xml",
		"../data/dxl/parse_tests/q73-LogicalLimit-NonRemovable.xml",
		"../data/dxl/parse_tests/q32-ScalarSubquery.xml",
		"../data/dxl/parse_tests/q33-ScalarSubqueryAny.xml",
		"../data/dxl/parse_tests/q34-SubqueryExists.xml",
		"../data/dxl/parse_tests/q35-ConstTable.xml",
		"../data/dxl/parse_tests/q41-LogicalTVF.xml",
		"../data/dxl/parse_tests/q43-LogicalCTE.xml",
		"../data/dxl/parse_tests/q46-LogicalWindow.xml",
		"../data/dxl/parse_tests/q47-WindowWithFraming.xml",
		"../data/dxl/parse_tests/q50-Switch.xml",
		"../data/dxl/parse_tests/q51-SwitchNoDefault.xml",
		"../data/dxl/parse_tests/q52-CaseTest.xml",
		"../data/dxl/parse_tests/q53-NullIf.xml",
		"../data/dxl/parse_tests/q54-WindowWithNoLeadingFrameEdge.xml",
		"../data/dxl/parse_tests/q55-Insert.xml",
		"../data/dxl/parse_tests/q56-Delete.xml",
		"../data/dxl/parse_tests/q59-Update.xml",
		"../data/dxl/parse_tests/q63-LogicalExternalGet.xml",
		"../data/dxl/parse_tests/q65-LogicalCTASHash.xml",
		"../data/dxl/parse_tests/q66-LogicalCTASRandom.xml",
		"../data/dxl/parse_tests/q68-ArrayRef1.xml",
		"../data/dxl/parse_tests/q69-ArrayRef2.xml",
		"../data/dxl/parse_tests/q75-MinMax.xml",
	};

// files for the statistics tests
const CHAR *
CParseHandlerTest::m_rgszStatsDXLFileNames[] =
	{
		"../data/dxl/parse_tests/q36-Statistics.xml",
	};

// files for the metadata tests
const CHAR *
CParseHandlerTest::m_rgszMetadataDXLFileNames[] =
	{
		"../data/dxl/parse_tests/q26-Metadata.xml",
	};

// input files for scalar expression tests
const CHAR *
CParseHandlerTest::m_rgszScalarExprDXLFileNames[] =
	{
		"../data/dxl/parse_tests/s01-ConstValue.xml",
		"../data/dxl/parse_tests/s02-NestedFuncExpr.xml",
	};

const CHAR *
CParseHandlerTest::m_rgszXerceTestFileNames[] =
	{
		"../data/dxl/parse_tests/sax-fail.xml",
	};
                                   
// files for testing exception handling in DXL parsing
const CHAR *
CParseHandlerTest::m_rgszNegativeTestsFileNames[] = 
	{
		"../data/dxl/parse_tests/f1-MissingAttribute.xml",
		"../data/dxl/parse_tests/f2-UnrecognizedOp.xml",
		"../data/dxl/parse_tests/f3-UnexpectedTag.xml",
		"../data/dxl/parse_tests/f4-InvalidAttrValue.xml",
		"../data/dxl/parse_tests/f5-UnexpectedTag.xml",
		"../data/dxl/parse_tests/f6-UnexpectedTag.xml",
		"../data/dxl/parse_tests/f7-UnexpectedTag.xml",
		"../data/dxl/parse_tests/f8-UnexpectedTag.xml",
		"../data/dxl/parse_tests/f9-UnexpectedTag.xml",
		"../data/dxl/parse_tests/f10-BoolExpr-MissingChild.xml",
		"../data/dxl/parse_tests/f11-BoolExpr-ExtraChild.xml",
		"../data/dxl/parse_tests/f12-Const-MissingAttribute.xml",
		"../data/dxl/parse_tests/f13-Case-Noelse.xml",
		"../data/dxl/parse_tests/f14-Case-IncorrectCondition.xml",
		"../data/dxl/parse_tests/f15-Case-MultipleElse.xml",
		"../data/dxl/parse_tests/f16-Case-NoResultClause.xml",
		"../data/dxl/parse_tests/f17-Limit-MissingCount.xml",
	};
                                  
//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest
//
//	@doc:
//		Unittest for DXL parsing
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CParseHandlerTest::EresUnittest_ScalarExpr),
		GPOS_UNITTEST_FUNC(CParseHandlerTest::EresUnittest_Statistics),
		GPOS_UNITTEST_FUNC(CParseHandlerTest::EresUnittest_Metadata),
		GPOS_UNITTEST_FUNC(CParseHandlerTest::EresUnittest_MDRequest),
		GPOS_UNITTEST_FUNC(CParseHandlerTest::EresUnittest_RunPlanTests),
		// tests involving dxl representation of queries.
		GPOS_UNITTEST_FUNC(CParseHandlerTest::EresUnittest_RunQueryTests),

		// test that throw XML validation exception
		GPOS_UNITTEST_FUNC_THROW
			(
			CParseHandlerTest::EresUnittest_ErrSAXParseException,
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLValidationError
			),

		// tests that should throw an exception
		GPOS_UNITTEST_FUNC(CParseHandlerTest::EresUnittest_RunAllNegativeTests),
		};

	// skip OOM and Abort simulation for this test, it takes hours
	if (ITask::PtskSelf()->FTrace(EtraceSimulateOOM) || ITask::PtskSelf()->FTrace(EtraceSimulateAbort))
	{
		return GPOS_OK;
	}

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_ErrSAXParseException
//
//	@doc:
//		Unittest for exception handling during parsing an ill-formed DXL document.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_ErrSAXParseException()
{
	// create own memory pool
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();

	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, m_rgszXerceTestFileNames[0]);
	
	// function call should throw an exception
	ULLONG ullPlanId = gpos::ullong_max;
	ULLONG ullPlanSpaceSize = gpos::ullong_max;
	(void) CDXLUtils::PdxlnParsePlan(pmp, szDXL, CTestUtils::m_szXSDPath, &ullPlanId, &ullPlanSpaceSize);

	return GPOS_FAILED;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_Metadata
//
//	@doc:
//		Tests parsing of a metadata entry
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_Metadata()
{
	return EresUnittest_RunAllPositiveTests
		(
		m_rgszMetadataDXLFileNames,
		GPOS_ARRAY_SIZE(m_rgszMetadataDXLFileNames),
		&EresParseAndSerializeMetadata,
		false/* fValidate */
		);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_MDRequest
//
//	@doc:
//		Tests parsing of a metadata request
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_MDRequest()
{
	// create own memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	return EresParseAndSerializeMDRequest(pmp, m_szMDRequestFile, false /* fvalidate */);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_Statistics
//
//	@doc:
//		Tests parsing of a statistics entry
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_Statistics()
{
	return EresUnittest_RunAllPositiveTests
		(
		m_rgszStatsDXLFileNames,
		GPOS_ARRAY_SIZE(m_rgszStatsDXLFileNames),
		&EresParseAndSerializeStatistics,
		false/* fValidate */
		);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_RunPlanTests
//
//	@doc:
//		Tests parsing dxl representation of plans
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_RunPlanTests()
{
	return EresUnittest_RunAllPositiveTests
		(
		m_rgszPlanDXLFileNames,
		GPOS_ARRAY_SIZE(m_rgszPlanDXLFileNames),
		&EresParseAndSerializePlan,
		false/* fValidate */
		);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_RunQueryTests
//
//	@doc:
//		Tests involving dxl representation of queries
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_RunQueryTests()
{
	return EresUnittest_RunAllPositiveTests
		(
		m_rgszQueryDXLFileNames,
		GPOS_ARRAY_SIZE(m_rgszQueryDXLFileNames),
		&EresParseAndSerializeQuery,
		false/* fValidate */
		);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_ScalarExpr
//
//	@doc:
//		Tests parsing of a scalar expression.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_ScalarExpr()
{
	return EresUnittest_RunAllPositiveTests
		(
		m_rgszScalarExprDXLFileNames,
		GPOS_ARRAY_SIZE(m_rgszScalarExprDXLFileNames),
		&EresParseAndSerializeScalarExpr,
		false/* fValidate */
		);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_RunAllPositiveTests
//
//	@doc:
//		Run all tests that are expected to pass without any exceptions
//		for parsing DXL documents into DXL trees.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_RunAllPositiveTests
	(
	const CHAR *rgszFileNames[],
	ULONG ulFiles,
	GPOS_RESULT (*testFunc)(IMemoryPool *,const CHAR *,BOOL),
	BOOL fValidate
	)
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// loop over all test files
	for (ULONG ul = 0; ul< ulFiles; ul++)
	{
		if (GPOS_FAILED == (*testFunc)(pmp, rgszFileNames[ul], fValidate))
		{
			return GPOS_FAILED;
		}
	}
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresUnittest_RunAllNegativeTests
//
//	@doc:
//		Run all negative tests for parsing DXL documents into DXL trees.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresUnittest_RunAllNegativeTests()
{
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();

	// loop over all test files
	for (ULONG ulFileNum = 0;
			ulFileNum < GPOS_ARRAY_SIZE(m_rgszNegativeTestsFileNames);
			ulFileNum++)
	{
		GPOS_TRY
		{
			// try running the test for the current file
			EresParseAndSerializePlan(
							pmp,
							m_rgszNegativeTestsFileNames[ulFileNum],
							true /* fValidate */
			);
			// if it does not throw an exception, then it failed
			return GPOS_FAILED;
		}
		GPOS_CATCH_EX(ex)
		{
			// these tests are supposed to throw exceptions, so if
			// the test throws an exception, then it is good
			GPOS_RESET_EX;
		}
		GPOS_CATCH_END;
	}

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresParseAndSerializePlan
//
//	@doc:
//		Verifies that after parsing the given DXL file into a DXL tree,
//		it will be serialized back to the same string.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresParseAndSerializePlan
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName,
	BOOL fValidate
	)
{
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	
	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szDXLFileName);

	GPOS_CHECK_ABORT;
		
	const CHAR *szValidationPath = NULL;
	
	if (fValidate)
	{  
	   szValidationPath = CTestUtils::m_szXSDPath;
	}

	// the root of the parsed DXL tree
	ULLONG ullPlanId = gpos::ullong_max;
	ULLONG ullPlanSpaceSize = gpos::ullong_max;
	CDXLNode *pdxlnRoot = CDXLUtils::PdxlnParsePlan(pmp, szDXL, szValidationPath, &ullPlanId, &ullPlanSpaceSize);
	
	GPOS_CHECK_ABORT;

	oss << "Serializing parsed tree" << std::endl;

	CWStringDynamic strPlan(pmp);
	COstreamString osPlan(&strPlan);

	CDXLUtils::SerializePlan(pmp, osPlan, pdxlnRoot, ullPlanId, ullPlanSpaceSize, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

	GPOS_CHECK_ABORT;

	CWStringDynamic dstrExpected(pmp);
	dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXL);

	if (!dstrExpected.FEquals(&strPlan))
	{
		GPOS_TRACE(dstrExpected.Wsz());
		GPOS_TRACE(strPlan.Wsz());

		GPOS_ASSERT(!"Not matching");
	}
	
	// cleanup
	pdxlnRoot->Release();
	GPOS_DELETE_ARRAY(szDXL);
	
	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresParseAndSerializeQuery
//
//	@doc:
//		Verifies that after parsing the given DXL file into a DXL tree
//		representing a query, it will be serialized back to the same string.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresParseAndSerializeQuery
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName,
	BOOL fValidate
	)
{
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szDXLFileName);

	const CHAR *szValidationPath = NULL;
	
	if (fValidate)
	{
		szValidationPath = CTestUtils::m_szXSDPath;
	}
	
	// the root of the parsed DXL tree
	CQueryToDXLResult *pq2dxlresult = CDXLUtils::PdxlnParseDXLQuery(pmp, szDXL, szValidationPath);
	GPOS_ASSERT(NULL != pq2dxlresult);

	oss << "Serializing parsed tree" << std::endl;

	CDXLNode *pdxlnRoot = const_cast<CDXLNode *>(pq2dxlresult->Pdxln());
	DrgPdxln* pdrgpdxln = const_cast<DrgPdxln* >(pq2dxlresult->PdrgpdxlnOutputCols());
	DrgPdxln* pdrgpdxlnCTE = const_cast<DrgPdxln* >(pq2dxlresult->PdrgpdxlnCTE());

	CWStringDynamic wstrQuery(pmp);
	COstreamString osQuery(&wstrQuery);

	CDXLUtils::SerializeQuery(pmp, osQuery, pdxlnRoot, pdrgpdxln, pdrgpdxlnCTE, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

	CWStringDynamic dstrExpected(pmp);
	dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXL);

	if (!dstrExpected.FEquals(&wstrQuery))
	{
		GPOS_TRACE(wstrQuery.Wsz());
	}

	// cleanup
	GPOS_DELETE(pq2dxlresult);
	GPOS_DELETE_ARRAY(szDXL);

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresParseAndSerializeMetadata
//
//	@doc:
//		Verifies that after parsing the given DXL file containing metadata into 
//		a list of metadata objects, which should be serialized back to the same string.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresParseAndSerializeMetadata
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName,
	BOOL fValidate
	)
{
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szDXLFileName);

	GPOS_CHECK_ABORT;
	
	// parse the metadata objects into a dynamic array	
	const CHAR *szValidationPath = NULL;
	
	if (fValidate)
	{  
	   szValidationPath = CTestUtils::m_szXSDPath;
	}
	
	DrgPimdobj *pdrgpmdobj = CDXLUtils::PdrgpmdobjParseDXL(pmp, szDXL, szValidationPath);
	
	GPOS_ASSERT(NULL != pdrgpmdobj);
	
	GPOS_CHECK_ABORT;

	oss << "Serializing metadata objects" << std::endl;
	CWStringDynamic *pstr = CDXLUtils::PstrSerializeMetadata(pmp, pdrgpmdobj, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

	GPOS_CHECK_ABORT;

	CWStringDynamic dstrExpected(pmp);
	dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXL);
	
	if (!dstrExpected.FEquals(pstr))
	{
		GPOS_TRACE(pstr->Wsz());
		GPOS_ASSERT(false);
	}

	pdrgpmdobj->Release();
	GPOS_DELETE(pstr);
	GPOS_DELETE_ARRAY(szDXL);
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresParseAndSerializeMDRequest
//
//	@doc:
//		Parse an MD request and verify correctness of serialization.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresParseAndSerializeMDRequest
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName,
	BOOL fValidate
	)
{
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	
	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szDXLFileName);

	GPOS_CHECK_ABORT;
	
	// parse the mdid objects into a dynamic array	
	const CHAR *szValidationPath = NULL;
	
	if (fValidate)
	{  
	   szValidationPath = CTestUtils::m_szXSDPath;
	}
	
	CMDRequest *pmdr = CDXLUtils::PmdrequestParseDXL(pmp, szDXL, szValidationPath);
	
	GPOS_ASSERT(NULL != pmdr);
	
	GPOS_CHECK_ABORT;

	CDXLUtils::SerializeMDRequest(pmp, pmdr, oss, true /*fSerializeHeaderFooter*/, true /*fIndent*/);

	GPOS_CHECK_ABORT;

	CWStringDynamic strExpected(pmp);
	strExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXL);
	
	GPOS_ASSERT(strExpected.FEquals(&str));

	pmdr->Release();
	GPOS_DELETE_ARRAY(szDXL);
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresParseAndSerializeStatistics
//
//	@doc:
//		Verifies that after parsing the given DXL file containing statistics
//		into a list of statistics objects, which should be serialized back
//		to the same string.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresParseAndSerializeStatistics
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName,
	BOOL fValidate
	)
{
	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL /* pceeval */,
					CTestUtils::Pcm(pmp)
					);

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szDXLFileName);

	GPOS_CHECK_ABORT;

	// parse the metadata objects into a dynamic array
	const CHAR *szValidationPath = NULL;
	if (fValidate)
	{
	   szValidationPath = CTestUtils::m_szXSDPath;
	}

	// parse the statistics objects
	DrgPdxlstatsderrel *pdrgpdxlstatsderrel = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXL, szValidationPath);
	DrgPstats *pdrgpstat = CDXLUtils::PdrgpstatsTranslateStats
								(
								pmp,
								&mda,
								pdrgpdxlstatsderrel
								);

	pdrgpdxlstatsderrel->Release();


	GPOS_ASSERT(NULL != pdrgpstat);

	CStatistics *pstats = (* pdrgpstat)[0];
	GPOS_ASSERT(pstats);

	pstats->DRows();
	oss << "Statistics:" << std::endl;
	CCardinalityTestUtils::PrintStats(pmp, pstats);

	GPOS_CHECK_ABORT;

	oss << "Serializing Statistics Objects" << std::endl;
	CWStringDynamic *pstr = CDXLUtils::PstrSerializeStatistics
											(
											pmp,
											&mda,
											pdrgpstat,
											true /*fSerializeHeaderFooter*/,
											true /*fIndent*/
											);

	CWStringDynamic dstrExpected(pmp);
	dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXL);

	GPOS_ASSERT(dstrExpected.FEquals(pstr));

	pdrgpstat->Release();

	GPOS_DELETE_ARRAY(szDXL);
	GPOS_DELETE(pstr);
	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTest::EresParseAndSerializeScalarExpr
//
//	@doc:
//      Parses a ScalarExpr and verifies that the serialization is identical
//      to the original input.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CParseHandlerTest::EresParseAndSerializeScalarExpr
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName,
	BOOL fValidate
	)
{
	// read DXL file
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szDXLFileName);
	GPOS_CHECK_ABORT;

	const CHAR *szValidationPath = NULL;
	if (fValidate)
	{
		szValidationPath = CTestUtils::m_szXSDPath;
	}

	// the root of the parsed DXL tree
	CDXLNode *pdxlnRoot = CDXLUtils::PdxlnParseScalarExpr(pmp, szDXL, szValidationPath);
	GPOS_CHECK_ABORT;

	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	oss << "Serializing parsed tree" << std::endl;
	CWStringDynamic *pstr = CDXLUtils::PstrSerializeScalarExpr(pmp, pdxlnRoot, true /*fSerializeHeaderFooter*/, true /*fIndent*/);
	GPOS_CHECK_ABORT;

	CWStringDynamic dstrExpected(pmp);
	dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXL);

	GPOS_RESULT eres = GPOS_OK;
	if (!dstrExpected.FEquals(pstr))
	{
		GPOS_TRACE(dstrExpected.Wsz());
		GPOS_TRACE(pstr->Wsz());

		GPOS_ASSERT(!"Not matching");
		eres = GPOS_FAILED;
	}

	// cleanup
	pdxlnRoot->Release();
	GPOS_DELETE(pstr);
	GPOS_DELETE_ARRAY(szDXL);

	return eres;
}

// EOF
