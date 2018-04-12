//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CStatisticsTest.cpp
//
//	@doc:
//		Tests for CPoint
//---------------------------------------------------------------------------

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <stdint.h>

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"

#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/base/CDatumBoolGPDB.h"

#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CScalarProjectElement.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "unittest/base.h"
#include "unittest/dxl/statistics/CCardinalityTestUtils.h"
#include "unittest/dxl/statistics/CStatisticsTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"

using namespace gpopt;

// DXL files
const CHAR *
szInputDXLFileName = "../data/dxl/statistics/Basic-Statistics-Input.xml";
const CHAR *
szOutputDXLFileName = "../data/dxl/statistics/Basic-Statistics-Output.xml";

const CHAR *
szQuerySelect = "../data/dxl/statistics/SelectQuery.xml";
const CHAR *
szPlanSelect = "../data/dxl/statistics/SelectPlan.xml";

const CTestUtils::STestCase rgtcStatistics[] =
{
	{"../data/dxl/statistics/Numeric-Input.xml", "../data/dxl/statistics/Numeric-Output-LT-MinBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input.xml", "../data/dxl/statistics/Numeric-Output-LTE-MinBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input.xml", "../data/dxl/statistics/Numeric-Output-E-MinBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input.xml", "../data/dxl/statistics/Numeric-Output-GT-MaxBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input.xml", "../data/dxl/statistics/Numeric-Output-GTE-MaxBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input.xml", "../data/dxl/statistics/Numeric-Output-E-MaxBoundary.xml"},

	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-LT-MinBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-LTE-MinBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-E-MinBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-GT-MinBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-GTE-MinBoundary.xml"},

	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-LT-MaxBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-LTE-MaxBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-GT-MaxBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-GTE-MaxBoundary.xml"},
	{"../data/dxl/statistics/Numeric-Input2.xml", "../data/dxl/statistics/Numeric-Output-2-E-MaxBoundary.xml"},
};

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest
//
//	@doc:
//		Unittest for statistics objects
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest()
{
	// tests that use shared optimization context
	CUnittest rgutSharedOptCtxt[] =
		{
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsBasic),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsBasicsFromDXL),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsBasicsFromDXLNumeric),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_UnionAll),
		// TODO,  Mar 18 2013 temporarily disabling the test
		// GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsSelectDerivation),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsFilter),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsFilterConj),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsFilterDisj),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsNestedPred),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsAccumulateCard)
		};

	// tests that use separate optimization contexts
	CUnittest rgutSeparateOptCtxt[] =
		{
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_GbAggWithRepeatedGbCols),
		};

	// run tests with shared optimization context first
	GPOS_RESULT eres = GPOS_FAILED;
	{
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();

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

		eres = CUnittest::EresExecute(rgutSharedOptCtxt, GPOS_ARRAY_SIZE(rgutSharedOptCtxt));

		if (GPOS_FAILED == eres)
		{
			return eres;
		}
	}

	// run tests with separate optimization contexts
	return CUnittest::EresExecute(rgutSeparateOptCtxt, GPOS_ARRAY_SIZE(rgutSeparateOptCtxt));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_UnionAll
//
//	@doc:
//		Testing statistical operations on Union All;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_UnionAll()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	SStatsUnionAllSTestCase rgstatsunionalltc[] =
	{
		{"../data/dxl/statistics/UnionAll-Input-1.xml", "../data/dxl/statistics/UnionAll-Output-1.xml"},
	};

	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgstatsunionalltc);
	for (ULONG ul = 0; ul < ulTestCases; ul++)
	{
		SStatsUnionAllSTestCase elem = rgstatsunionalltc[ul];

		// read input/output DXL file
		CHAR *szDXLInput = CDXLUtils::SzRead(pmp, elem.m_szInputFile);
		CHAR *szDXLOutput = CDXLUtils::SzRead(pmp, elem.m_szOutputFile);

		GPOS_CHECK_ABORT;

		// parse the input statistics objects
		DrgPdxlstatsderrel *pdrgpdxlstatsderrel = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInput, NULL);
		DrgPstats *pdrgpstatBefore = CDXLUtils::PdrgpstatsTranslateStats(pmp, pmda, pdrgpdxlstatsderrel);
		pdrgpdxlstatsderrel->Release();

		GPOS_ASSERT(NULL != pdrgpstatBefore);
		GPOS_ASSERT(2 == pdrgpstatBefore->UlLength());
		CStatistics *pstats1 = (*pdrgpstatBefore)[0];
		CStatistics *pstats2 = (*pdrgpstatBefore)[1];

		GPOS_CHECK_ABORT;

		DrgPul *pdrgpulColIdOutput = Pdrgpul(pmp, 1);
		DrgPul *pdrgpulColIdInput1 = Pdrgpul(pmp, 1);
		DrgPul *pdrgpulColIdInput2 = Pdrgpul(pmp, 2);

		CStatistics *pstatsOutput = pstats1->PstatsUnionAll(pmp, pstats2, pdrgpulColIdOutput, pdrgpulColIdInput1, pdrgpulColIdInput2);

		GPOS_ASSERT(NULL != pstatsOutput);

		DrgPstats *pdrgpstatOutput = GPOS_NEW(pmp) DrgPstats(pmp);
		pdrgpstatOutput->Append(pstatsOutput);

		// serialize and compare against expected stats
		CWStringDynamic *pstrOutput = CDXLUtils::PstrSerializeStatistics
													(
													pmp,
													pmda,
													pdrgpstatOutput,
													true /*fSerializeHeaderFooter*/,
													true /*fIndent*/
													);
		CWStringDynamic dstrExpected(pmp);
		dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXLOutput);

		GPOS_RESULT eres = GPOS_OK;
		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		// compare the two dxls
		if (!pstrOutput->FEquals(&dstrExpected))
		{
			oss << "Output does not match expected DXL document" << std::endl;
			oss << "Actual: " << std::endl;
			oss << pstrOutput->Wsz() << std::endl;
			oss << "Expected: " << std::endl;
			oss << dstrExpected.Wsz() << std::endl;
			GPOS_TRACE(str.Wsz());
			
			eres = GPOS_FAILED;
		}


		// clean up
		pdrgpstatBefore->Release();
		pdrgpstatOutput->Release();

		GPOS_DELETE_ARRAY(szDXLInput);
		GPOS_DELETE_ARRAY(szDXLOutput);
		GPOS_DELETE(pstrOutput);

		if (GPOS_FAILED == eres)
		{
			return eres;
		}
	}

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_GbAggWithRepeatedGbCols
//
//	@doc:
//		GbAgg test when grouping on repeated columns
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_GbAggWithRepeatedGbCols()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

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

	CExpression *pexpr = CTestUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp);
	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexpr->PdpDerive());
	CColRefSet *pcrs = pdprel->PcrsOutput();

	// create first GbAgg expression: GbAgg on top of given expression
	DrgPcr *pdrgpcr1 = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcr1->Append(pcrs->PcrFirst());
	CExpression *pexprGbAgg1 =
		CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr1, pexpr, GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp)));

	// create second GbAgg expression: GbAgg with repeated base column on top of given expression
	DrgPcr *pdrgpcr2 = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcr2->Append(pcrs->PcrFirst());
	pdrgpcr2->Append(pcrs->PcrFirst());
	pexpr->AddRef();
	CExpression *pexprGbAgg2 =
			CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr2, pexpr, GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp)));

	// create third GbAgg expression: GbAgg with a repeated projected base column on top of given expression
	pexpr->AddRef();
	CExpression *pexprPrj = CUtils::PexprAddProjection(pmp, pexpr, CUtils::PexprScalarIdent(pmp, pcrs->PcrFirst()));
	CColRef *pcrComputed = CScalarProjectElement::PopConvert((*(*pexprPrj)[1])[0]->Pop())->Pcr();
	DrgPcr *pdrgpcr3 = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcr3->Append(pcrs->PcrFirst());
	pdrgpcr3->Append(pcrComputed);
	CExpression *pexprGbAgg3 =
			CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr3, pexprPrj, GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp)));

	// derive stats on different GbAgg expressions
	CReqdPropRelational *prprel = GPOS_NEW(pmp) CReqdPropRelational(GPOS_NEW(pmp) CColRefSet(pmp));
	(void) pexprGbAgg1->PstatsDerive(prprel, NULL /* pdrgpstatCtxt */);
	(void) pexprGbAgg2->PstatsDerive(prprel, NULL /* pdrgpstatCtxt */);
	(void) pexprGbAgg3->PstatsDerive(prprel, NULL /* pdrgpstatCtxt */);

	BOOL fRows1EqualRows2 = (pexprGbAgg1->Pstats()->DRows() == pexprGbAgg2->Pstats()->DRows());
	BOOL fRows2EqualRows3 = (pexprGbAgg2->Pstats()->DRows() == pexprGbAgg3->Pstats()->DRows());

	{
		CAutoTrace at(pmp);
		at.Os() << std::endl << "pexprGbAgg1:" <<  std::endl << *pexprGbAgg1 << std::endl;
		at.Os() << std::endl << "pexprGbAgg2:" <<  std::endl << *pexprGbAgg2 << std::endl;
		at.Os() << std::endl << "pexprGbAgg3:" <<  std::endl << *pexprGbAgg3 << std::endl;
	}

	// cleanup
	pexprGbAgg1->Release();
	pexprGbAgg2->Release();
	pexprGbAgg3->Release();
	prprel->Release();

	if (fRows1EqualRows2 && fRows2EqualRows3)
	{
		return GPOS_OK;
	}

	return GPOS_FAILED;
}

// generates example int histogram corresponding to dimension table
CHistogram*
CStatisticsTest::PhistExampleInt4Dim
	(
	IMemoryPool *pmp
	)
{
	// generate histogram of the form [0, 10), [10, 20), [20, 30) ... [80, 90)
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	for (ULONG ulIdx = 0; ulIdx < 9; ulIdx++)
	{
		INT iLower = INT(ulIdx * 10);
		INT iUpper = iLower + INT(10);
		CDouble dFrequency(0.1);
		CDouble dDistinct(10.0);
		CBucket *pbucket = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, iLower, iUpper, dFrequency, dDistinct);
		pdrgppbucket->Append(pbucket);
	}

	return  GPOS_NEW(pmp) CHistogram(pdrgppbucket);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsBasicsFromDXL
//
//	@doc:
//		Reads a DXL document, generates the statistics object, performs a
//		filter operation on it, serializes it into a DXL document and
//		compares the generated DXL document with the expected DXL document.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsBasicsFromDXL()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	// read input DXL file
	CHAR *szDXLInput = CDXLUtils::SzRead(pmp, szInputDXLFileName);
	// read output DXL file
	CHAR *szDXLOutput = CDXLUtils::SzRead(pmp, szOutputDXLFileName);

	GPOS_CHECK_ABORT;

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// parse the statistics objects
	DrgPdxlstatsderrel *pdrgpdxlstatsderrel = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInput, NULL);
	DrgPstats *pdrgpstatsBefore = CDXLUtils::PdrgpstatsTranslateStats
									(
									pmp,
									pmda,
									pdrgpdxlstatsderrel
									);
	pdrgpdxlstatsderrel->Release();
	GPOS_ASSERT(NULL != pdrgpstatsBefore);

	GPOS_CHECK_ABORT;

	// create a filter
	CStatsPredConj *pstatspred = GPOS_NEW(pmp) CStatsPredConj(CStatisticsTest::Pdrgpstatspred2(pmp));
	GPOS_RESULT eres = EresUnittest_CStatisticsCompare
							(
							pmp,
							pmda,
							pdrgpstatsBefore,
							pstatspred,
							szDXLOutput
							);

	// clean up
	pdrgpstatsBefore->Release();
	pstatspred->Release();
	GPOS_DELETE_ARRAY(szDXLInput);
	GPOS_DELETE_ARRAY(szDXLOutput);

	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsFilter
//
//	@doc:
//		Reads a DXL document, generates the statistics object, performs a
//		filter operation on it, serializes it into a DXL document and
//		compares the generated DXL document with the expected DXL document.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsFilter()
{
	SStatsFilterSTestCase rgstatstc[] =
	{
		{"../data/dxl/statistics/Select-Statistics-Input-Null-Bucket.xml", "../data/dxl/statistics/Select-Statistics-Output-Null-Bucket.xml", PstatspredNullableCols},
		{"../data/dxl/statistics/Select-Statistics-Input-Null-Bucket.xml", "../data/dxl/statistics/Select-Statistics-Output-Null-Constant.xml", PstatspredWithNullConstant},
		{"../data/dxl/statistics/Select-Statistics-Input-Null-Bucket.xml", "../data/dxl/statistics/Select-Statistics-Output-NotNull-Constant.xml", PstatspredNotNull},
	};

	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgstatstc);

	return EresUnittest_CStatistics(rgstatstc, ulTestCases);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsFilterDisj
//
//	@doc:
//		Reads a DXL document, generates the statistics object, performs a
//		filter operation on it, serializes it into a DXL document and
//		compares the generated DXL document with the expected DXL document.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsFilterDisj()
{
	SStatsFilterSTestCase rgstatsdisjtc[] =
	{
		{"../data/dxl/statistics/Disj-Input-1.xml", "../data/dxl/statistics/Disj-Output-1.xml", PstatspredDisj1},
		{"../data/dxl/statistics/Disj-Input-1.xml", "../data/dxl/statistics/Disj-Output-1.xml", PstatspredDisj2},
		{"../data/dxl/statistics/Disj-Input-1.xml", "../data/dxl/statistics/Disj-Output-1.xml", PstatspredDisj3},
		{"../data/dxl/statistics/Disj-Input-2.xml", "../data/dxl/statistics/Disj-Output-2-1.xml", PstatspredDisj4},
		{"../data/dxl/statistics/Disj-Input-2.xml", "../data/dxl/statistics/Disj-Output-2-2.xml", PstatspredDisj5},
		{"../data/dxl/statistics/Disj-Input-2.xml", "../data/dxl/statistics/Disj-Output-2-3.xml", PstatspredDisj6},
		{"../data/dxl/statistics/Disj-Input-2.xml", "../data/dxl/statistics/Disj-Output-2-4.xml", PstatspredDisj7},
		{"../data/dxl/statistics/NestedPred-Input-10.xml", "../data/dxl/statistics/Disj-Output-8.xml", PstatspredDisj8},
	};

	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgstatsdisjtc);

	return EresUnittest_CStatistics(rgstatsdisjtc, ulTestCases);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsFilterConj
//
//	@doc:
//		Reads a DXL document, generates the statistics object, performs a
//		filter operation on it, serializes it into a DXL document and
//		compares the generated DXL document with the expected DXL document.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsFilterConj()
{
	SStatsFilterSTestCase rgstatsdisjtc[] =
	{
			{"../data/dxl/statistics/NestedPred-Input-9.xml", "../data/dxl/statistics/NestedPred-Output-9.xml", PstatspredConj},
	};

	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgstatsdisjtc);

	return EresUnittest_CStatistics(rgstatsdisjtc, ulTestCases);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsNestedPred
//
//	@doc:
//		Reads a DXL document, generates the statistics object, performs a
//		filter operation on it, serializes it into a DXL document and
//		compares the generated DXL document with the expected DXL document.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsNestedPred()
{
	SStatsFilterSTestCase rgstatsdisjtc[] =
	{
	    {"../data/dxl/statistics/NestedPred-Input-1.xml", "../data/dxl/statistics/NestedPred-Output-1.xml", PstatspredNestedPredDiffCol1},
		{"../data/dxl/statistics/NestedPred-Input-1.xml", "../data/dxl/statistics/NestedPred-Output-1.xml", PstatspredNestedPredDiffCol2},
		{"../data/dxl/statistics/NestedPred-Input-2.xml", "../data/dxl/statistics/NestedPred-Output-2.xml", PstatspredNestedPredCommonCol1},
		{"../data/dxl/statistics/NestedPred-Input-1.xml", "../data/dxl/statistics/NestedPred-Output-3.xml", PstatspredNestedSharedCol},
		{"../data/dxl/statistics/NestedPred-Input-3.xml", "../data/dxl/statistics/NestedPred-Output-4.xml", PstatspredDisjOverConjSameCol1},
		{"../data/dxl/statistics/NestedPred-Input-3.xml", "../data/dxl/statistics/NestedPred-Input-3.xml", PstatspredDisjOverConjSameCol2},
		{"../data/dxl/statistics/NestedPred-Input-1.xml", "../data/dxl/statistics/NestedPred-Output-5.xml", PstatspredDisjOverConjDifferentCol1},
		{"../data/dxl/statistics/NestedPred-Input-1.xml", "../data/dxl/statistics/NestedPred-Output-6.xml", PstatspredDisjOverConjMultipleIdenticalCols},
		{"../data/dxl/statistics/NestedPred-Input-2.xml", "../data/dxl/statistics/NestedPred-Output-7.xml", PstatspredNestedPredCommonCol2},
		{"../data/dxl/statistics/NestedPred-Input-8.xml", "../data/dxl/statistics/NestedPred-Output-8.xml", PstatspredDisjOverConjSameCol3},
		{"../data/dxl/statistics/NestedPred-Input-10.xml", "../data/dxl/statistics/NestedPred-Output-10.xml", PstatspredDisjOverConjSameCol4},
	};

	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgstatsdisjtc);

	return EresUnittest_CStatistics(rgstatsdisjtc, ulTestCases);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatistics
//
//	@doc:
//		Reads a DXL document, generates the statistics object, performs a
//		filter operation on it, serializes it into a DXL document and
//		compares the generated DXL document with the expected DXL document.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatistics
	(
	SStatsFilterSTestCase rgstatsdisjtc[],
	ULONG ulTestCases
	)
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	for (ULONG ul = 0; ul < ulTestCases; ul++)
	{
		SStatsFilterSTestCase elem = rgstatsdisjtc[ul];

		// read input/output DXL file
		CHAR *szDXLInput = CDXLUtils::SzRead(pmp, elem.m_szInputFile);
		CHAR *szDXLOutput = CDXLUtils::SzRead(pmp, elem.m_szOutputFile);

		GPOS_CHECK_ABORT;

		// parse the statistics objects
		DrgPdxlstatsderrel *pdrgpdxlstatsderrel = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInput, NULL);
		DrgPstats *pdrgpstatBefore = CDXLUtils::PdrgpstatsTranslateStats(pmp, pmda, pdrgpdxlstatsderrel);
		pdrgpdxlstatsderrel->Release();
		GPOS_ASSERT(NULL != pdrgpstatBefore);

		GPOS_CHECK_ABORT;

		// generate the disjunctive predicate
		FnPstatspredDisj *pf = elem.m_pf;
		GPOS_ASSERT(NULL != pf);
		CStatsPred *pstatspredDisj = pf(pmp);

		GPOS_RESULT eres = EresUnittest_CStatisticsCompare
									(
									pmp,
									pmda,
									pdrgpstatBefore,
									pstatspredDisj,
									szDXLOutput
									);

		// clean up
		pdrgpstatBefore->Release();
		pstatspredDisj->Release();
		GPOS_DELETE_ARRAY(szDXLInput);
		GPOS_DELETE_ARRAY(szDXLOutput);

		if (GPOS_FAILED == eres)
		{
			return eres;
		}
	}

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsBasicsFromDXLNumeric
//
//	@doc:
//		Reads a DXL document, generates the statistics object, performs a
//		filter operation on it, serializes it into a DXL document and
//		compares the generated DXL document with the expected DXL document.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsBasicsFromDXLNumeric()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	SStatsCmpValElem rgStatsCmpValElem[] =
	{
		{CStatsPred::EstatscmptL,    GPOS_WSZ_LIT("AAAACgAAAgABAA=="), CDouble(1.0)},
		{CStatsPred::EstatscmptLEq,  GPOS_WSZ_LIT("AAAACgAAAgABAA=="), CDouble(1.0)},
		{CStatsPred::EstatscmptEq,   GPOS_WSZ_LIT("AAAACgAAAgABAA=="), CDouble(1.0)},

		{CStatsPred::EstatscmptG,    GPOS_WSZ_LIT("AAAACgAAAgAyAA=="), CDouble(50.0)},
		{CStatsPred::EstatscmptGEq,  GPOS_WSZ_LIT("AAAACgAAAgAyAA=="), CDouble(50.0)},
		{CStatsPred::EstatscmptEq,   GPOS_WSZ_LIT("AAAACgAAAgAyAA=="), CDouble(50.0)},

		{CStatsPred::EstatscmptL,    GPOS_WSZ_LIT("AAAACgAAAgABAA=="), CDouble(1.0)},
		{CStatsPred::EstatscmptLEq,  GPOS_WSZ_LIT("AAAACgAAAgABAA=="), CDouble(1.0)},
		{CStatsPred::EstatscmptEq,   GPOS_WSZ_LIT("AAAACgAAAgABAA=="), CDouble(1.0)},
		{CStatsPred::EstatscmptG,    GPOS_WSZ_LIT("AAAACgAAAgABAA=="), CDouble(1.0)},
		{CStatsPred::EstatscmptGEq,  GPOS_WSZ_LIT("AAAACgAAAgABAA=="), CDouble(1.0)},

		{CStatsPred::EstatscmptL,    GPOS_WSZ_LIT("AAAACgAAAgAyAA=="), CDouble(50.0)},
		{CStatsPred::EstatscmptLEq,  GPOS_WSZ_LIT("AAAACgAAAgAyAA=="), CDouble(50.0)},
		{CStatsPred::EstatscmptG,    GPOS_WSZ_LIT("AAAACgAAAgAyAA=="), CDouble(50.0)},
		{CStatsPred::EstatscmptGEq,  GPOS_WSZ_LIT("AAAACgAAAgAyAA=="), CDouble(50.0)},
		{CStatsPred::EstatscmptEq,   GPOS_WSZ_LIT("AAAACgAAAgAyAA=="), CDouble(50.0)},
	};

	const ULONG ulLen = GPOS_ARRAY_SIZE(rgStatsCmpValElem);
	GPOS_ASSERT(ulLen == GPOS_ARRAY_SIZE(rgtcStatistics));
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		// read input DXL file
		CHAR *szDXLInput = CDXLUtils::SzRead(pmp, rgtcStatistics[ul].szInputFile);
		// read output DXL file
		CHAR *szDXLOutput = CDXLUtils::SzRead(pmp, rgtcStatistics[ul].szOutputFile);

		GPOS_CHECK_ABORT;

		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
		// parse the statistics objects
		DrgPdxlstatsderrel *pdrgpdxlstatsderrel = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInput, NULL);
		DrgPstats *pdrgpstatBefore = CDXLUtils::PdrgpstatsTranslateStats(pmp, pmda, pdrgpdxlstatsderrel);
		pdrgpdxlstatsderrel->Release();

		GPOS_ASSERT(NULL != pdrgpstatBefore);

		GPOS_CHECK_ABORT;

		SStatsCmpValElem statsCmpValElem = rgStatsCmpValElem[ul];

		DrgPstatspred *pdrgpstatspred = PdrgppredfilterNumeric(pmp, 1 /*ulColId*/, statsCmpValElem);
		CStatsPredConj *pstatspred = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred);
		GPOS_RESULT eres = EresUnittest_CStatisticsCompare
							(
							pmp,
							pmda,
							pdrgpstatBefore,
							pstatspred,
							szDXLOutput,
							true /*fApplyTwice*/
							);

		// clean up
		pdrgpstatBefore->Release();
		pstatspred->Release();
		GPOS_DELETE_ARRAY(szDXLInput);
		GPOS_DELETE_ARRAY(szDXLOutput);

		if (GPOS_OK != eres)
		{
			return eres;
		}
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PdrgpstatspredNumeric
//
//	@doc:
//		Generate a numeric filter on the column specified and the literal value
//
//---------------------------------------------------------------------------
DrgPstatspred *
CStatisticsTest::PdrgppredfilterNumeric
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	SStatsCmpValElem statsCmpValElem
	)
{
	// create a filter
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);
	CWStringDynamic *pstrNumeric = GPOS_NEW(pmp) CWStringDynamic(pmp, statsCmpValElem.m_wsz);
	CStatsPredPoint *pstatspred = GPOS_NEW(pmp) CStatsPredPoint
													(
													ulColId,
													statsCmpValElem.m_escmpt,
													CCardinalityTestUtils::PpointNumeric(pmp, pstrNumeric, statsCmpValElem.m_dVal)
													);
	pdrgpstatspred->Append(pstatspred);
	GPOS_DELETE(pstrNumeric);

	return pdrgpstatspred;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsCompare
//
//	@doc:
//		Performs a filter operation on it, serializes it into a DXL document
//		and compares the generated DXL document with the expected DXL document
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsCompare
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	DrgPstats *pdrgpstatBefore,
	CStatsPred *pstatspred,
	const CHAR *szDXLOutput,
	BOOL fApplyTwice
	)
{
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	CStatistics *pstatsInput = (* pdrgpstatBefore)[0];

	GPOS_TRACE(GPOS_WSZ_LIT("Statistics before"));
	CCardinalityTestUtils::PrintStats(pmp, pstatsInput);

	CStatistics *pstatsOutput = pstatsInput->PstatsFilter(pmp, pstatspred, true /* fCapNdvs */);

	GPOS_TRACE(GPOS_WSZ_LIT("Statistics after"));
	CCardinalityTestUtils::PrintStats(pmp, pstatsOutput);

	// output array of stats objects
	DrgPstats *pdrgpstatOutput = GPOS_NEW(pmp) DrgPstats(pmp);
	pdrgpstatOutput->Append(pstatsOutput);

	oss << "Serializing Input Statistics Objects (Before Filter)" << std::endl;
	CWStringDynamic *pstrInput = CDXLUtils::PstrSerializeStatistics
												(
												pmp,
												pmda,
												pdrgpstatBefore,
												true /*fSerializeHeaderFooter*/,
												true /*fIndent*/
												);
	GPOS_TRACE(pstrInput->Wsz());
	GPOS_DELETE(pstrInput);

	oss << "Serializing Output Statistics Objects (After Filter)" << std::endl;
	CWStringDynamic *pstrOutput = CDXLUtils::PstrSerializeStatistics
												(
												pmp,
												pmda,
												pdrgpstatOutput,
												true /*fSerializeHeaderFooter*/,
												true /*fIndent*/
												);
	GPOS_TRACE(pstrOutput->Wsz());

	CWStringDynamic dstrExpected(pmp);
	dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXLOutput);

	GPOS_RESULT eres = CTestUtils::EresCompare
							(
							oss,
							pstrOutput,
							&dstrExpected,
							false /* ignore mismatch */
							);
	
	if (fApplyTwice && GPOS_OK == eres)
	{
		CStatistics *pstatsOutput2 = pstatsOutput->PstatsFilter(pmp, pstatspred, true /* fCapNdvs */);
		pstatsOutput2->DRows();
		GPOS_TRACE(GPOS_WSZ_LIT("Statistics after another filter"));
		CCardinalityTestUtils::PrintStats(pmp, pstatsOutput2);

		// output array of stats objects
		DrgPstats *pdrgpstatOutput2 = GPOS_NEW(pmp) DrgPstats(pmp);
		pdrgpstatOutput2->Append(pstatsOutput2);

		CWStringDynamic *pstrOutput2 = CDXLUtils::PstrSerializeStatistics
													(
													pmp,
													pmda,
													pdrgpstatOutput2,
													true /*fSerializeHeaderFooter*/,
													true /*fIndent*/
													);
		eres = CTestUtils::EresCompare
					(
					oss,
					pstrOutput2,
					&dstrExpected,
					false /* ignore mismatch */
					);

		pdrgpstatOutput2->Release();
		GPOS_DELETE(pstrOutput2);
	}

	pdrgpstatOutput->Release();
	GPOS_DELETE(pstrOutput);

	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PtabdescTwoColumnSource
//
//	@doc:
//		Create a table descriptor with two columns having the given names.
//
//---------------------------------------------------------------------------
CTableDescriptor *
CStatisticsTest::PtabdescTwoColumnSource
	(
	IMemoryPool *pmp,
	const CName &nameTable,
	const IMDTypeInt4 *pmdtype,
	const CWStringConst &strColA,
	const CWStringConst &strColB
	)
{
	CTableDescriptor *ptabdesc = GPOS_NEW(pmp) CTableDescriptor
									(
									pmp,
									GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1),
									nameTable,
									false, // fConvertHashToRandom
									IMDRelation::EreldistrRandom,
									IMDRelation::ErelstorageHeap,
									0  // ulExecuteAsUser
									);

	for (ULONG ul = 0; ul < 2; ul++)
	{
		// create a shallow constant string to embed in a name
		const CWStringConst *pstrName = &strColA;
		if (0 < ul)
		{
			pstrName = &strColB;
		}
		CName nameColumn(pstrName);

		CColumnDescriptor *pcoldesc = GPOS_NEW(pmp) CColumnDescriptor
											(
											pmp,
											pmdtype,
											IDefaultTypeModifier,
											OidInvalidCollation,
											nameColumn,
											ul + 1,
											false /*fNullable*/
											);
		ptabdesc->AddColumn(pcoldesc);
	}

	return ptabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsBasic
//
//	@doc:
//		Basic statistics test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsBasic()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	const IMDTypeInt4 *pmdtypeint4 = COptCtxt::PoctxtFromTLS()->Pmda()->PtMDType<IMDTypeInt4>();

	CWStringConst strRelAlias(GPOS_WSZ_LIT("Rel1"));
	CWStringConst strColA(GPOS_WSZ_LIT("a"));
	CWStringConst strColB(GPOS_WSZ_LIT("b"));
	CTableDescriptor *ptabdesc =
			PtabdescTwoColumnSource(pmp, CName(&strRelAlias), pmdtypeint4, strColA, strColB);
	CExpression *pexprGet = CTestUtils::PexprLogicalGet(pmp, ptabdesc, &strRelAlias);

	if (NULL == pcf->PcrLookup(1 /*ulId*/))
	{
		// create column references for grouping columns
		(void) pcf->PcrCreate
				(
				pmdtypeint4,
				IDefaultTypeModifier,
				OidInvalidCollation,
				0 /* iAttno */,
				false /*FNullable*/,
				1 /* ulId */,
				CName(&strColA),
				pexprGet->Pop()->UlOpId()
				);
	}

	if (NULL == pcf->PcrLookup(2 /*ulId*/))
	{
		(void) pcf->PcrCreate
				(
				pmdtypeint4,
				IDefaultTypeModifier,
				OidInvalidCollation,
				1 /* iAttno */,
				false /*FNullable*/,
				2 /* ulId */,
				CName(&strColB),
				pexprGet->Pop()->UlOpId()
				);
	}

	// create hash map from colid -> histogram
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	// generate bool histogram for column 1
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(1), CCardinalityTestUtils::PhistExampleBool(pmp));

	// generate int histogram for column 2
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(2), CCardinalityTestUtils::PhistExampleInt4(pmp));

	// array capturing columns for which width information is available
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	// width for boolean
	phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(1), GPOS_NEW(pmp) CDouble(1.0));

	// width for int
	phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(2), GPOS_NEW(pmp) CDouble(4.0));

	CStatistics *pstats = GPOS_NEW(pmp) CStatistics(pmp, phmulhist, phmuldoubleWidth, 1000.0 /* dRows */, false /* fEmpty */);
	pstats->DRows();

	GPOS_TRACE(GPOS_WSZ_LIT("pstats"));

	// before stats
	CCardinalityTestUtils::PrintStats(pmp, pstats);

	// create a filter: column 1: [25,45), column 2: [true, true)
	DrgPstatspred *pdrgpstatspred = Pdrgpstatspred1(pmp);

	CStatsPredConj *pstatspred = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred);
	CStatistics *pstats1 = pstats->PstatsFilter(pmp, pstatspred, true /* fCapNdvs */);
	pstats1->DRows();

	GPOS_TRACE(GPOS_WSZ_LIT("pstats1 after filter"));

	// after stats
	CCardinalityTestUtils::PrintStats(pmp, pstats1);

	// create another statistics structure with a single int4 column with id 10
	HMUlHist *phmulhist2 = GPOS_NEW(pmp) HMUlHist(pmp);
	phmulhist2->FInsert(GPOS_NEW(pmp) ULONG(10), PhistExampleInt4Dim(pmp));

	HMUlDouble *phmuldoubleWidth2 = GPOS_NEW(pmp) HMUlDouble(pmp);
	phmuldoubleWidth2->FInsert(GPOS_NEW(pmp) ULONG(10), GPOS_NEW(pmp) CDouble(4.0));

	CStatistics *pstats2 = GPOS_NEW(pmp) CStatistics(pmp, phmulhist2, phmuldoubleWidth2, 100.0 /* dRows */, false /* fEmpty */);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats2"));
	CCardinalityTestUtils::PrintStats(pmp, pstats2);

	// join pstats with pstats2
	CStatsPredJoin *pstatspredjoin = GPOS_NEW(pmp) CStatsPredJoin(2, CStatsPred::EstatscmptEq, 10);
	DrgPstatspredjoin *pdrgpstatspredjoin = GPOS_NEW(pmp) DrgPstatspredjoin(pmp);
	pdrgpstatspredjoin->Append(pstatspredjoin);
	CStatistics *pstats3 = pstats->PstatsInnerJoin(pmp, pstats2, pdrgpstatspredjoin);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats3 = pstats JOIN pstats2 on (col2 = col10)"));
	// after stats
	CCardinalityTestUtils::PrintStats(pmp, pstats3);

	// group by pstats on columns 1 and 2
	DrgPul *pdrgpulGC = GPOS_NEW(pmp) DrgPul(pmp);
	pdrgpulGC->Append(GPOS_NEW(pmp) ULONG(1));
	pdrgpulGC->Append(GPOS_NEW(pmp) ULONG(2));

	DrgPul *pdrgpulAgg = GPOS_NEW(pmp) DrgPul(pmp);
	CStatistics *pstats4 = pstats->PstatsGroupBy(pmp, pdrgpulGC, pdrgpulAgg, NULL /*pbsKeys*/);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats4 = pstats group by"));
	CCardinalityTestUtils::PrintStats(pmp, pstats4);

	// LASJ stats
	CStatistics *pstats5 = pstats->PstatsLASJoin(pmp, pstats2, pdrgpstatspredjoin, true /* fIgnoreLasjHistComputation */);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats5 = pstats LASJ pstats2 on (col2 = col10)"));
	CCardinalityTestUtils::PrintStats(pmp, pstats5);

	// union all
	DrgPul *pdrgpulColIds = GPOS_NEW(pmp) DrgPul(pmp);
	pdrgpulColIds->Append(GPOS_NEW(pmp) ULONG(1));
	pdrgpulColIds->Append(GPOS_NEW(pmp) ULONG(2));
	pdrgpulColIds->AddRef();
	pdrgpulColIds->AddRef();
	pdrgpulColIds->AddRef();

	CStatistics *pstats6 = pstats->PstatsUnionAll(pmp, pstats, pdrgpulColIds, pdrgpulColIds, pdrgpulColIds);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats6 = pstats1 union all pstats1"));
	CCardinalityTestUtils::PrintStats(pmp, pstats6);

	CStatistics *pstats7 = pstats->PstatsLimit(pmp, CDouble(4.0));

	GPOS_TRACE(GPOS_WSZ_LIT("pstats7 = pstats limit 4"));
	CCardinalityTestUtils::PrintStats(pmp, pstats7);

	pstats->Release();
	pstats1->Release();
	pstats2->Release();
	pstats3->Release();
	pstats4->Release();
	pstats5->Release();
	pstats6->Release();
	pstats7->Release();
	pstatspred->Release();
	pdrgpstatspredjoin->Release();
	pdrgpulGC->Release();
	pdrgpulAgg->Release();
	pdrgpulColIds->Release();
	pexprGet->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PdrgpstatspredInteger
//
//	@doc:
//		Generate an array of filter given a column identifier, comparison type,
//		and array of integer point
//---------------------------------------------------------------------------
DrgPstatspred *
CStatisticsTest::PdrgpstatspredInteger
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	CStatsPred::EStatsCmpType escmpt,
	INT *piVals,
	ULONG ulVals
	)
{
	GPOS_ASSERT(0 < ulVals);

	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);
	for (ULONG ul = 0; ul < ulVals; ul++)
	{
		pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredPoint(ulColId, escmpt, CTestUtils::PpointInt4(pmp, piVals[ul])));
	}

	return pdrgpstatspred;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredNestedPredDiffCol1
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//		are on different columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredNestedPredDiffCol1
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredConj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_1 <> 3
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptNEq, CTestUtils::PpointInt4(pmp, 3)));

	// predicate col_2 in (15, 20, 22, 24, 31, 39, 42, 46);
	INT rgiVal[] = {15, 20, 22, 24, 31, 39, 42, 46};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 2, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	CStatsPredDisj *pstatspredDisj = GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
	pdrgpstatspredConj->Append(pstatspredDisj);

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredNestedPredDiffCol2
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//		are on different columns. note: the order of the predicates in
//		reversed as in PstatspredNestedPredDiffCol1
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredNestedPredDiffCol2
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredConj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_2 in (15, 20, 22, 24, 31, 39, 42, 46);
	INT rgiVal[] = {15, 20, 22, 24, 31, 39, 42, 46};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 2, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	CStatsPredDisj *pstatspredDisj = GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
	pdrgpstatspredConj->Append(pstatspredDisj);

	// predicate col_1 <> 3
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptNEq, CTestUtils::PpointInt4(pmp, 3)));

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredNestedPredCommonCol1
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//		are on the same columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredNestedPredCommonCol1
	(
	IMemoryPool *pmp
	)
{
	// predicate is col_2 in (39, 31, 24, 22, 46, 20, 42, 15) AND col_2 == 2
	DrgPstatspred *pdrgpstatspredConj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_2 in (15, 20, 22, 24, 31, 39, 42, 46);
	INT rgiVal[] = {15, 20, 22, 24, 31, 39, 42, 46};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 2, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	CStatsPredDisj *pstatspredDisj = GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
	pdrgpstatspredConj->Append(pstatspredDisj);

	// predicate col_2 == 2
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(2, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2)));

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredNestedPredCommonCol2
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//		are on the same columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredNestedPredCommonCol2
	(
	IMemoryPool *pmp
	)
{
	// predicate is col_2 in (2, 39, 31, 24, 22, 46, 20, 42, 15) AND col_2 == 2
	DrgPstatspred *pdrgpstatspredConj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// IN predicate: col_2 in (2, 39, 31, 24, 22, 46, 20, 42, 15);
	INT rgiVal[] = {2, 15, 20, 22, 24, 31, 39, 42, 46};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 2, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	CStatsPredDisj *pstatspredDisj = GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
	pdrgpstatspredConj->Append(pstatspredDisj);

	// predicate col_2 == 2
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(2, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2)));

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredNestedSharedCol
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//		share common columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredNestedSharedCol
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredConj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_1 <> 3
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptNEq, CTestUtils::PpointInt4(pmp, 3)));

	// predicate col_2 in (15, 20, 22, 24, 31, 39, 42, 46) OR (col_1 == 4));

	INT rgiVal[] = {15, 20, 22, 24, 31, 39, 42, 46};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 2, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 4)));

	CStatsPredDisj *pstatspredDisj = GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
	pdrgpstatspredConj->Append(pstatspredDisj);

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisjOverConjSameCol1
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//		share common columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisjOverConjSameCol1
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredConj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_1 = 3 AND col_1 >=3
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 3)));
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptGEq, CTestUtils::PpointInt4(pmp, 3)));

	CStatsPredConj *pstatspredConj = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj);

	// predicate (col_1 = 1);
	DrgPstatspred *pdrgpstatspredDisj = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 1)));
	pdrgpstatspredDisj->Append(pstatspredConj);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisjOverConjSameCol2
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//		share common columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisjOverConjSameCol2
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredConj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_1 <= 5 AND col_1 >=1
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptLEq, CTestUtils::PpointInt4(pmp, 5)));
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptGEq, CTestUtils::PpointInt4(pmp, 1)));

	CStatsPredConj *pstatspredConj = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj);

	// predicate (col_1 = 1);
	DrgPstatspred *pdrgpstatspredDisj = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 1)));
	pdrgpstatspredDisj->Append(pstatspredConj);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisjOverConjSameCol3
//
//	@doc:
//		Create disjunctive predicate over conjunctions on same columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisjOverConjSameCol3
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredDisj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	CWStringDynamic *pstrS = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAABXM="));
	CWStringDynamic *pstrW = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAABXc="));

	// predicate is a == 's' AND b == 2001
	DrgPstatspred *pdrgpstatspredConj1 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(142, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrS, 160588332)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(113, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj1));

	// predicate is a == 's' AND b == 2002
	DrgPstatspred *pdrgpstatspredConj2 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(142, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrS, 160588332)));
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(113, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2002)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj2));

	// predicate is a == 'w' AND b == 2001
	DrgPstatspred *pdrgpstatspredConj3 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(142, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(113, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj3));

	// predicate is a == 'w' AND b == 2002
	DrgPstatspred *pdrgpstatspredConj4 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj4->Append(GPOS_NEW(pmp) CStatsPredPoint(142, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
	pdrgpstatspredConj4->Append(GPOS_NEW(pmp) CStatsPredPoint(113, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2002)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj4));

	GPOS_DELETE(pstrS);
	GPOS_DELETE(pstrW);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisjOverConjSameCol4
//
//	@doc:
//		Create disjunctive predicate over conjunctions on same columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisjOverConjSameCol4
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredDisj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	CWStringDynamic *pstrS = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAABXM="));
	CWStringDynamic *pstrW = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAABXc="));

	// predicate is a == 's' AND b == 2001 AND c > 0
	DrgPstatspred *pdrgpstatspredConj1 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(91, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrS, 160588332)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(61, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(90, CStatsPred::EstatscmptG, CTestUtils::PpointInt4(pmp, 0)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj1));

	// predicate is a == 's' AND b == 2002
	DrgPstatspred *pdrgpstatspredConj2 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(91, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrS, 160588332)));
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(61, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2002)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj2));

	// predicate is a == 'w' AND b == 2001 AND c > 0
	DrgPstatspred *pdrgpstatspredConj3 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(91, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(61, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(90, CStatsPred::EstatscmptG, CTestUtils::PpointInt4(pmp, 0)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj3));

	// predicate is a == 'w' AND b == 2002
	DrgPstatspred *pdrgpstatspredConj4 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj4->Append(GPOS_NEW(pmp) CStatsPredPoint(91, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
	pdrgpstatspredConj4->Append(GPOS_NEW(pmp) CStatsPredPoint(61, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2002)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj4));

	GPOS_DELETE(pstrS);
	GPOS_DELETE(pstrW);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredConj
//
//	@doc:
//		Create conjunctive predicate
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredConj
	(
	IMemoryPool *pmp
	)
{
	CWStringDynamic *pstrW = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAABXc="));

	// predicate is a == 'w' AND b == 2001 AND c > 0
	DrgPstatspred *pdrgpstatspredConj3 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(594, CStatsPred::EstatscmptEq, CCardinalityTestUtils::PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(592, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(593, CStatsPred::EstatscmptG, CTestUtils::PpointInt4(pmp, 0)));

	GPOS_DELETE(pstrW);

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj3);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisjOverConjDifferentCol1
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//		share common columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisjOverConjDifferentCol1
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredConj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_1 = 3 AND col_2 >=3
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 3)));
	pdrgpstatspredConj->Append(GPOS_NEW(pmp) CStatsPredPoint(2, CStatsPred::EstatscmptGEq, CTestUtils::PpointInt4(pmp, 3)));

	CStatsPredConj *pstatspredConj = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj);

	// predicate (col_1 = 1);
	DrgPstatspred *pdrgpstatspredDisj = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 1)));
	pdrgpstatspredDisj->Append(pstatspredConj);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisjOverConjMultipleIdenticalCols
//
//	@doc:
//		Create nested AND and OR predicates where the AND and OR predicates
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisjOverConjMultipleIdenticalCols
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspredConj1 = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_1 = 1 AND col_2 = 1
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 1)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(2, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 1)));

	CStatsPredConj *pstatspredConj1 = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj1);

	DrgPstatspred *pdrgpstatspredConj2 = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// predicate col_1 = 2 AND col_2 = 2
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2)));
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(2, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2)));

	CStatsPredConj *pstatspredConj2 = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj2);
	DrgPstatspred *pdrgpstatspredDisj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	pdrgpstatspredDisj->Append(pstatspredConj1);
	pdrgpstatspredDisj->Append(pstatspredConj2);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredNullableCols
//
//	@doc:
//		Create a filter on a column with null values
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredNullableCols
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);

	pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptLEq, CTestUtils::PpointInt4(pmp, 1)));

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredWithNullConstant
//
//	@doc:
//		Create a point filter where the constant is null
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredWithNullConstant
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);

	pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq,  CTestUtils::PpointInt4NullVal(pmp)));

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredNotNull
//
//	@doc:
//		Create an 'is not null' point filter
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredNotNull
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);

	pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptNEq,  CTestUtils::PpointInt4NullVal(pmp)));

	return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisj1
//
//	@doc:
//		Create an or filter (no duplicate)
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisj1
	(
	IMemoryPool *pmp
	)
{
	// predicate col_1 in (13, 25, 47, 49);
	INT rgiVal[] = {13, 25, 47, 49};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 1, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisj2
//
//	@doc:
//		Create an or filter (one duplicate constant)
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisj2
	(
	IMemoryPool *pmp
	)
{

	// predicate col_1 in (13, 13, 25, 47, 49);
	INT rgiVal[] = {13, 13, 25, 47, 49};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 1, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisj3
//
//	@doc:
//		Create an or filter (multiple duplicate constants)
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisj3
	(
	IMemoryPool *pmp
	)
{
	// predicate col_1 in (13, 25, 47, 47, 47, 49, 13);
	INT rgiVal[] = {13, 25, 47, 47, 47, 49, 13};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 1, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisj4
//
//	@doc:
//		Create an or filter
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisj4
	(
	IMemoryPool *pmp
	)
{
	// the predicate is (x <= 5 or x <= 10 or x <= 13) (domain [0 -- 20])
	INT rgiVal[] = {5, 10, 13};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 1, CStatsPred::EstatscmptLEq, rgiVal, ulVals);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisj5
//
//	@doc:
//		Create an or filter (multiple LEQ)
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisj5
	(
	IMemoryPool *pmp
	)
{
	// the predicate is (x >= 5 or x >= 13) (domain [0 -- 20])
	INT rgiVal[] = {5, 13};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 1, CStatsPred::EstatscmptGEq, rgiVal, ulVals);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisj6
//
//	@doc:
//		Create an or filter
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisj6
	(
	IMemoryPool *pmp
	)
{
	// the predicate is (x > 10 or x < 5) (domain [0 -- 20])
	DrgPstatspred *pdrgpstatspredDisj = GPOS_NEW(pmp) DrgPstatspred(pmp);

	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptG, CTestUtils::PpointInt4(pmp, 10)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptL, CTestUtils::PpointInt4(pmp, 5)));

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisj7
//
//	@doc:
//		Create an or filter
//
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisj7
	(
	IMemoryPool *pmp
	)
{
	// the predicate is (x <= 15 or x >= 5 or x > = 10) (domain [0 -- 20])
	INT rgiVal[] = {5, 10};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 1, CStatsPred::EstatscmptGEq, rgiVal, ulVals);
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptLEq, CTestUtils::PpointInt4(pmp, 15)));

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PstatspredDisj8
//
//	@doc:
//		Create disjunctive predicate on same columns
//---------------------------------------------------------------------------
CStatsPred *
CStatisticsTest::PstatspredDisj8
	(
	IMemoryPool *pmp
	)
{
	// predicate is b = 2001 OR b == 2002
	INT rgiVal[] = {2001, 2002};
	const ULONG ulVals = GPOS_ARRAY_SIZE(rgiVal);
	DrgPstatspred *pdrgpstatspredDisj = PdrgpstatspredInteger(pmp, 61, CStatsPred::EstatscmptEq, rgiVal, ulVals);

	return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Pdrgpstatspred1
//
//	@doc:
//		Create a filter clause
//
//---------------------------------------------------------------------------
DrgPstatspred *
CStatisticsTest::Pdrgpstatspred1
	(
	IMemoryPool *pmp
	)
{
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// col1 = true
	StatsFilterBool(pmp, 1, true, pdrgpstatspred);

	// col2 >= 25 and col2 < 35
	StatsFilterInt4(pmp, 2, 25, 35, pdrgpstatspred);

	return pdrgpstatspred;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Pdrgpstatspred2
//
//	@doc:
//		Create a filter clause
//
//---------------------------------------------------------------------------
DrgPstatspred *
CStatisticsTest::Pdrgpstatspred2
	(
	IMemoryPool *pmp
	)
{
	// contain for filters
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// create int4 filter column 2: [5,15)::int4
	StatsFilterInt4(pmp, 2, 5, 15, pdrgpstatspred);

	// create numeric filter column 3: [1.0, 2.0)::numeric
	CWStringDynamic *pstrLowerNumeric = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAACgAAAQABAA=="));
	CWStringDynamic *pstrUpperNumeric = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAAACgAAAQACAA=="));

	StatsFilterNumeric(pmp, 3, pstrLowerNumeric, pstrUpperNumeric, CDouble(1.0), CDouble(2.0), pdrgpstatspred);

	GPOS_DELETE(pstrLowerNumeric);
	GPOS_DELETE(pstrUpperNumeric);

	// create a date filter column 4: ['01-01-2012', '01-21-2012')::date
	CWStringDynamic *pstrLowerDate = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("HxEAAA=="));
	CWStringDynamic *pstrUpperDate = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("LREAAA=="));
	LINT lLowerDate = LINT(4383) * LINT(INT64_C(86400000000)); // microseconds per day
	LINT lUpperDate = LINT(4397) * LINT(INT64_C(86400000000)); // microseconds per day
	StatsFilterGeneric(pmp, 4, GPDB_DATE, pstrLowerDate, pstrUpperDate, lLowerDate, lUpperDate, pdrgpstatspred);

	GPOS_DELETE(pstrLowerDate);
	GPOS_DELETE(pstrUpperDate);

	// create timestamp filter column 5: ['01-01-2012 00:01:00', '01-01-2012 10:00:00')::timestamp
	CWStringDynamic *pstrLowerTS = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("ACcI7mpYAQA="));
	CWStringDynamic *pstrUpperTS = GPOS_NEW(pmp) CWStringDynamic(pmp, GPOS_WSZ_LIT("AAg5THNYAQA="));
	LINT lLowerTS = LINT(INT64_C(378691260000000)); // microseconds
	LINT lUpperTS = LINT(INT64_C(378727200000000)); // microseconds

	StatsFilterGeneric(pmp, 5, GPDB_TIMESTAMP, pstrLowerTS, pstrUpperTS, lLowerTS, lUpperTS, pdrgpstatspred);

	GPOS_DELETE(pstrLowerTS);
	GPOS_DELETE(pstrUpperTS);

	return pdrgpstatspred;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::StatsFilterInt4
//
//	@doc:
//		Create a stats filter on integer range
//
//---------------------------------------------------------------------------
void
CStatisticsTest::StatsFilterInt4
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	INT iLower,
	INT iUpper,
	DrgPstatspred *pdrgpstatspred
	)
{
	CStatsPredPoint *pstatspred1 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptGEq,
												CTestUtils::PpointInt4(pmp, iLower)
												);

	CStatsPredPoint *pstatspred2 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptL,
												CTestUtils::PpointInt4(pmp, iUpper)
												);

	pdrgpstatspred->Append(pstatspred1);
	pdrgpstatspred->Append(pstatspred2);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::StatsFilterBool
//
//	@doc:
//		Create a stats filter on boolean
//
//---------------------------------------------------------------------------
void
CStatisticsTest::StatsFilterBool
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	BOOL fValue,
	DrgPstatspred *pdrgpstatspred
	)
{
	CStatsPredPoint *pstatspred1 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptEq,
												CTestUtils::PpointBool(pmp, fValue)
												);

	pdrgpstatspred->Append(pstatspred1);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::StatsFilter
//
//	@doc:
//		Create a stats filter on numeric types
//
//---------------------------------------------------------------------------
void
CStatisticsTest::StatsFilterNumeric
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	CWStringDynamic *pstrLowerEncoded,
	CWStringDynamic *pstrUpperEncoded,
	CDouble dValLower,
	CDouble dValUpper,
	DrgPstatspred *pdrgpstatspred
	)
{
	CStatsPredPoint *pstatspred1 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptGEq,
												CCardinalityTestUtils::PpointNumeric(pmp, pstrLowerEncoded, dValLower)
												);

	CStatsPredPoint *pstatspred2 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptL,
												CCardinalityTestUtils::PpointNumeric(pmp, pstrUpperEncoded, dValUpper)
												);

	pdrgpstatspred->Append(pstatspred1);
	pdrgpstatspred->Append(pstatspred2);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::StatsFilterGeneric
//
//	@doc:
//		Create a stats filter on other types
//
//---------------------------------------------------------------------------
void
CStatisticsTest::StatsFilterGeneric
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	OID oid,
	CWStringDynamic *pstrLowerEncoded,
	CWStringDynamic *pstrUpperEncoded,
	LINT lValueLower,
	LINT lValueUpper,
	DrgPstatspred *pdrgpstatspred
	)
{
	CStatsPredPoint *pstatspred1 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptGEq,
												CCardinalityTestUtils::PpointGeneric(pmp, oid, pstrLowerEncoded, lValueLower)
												);

	CStatsPredPoint *pstatspred2 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptL,
												CCardinalityTestUtils::PpointGeneric(pmp, oid, pstrUpperEncoded, lValueUpper)
												);

	pdrgpstatspred->Append(pstatspred1);
	pdrgpstatspred->Append(pstatspred2);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsSelectDerivation
//
//	@doc:
//		Derivation over select query
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsSelectDerivation()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	return
		CTestUtils::EresTranslate
			(
			pmp,
			szQuerySelect,
			szPlanSelect,
			true // ignore mismatch in output dxl due to column id differences
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CStatisticsAccumulateCard
//
//	@doc:
//		Test for accumulating cardinality in disjunctive and conjunctive
//		predicates
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CStatisticsAccumulateCard()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// create hash map from colid -> histogram
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	// array capturing columns for which width information is available
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	const ULONG ulCols = 3;
	for (ULONG ul = 0; ul < ulCols; ul ++)
	{
		// generate histogram of the form [0, 10), [10, 20), [20, 30), [80, 90), [100,100]
		phmulhist->FInsert(GPOS_NEW(pmp) ULONG(ul), CCardinalityTestUtils::PhistExampleInt4(pmp));

		// width for int
		phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ul), GPOS_NEW(pmp) CDouble(4.0));
	}

	CStatistics *pstats = GPOS_NEW(pmp) CStatistics
									(
									pmp,
									phmulhist,
									phmuldoubleWidth,
									CDouble(1000.0) /* dRows */,
									false /* fEmpty() */
									);
	GPOS_TRACE(GPOS_WSZ_LIT("\nOriginal Stats:\n"));
	CCardinalityTestUtils::PrintStats(pmp, pstats);

	// (1)
	// create disjunctive filter
	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredPoint(0, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 5)));
	pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 200)));
	pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredPoint(2, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 200)));
	CStatsPredDisj *pstatspredDisj = GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspred);

	// apply filter and print resulting stats
	CStatistics *pstats1 = pstats->PstatsFilter(pmp, pstatspredDisj, true /* fCapNdvs */);
	CDouble dRows1 = pstats1->DRows();
	GPOS_TRACE(GPOS_WSZ_LIT("\n\nStats after disjunctive filter [Col0=5 OR Col1=200 OR Col2=200]:\n"));
	CCardinalityTestUtils::PrintStats(pmp, pstats1);

	pstatspredDisj->Release();

	// (2)
	// create point filter
	DrgPstatspred *pdrgpstatspred1 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspred1->Append(GPOS_NEW(pmp) CStatsPredPoint(0, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 5)));
	CStatsPredConj *pstatspredConj1 = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred1);

	// apply filter and print resulting stats
	CStatistics *pstats2 = pstats->PstatsFilter(pmp, pstatspredConj1, true /* fCapNdvs */);
	CDouble dRows2 = pstats2->DRows();
	GPOS_TRACE(GPOS_WSZ_LIT("\n\nStats after point filter [Col0=5]:\n"));
	CCardinalityTestUtils::PrintStats(pmp, pstats2);

	pstatspredConj1->Release();

	GPOS_RTL_ASSERT(dRows1 - dRows2 < 10 && "Disjunctive filter and point filter have very different row estimates");

	// (3)
	// create conjunctive filter
	DrgPstatspred *pdrgpstatspred2 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspred2->Append(GPOS_NEW(pmp) CStatsPredPoint(0, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 5)));
	pdrgpstatspred2->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 200)));
	pdrgpstatspred2->Append(GPOS_NEW(pmp) CStatsPredPoint(2, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 200)));

	CStatsPredConj *pstatspredConj2 = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred2);

	// apply filter and print resulting stats
	CStatistics *pstats3 = pstats->PstatsFilter(pmp, pstatspredConj2, true /* fCapNdvs */);
	CDouble dRows3 = pstats3->DRows();
	GPOS_TRACE(GPOS_WSZ_LIT("\n\nStats after conjunctive filter [Col0=5 AND Col1=200 AND Col2=200]:\n"));
	CCardinalityTestUtils::PrintStats(pmp, pstats3);

	pstatspredConj2->Release();
	GPOS_RTL_ASSERT(dRows3 < dRows2  && "Conjunctive filter passes more rows than than point filter");

	// (4)
	// create selective disjunctive filter that pass no rows
	DrgPstatspred *pdrgpstatspred3 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspred3->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 200)));
	pdrgpstatspred3->Append(GPOS_NEW(pmp) CStatsPredPoint(2, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 200)));
	CStatsPredDisj *pstatspredDisj1 = GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspred3);

	// apply filter and print resulting stats
	CStatistics *pstats4 = pstats->PstatsFilter(pmp, pstatspredDisj1, true /* fCapNdvs */);
	CDouble dRows4 = pstats4->DRows();
	GPOS_TRACE(GPOS_WSZ_LIT("\n\nStats after disjunctive filter [Col1=200 OR Col2=200]:\n"));
	CCardinalityTestUtils::PrintStats(pmp, pstats4);

	pstatspredDisj1->Release();

	GPOS_RTL_ASSERT(dRows4 < dRows2  && "Selective disjunctive filter passes more rows than than point filter");

	// (5)
	// create selective conjunctive filter that pass no rows
	DrgPstatspred *pdrgpstatspred4 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspred4->Append(GPOS_NEW(pmp) CStatsPredPoint(0, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 5)));
	pdrgpstatspred4->Append(GPOS_NEW(pmp) CStatsPredPoint(1, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 200)));
	CStatsPredConj *pstatspredConj3 = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred4);

	// apply filter and print resulting stats
	CStatistics *pstats5 = pstats->PstatsFilter(pmp, pstatspredConj3, true /* fCapNdvs */);
	CDouble dRows5 = pstats5->DRows();
	GPOS_TRACE(GPOS_WSZ_LIT("\n\nStats after conjunctive filter [Col0=5 AND Col1=200]:\n"));
	CCardinalityTestUtils::PrintStats(pmp, pstats5);

	pstatspredConj3->Release();

	GPOS_RTL_ASSERT(dRows5 < dRows2  && "Selective conjunctive filter passes more rows than than point filter");

	// clean up
	pstats->Release();
	pstats1->Release();
	pstats2->Release();
	pstats3->Release();
	pstats4->Release();
	pstats5->Release();

	return GPOS_OK;
}

// EOF
