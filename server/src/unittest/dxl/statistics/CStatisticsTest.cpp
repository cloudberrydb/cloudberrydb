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
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"
#include "naucrates/dxl/operators/CDXLDatumStatsDoubleMappable.h"
#include "naucrates/dxl/operators/CDXLDatumStatsLintMappable.h"

#include "unittest/base.h"
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
szMCVSortExpectedFileName = "../data/dxl/statistics/MCV-Sort-Output.xml";

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
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CPointInt4),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CPointBool),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CBucketInt4),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CBucketBool),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CBucketScale),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CBucketDifference),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CHistogramInt4),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CHistogramBool),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsBasic),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsBasicsFromDXL),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsBasicsFromDXLNumeric),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_UnionAll),
		// TODO,  Mar 18 2013 temporarily disabling the test
		// GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsSelectDerivation),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_Skew),
		GPOS_UNITTEST_FUNC_ASSERT(CStatisticsTest::EresUnittest_CHistogramValid),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_Join),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_JoinNDVRemain),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CBucketIntersect),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsFilter),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsFilterConj),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsFilterDisj),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_CStatisticsNestedPred),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_SortInt4MCVs),
		GPOS_UNITTEST_FUNC(CStatisticsTest::EresUnittest_MergeHistMCV),
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
//		CStatisticsTest::EresUnittest_CPointInt4
//
//	@doc:
//		Basic int4 point tests;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CPointInt4()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate integer points
	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 1);
	CPoint *ppoint2 = CTestUtils::PpointInt4(pmp, 2);

	GPOS_RTL_ASSERT_MSG(ppoint1->FEqual(ppoint1), "1 == 1");
	GPOS_RTL_ASSERT_MSG(ppoint1->FLessThan(ppoint2), "1 < 2");
	GPOS_RTL_ASSERT_MSG(ppoint2->FGreaterThan(ppoint1), "2 > 1");
	GPOS_RTL_ASSERT_MSG(ppoint1->FLessThanOrEqual(ppoint2), "1 <= 2");
	GPOS_RTL_ASSERT_MSG(ppoint2->FGreaterThanOrEqual(ppoint2), "2 >= 2");

	CDouble dDistance = ppoint2->DDistance(ppoint1);

	// should be 1.0
	GPOS_RTL_ASSERT_MSG(0.99 < dDistance
						&& dDistance < 1.01, "incorrect distance calculation");

	ppoint1->Release();
	ppoint2->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CPointBool
//
//	@doc:
//		Basic bool point tests;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CPointBool()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate boolean points
	CPoint *ppoint1 = CTestUtils::PpointBool(pmp, true);
	CPoint *ppoint2 = CTestUtils::PpointBool(pmp, false);

	// true == true
	GPOS_RTL_ASSERT_MSG(ppoint1->FEqual(ppoint1), "true must be equal to true");

	// true != false
	GPOS_RTL_ASSERT_MSG(ppoint1->FNotEqual(ppoint2), "true must not be equal to false");

	ppoint1->Release();
	ppoint2->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_JoinNDVRemain
//
//	@doc:
//		Join of histograms with NDVRemain information
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_JoinNDVRemain()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	SHistogramTestCase rghisttc[] =
	{
		{0, 0, false, 0}, // empty histogram
		{10, 100, false, 0},  // distinct values only in buckets
		{0, 0, false, 1000},   // distinct values only in NDVRemain
		{5, 100, false, 500} // distinct values spread in both buckets and NDVRemain
	};

	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	const ULONG ulHist = GPOS_ARRAY_SIZE(rghisttc);
	for (ULONG ul1 = 0; ul1 < ulHist; ul1++)
	{
		SHistogramTestCase elem = rghisttc[ul1];

		ULONG ulBuckets = elem.m_ulBuckets;
		CDouble dNDVPerBucket = elem.m_dNDVPerBucket;
		BOOL fNullFreq = elem.m_fNullFreq;
		CDouble dNDVRemain = elem.m_dNDVRemain;

		CHistogram *phist = PhistInt4Remain(pmp, ulBuckets, dNDVPerBucket, fNullFreq, dNDVRemain);
#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
		phmulhist->FInsert(GPOS_NEW(pmp) ULONG(ul1), phist);
		GPOS_ASSERT(fResult);
	}

	SStatsJoinNDVRemainTestCase rgjoinndvrtc[] =
	{
		// cases where we are joining with an empty histogram
	    // first two columns refer to the histogram entries that are joining
		{0, 0, 0, CDouble(0.0), CDouble(0.0), CDouble(0.0)},
		{0, 1, 0, CDouble(0.0), CDouble(0.0), CDouble(0.0)},
		{0, 2, 0, CDouble(0.0), CDouble(0.0), CDouble(0.0)},
		{0, 3, 0, CDouble(0.0), CDouble(0.0), CDouble(0.0)},

		{1, 0, 0, CDouble(0.0), CDouble(0.0), CDouble(0.0)},
		{2, 0, 0, CDouble(0.0), CDouble(0.0), CDouble(0.0)},
		{3, 0, 0, CDouble(0.0), CDouble(0.0), CDouble(0.0)},

		// cases where one or more input histogram has only buckets and no remaining NDV information
		{1, 1, 10, CDouble(1000.00), CDouble(0.0), CDouble(0.0)},
		{1, 3, 5, CDouble(500.00), CDouble(500.0), CDouble(0.333333)},
		{3, 1, 5, CDouble(500.00), CDouble(500.0), CDouble(0.333333)},

		// cases where for one or more input histogram has only remaining NDV information and no buckets
		{1, 2, 0, CDouble(0.0), CDouble(1000.0), CDouble(1.0)},
		{2, 1, 0, CDouble(0.0), CDouble(1000.0), CDouble(1.0)},
		{2, 2, 0, CDouble(0.0), CDouble(1000.0), CDouble(1.0)},
		{2, 3, 0, CDouble(0.0), CDouble(1000.0), CDouble(1.0)},
		{3, 2, 0, CDouble(0.0), CDouble(1000.0), CDouble(1.0)},

		// cases where both buckets and NDV remain information available for both inputs
		{3, 3, 5, CDouble(500.0), CDouble(500.0), CDouble(0.5)},
	};

	GPOS_RESULT eres = GPOS_OK;
	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgjoinndvrtc);
	for (ULONG ul2 = 0; ul2 < ulTestCases && (GPOS_FAILED != eres); ul2++)
	{
		SStatsJoinNDVRemainTestCase elem = rgjoinndvrtc[ul2];
		ULONG ulColId1 = elem.m_ulCol1;
		ULONG ulColId2 = elem.m_ulCol2;
		CHistogram *phist1 = phmulhist->PtLookup(&ulColId1);
		CHistogram *phist2 = phmulhist->PtLookup(&ulColId2);

		CHistogram *phistJoin = phist1->PhistJoin(pmp, CStatsPred::EstatscmptEq, phist2);

		{
			CAutoTrace at(pmp);
			at.Os() <<  std::endl << "Input Histogram 1" <<  std::endl;
			phist1->OsPrint(at.Os());
			at.Os() << "Input Histogram 2" <<  std::endl;
			phist2->OsPrint(at.Os());
			at.Os() << "Join Histogram" <<  std::endl;
			phistJoin->OsPrint(at.Os());

			phistJoin->DNormalize();

			at.Os() <<  std::endl << "Normalized Join Histogram" <<  std::endl;
			phistJoin->OsPrint(at.Os());
		}

		ULONG ulBucketsJoin = elem.m_ulBucketsJoin;
		CDouble dNDVBucketsJoin = elem.m_dNDVBucketsJoin;
		CDouble dNDVRemainJoin = elem.m_dNDVRemainJoin;
		CDouble dFreqRemainJoin = elem.m_dFreqRemainJoin;

		CDouble dDiffNDVJoin(fabs((dNDVBucketsJoin - CStatisticsUtils::DDistinct(phistJoin->Pdrgpbucket())).DVal()));
		CDouble dDiffNDVRemainJoin(fabs((dNDVRemainJoin - phistJoin->DDistinctRemain()).DVal()));
		CDouble dDiffFreqRemainJoin(fabs((dFreqRemainJoin - phistJoin->DFreqRemain()).DVal()));

		if (phistJoin->UlBuckets() != ulBucketsJoin || (dDiffNDVJoin > CStatistics::DEpsilon)
			|| (dDiffNDVRemainJoin > CStatistics::DEpsilon) || (dDiffFreqRemainJoin > CStatistics::DEpsilon))
		{
			eres = GPOS_FAILED;
		}

		GPOS_DELETE(phistJoin);
	}
	// clean up
	phmulhist->Release();

	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CBucketInt4
//
//	@doc:
//		Basic int4 bucket tests;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CBucketInt4()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate integer points
	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 1);
	CPoint *ppoint2 = CTestUtils::PpointInt4(pmp, 2);
	CPoint *ppoint3 = CTestUtils::PpointInt4(pmp, 3);

	// bucket [1,1]
	CBucket *pbucket1 = Pbucket(pmp, 1, 1, CDouble(1.0), CDouble(1.0));
	Print(pmp, "b1", pbucket1);

	GPOS_RTL_ASSERT_MSG(pbucket1->FContains(ppoint1), "[1,1] must contain 1");
	GPOS_RTL_ASSERT_MSG(CDouble(1.0) == pbucket1->DOverlap(ppoint1),
						"overlap of 1 in [1,1] must 1.0");

	GPOS_RTL_ASSERT_MSG(!pbucket1->FContains(ppoint2), "[1,1] must not contain 2");

	// bucket [1,3)
	CBucket *pbucket2 = Pbucket(pmp, 1, 3, CDouble(1.0), CDouble(10.0));
	Print(pmp, "b2", pbucket2);

	// overlap of [1,2) w.r.t [1,3) should be about 50%
	CDouble dOverlap = pbucket2->DOverlap(ppoint2);
	GPOS_RTL_ASSERT(0.49 <= dOverlap && dOverlap <= 0.51);

	// subsumption
	GPOS_RTL_ASSERT(pbucket1->FSubsumes(pbucket1));
	GPOS_RTL_ASSERT(pbucket2->FSubsumes(pbucket1));

	// width
	CDouble dWidth = pbucket1->DWidth();
	GPOS_RTL_ASSERT(0.99 <= dWidth && dWidth <= 1.01);

	// bucket [1,2] and (2,4)
	CBucket *pbucket3 = Pbucket(pmp, 1, 2, true, true, CDouble(1.0), CDouble(1.0));
	CBucket *pbucket4 = Pbucket(pmp, 2, 4, false, false, CDouble(1.0), CDouble(1.0));

	// point FBefore
	GPOS_RTL_ASSERT_MSG(pbucket4->FBefore(ppoint2), "2 must be before (2,4)");

	// bucket FBefore
	GPOS_RTL_ASSERT_MSG(pbucket3->FBefore(pbucket4), "[1,2] must be before (2,4)");

	ppoint1->Release();
	ppoint2->Release();
	ppoint3->Release();
	GPOS_DELETE(pbucket1);
	GPOS_DELETE(pbucket2);
	GPOS_DELETE(pbucket3);
	GPOS_DELETE(pbucket4);

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CBucketBool
//
//	@doc:
//		Basic BOOL bucket tests;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CBucketBool()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate boolean points
	CPoint *p1 = CTestUtils::PpointBool(pmp, true);
	CPoint *p2 = CTestUtils::PpointBool(pmp, false);

	// bucket for true
	CBucket *pbucket = Pbucket(pmp, true, CDouble(1.0));

	GPOS_RTL_ASSERT_MSG(pbucket->FContains(p1), "true bucket must contain true");
	GPOS_RTL_ASSERT_MSG(CDouble(1.0) == pbucket->DOverlap(p1), "overlap must 1.0");

	GPOS_RTL_ASSERT_MSG(!pbucket->FContains(p2), "true bucket must not contain false");
	GPOS_RTL_ASSERT_MSG(CDouble(0.0) == pbucket->DOverlap(p2), "overlap must 0.0");

	p1->Release();
	p2->Release();

	GPOS_DELETE(pbucket);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_Join
//
//	@doc:
//		Join buckets tests;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_Join()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	SStatsJoinSTestCase rgstatsjointc[] =
	{
		{"../data/dxl/statistics/Join-Statistics-Input.xml", "../data/dxl/statistics/Join-Statistics-Output.xml", false, Pdrgpstatsjoin1},
		{"../data/dxl/statistics/Join-Statistics-Input-Null-Bucket.xml", "../data/dxl/statistics/Join-Statistics-Output-Null-Bucket.xml", false, PdrgpstatsjoinNullableCols},
		{"../data/dxl/statistics/LOJ-Input.xml", "../data/dxl/statistics/LOJ-Output.xml", true, PdrgpstatsjoinNullableCols},
		{"../data/dxl/statistics/Join-Statistics-Input-Only-Nulls.xml", "../data/dxl/statistics/Join-Statistics-Output-Only-Nulls.xml", false, PdrgpstatsjoinNullableCols},
		{"../data/dxl/statistics/Join-Statistics-Input-Only-Nulls.xml", "../data/dxl/statistics/Join-Statistics-Output-LOJ-Only-Nulls.xml", true, PdrgpstatsjoinNullableCols},
		// TODO:  - Sep 13, 2013 re-enable after dDistinct value is fixed in a cleaner way
//		{"../data/dxl/statistics/Join-Statistics-DDistinct-Input.xml", "../data/dxl/statistics/Join-Statistics-DDistinct-Output.xml", false, Pdrgpstatsjoin2},

	};

	const ULONG ulTestCases = GPOS_ARRAY_SIZE(rgstatsjointc);
	for (ULONG ul = 0; ul < ulTestCases; ul++)
	{
		SStatsJoinSTestCase elem = rgstatsjointc[ul];

		// read input/output DXL file
		CHAR *szDXLInput = CDXLUtils::SzRead(pmp, elem.m_szInputFile);
		CHAR *szDXLOutput = CDXLUtils::SzRead(pmp, elem.m_szOutputFile);
		BOOL fLeftOuterJoin = elem.m_fLeftOuterJoin;

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

		// generate the join conditions
		FnPdrgpstatjoin *pf = elem.m_pf;
		GPOS_ASSERT(NULL != pf);
		DrgPstatsjoin *pdrgpstatsjoin = pf(pmp);

		// calculate the output stats
		CStatistics *pstatsOutput = NULL;
		if (fLeftOuterJoin)
		{
			pstatsOutput = pstats1->PstatsLOJ(pmp, pstats2, pdrgpstatsjoin);
		}
		else
		{
			pstatsOutput = pstats1->PstatsInnerJoin(pmp, pstats2, pdrgpstatsjoin);
		}
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
		pdrgpstatsjoin->Release();

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


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Pdrgpstatsjoin1
//
//	@doc:
//		Generate join predicates
//
//---------------------------------------------------------------------------
DrgPstatsjoin *
CStatisticsTest::Pdrgpstatsjoin1
	(
	IMemoryPool *pmp
	)
{
	DrgPstatsjoin *pdrgpstatsjoin = GPOS_NEW(pmp) DrgPstatsjoin(pmp);
	pdrgpstatsjoin->Append(GPOS_NEW(pmp) CStatisticsJoin(16, CStatsPred::EstatscmptEq, 32));
	pdrgpstatsjoin->Append(GPOS_NEW(pmp) CStatisticsJoin(0, CStatsPred::EstatscmptEq, 31));
	pdrgpstatsjoin->Append(GPOS_NEW(pmp) CStatisticsJoin(54, CStatsPred::EstatscmptEq, 32));
	pdrgpstatsjoin->Append(GPOS_NEW(pmp) CStatisticsJoin(53, CStatsPred::EstatscmptEq, 31));

	return pdrgpstatsjoin;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PdrgpstatsjoinNullableCols
//
//	@doc:
//		Generate join predicate over columns that contain null values
//
//---------------------------------------------------------------------------
DrgPstatsjoin *
CStatisticsTest::PdrgpstatsjoinNullableCols
	(
	IMemoryPool *pmp
	)
{
	DrgPstatsjoin *pdrgpstatsjoin = GPOS_NEW(pmp) DrgPstatsjoin(pmp);
	pdrgpstatsjoin->Append(GPOS_NEW(pmp) CStatisticsJoin(1, CStatsPred::EstatscmptEq, 2));

	return pdrgpstatsjoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Pdrgpstatsjoin2
//
//	@doc:
//		Generate join predicates
//
//---------------------------------------------------------------------------
DrgPstatsjoin *
CStatisticsTest::Pdrgpstatsjoin2
	(
	IMemoryPool *pmp
	)
{
	DrgPstatsjoin *pdrgpstatsjoin = GPOS_NEW(pmp) DrgPstatsjoin(pmp);
	pdrgpstatsjoin->Append(GPOS_NEW(pmp) CStatisticsJoin(0, CStatsPred::EstatscmptEq, 8));

	return pdrgpstatsjoin;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CBucketIntersect
//
//	@doc:
//		Intersection of buckets
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CBucketIntersect()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	SBucketsIntersectTestElem rgBucketsIntersectTestElem[] =
		{
			{7, 203, true, true, 3, 213, true, true, true, 7, 203, true, true}, // overlaps
			{3, 213, true, true, 7, 203, true, true, true, 7, 203, true, true}, // same as above but reversed
			{13, 103, true, true, 2, 98, true, true, true, 13,  98, true, true,}, // subsumes
			{2, 99, true, true, 13, 103, true, true,  true, 13, 99, true, true}, // same as above but reversed
			{0, 5, true, true, 10, 15, false, true, false, -1 , -1 , false, false}, // negative
			{10, 15, true, true, 0, 5, false, true, false, -1 , -1 , false, false}, // same as above but reversed
			{0, 5, true, true, 5, 10, true, true, true, 5 , 5 , true, true}, // ub of one bucket is the same as lb of the other and both bounds are closed
			{5, 10, true, true, 0, 5, true, true, true, 5 , 5 , true, true}, // same as above but reversed
			{0, 5, true, true, 5, 10, false, true, false, -1 , -1 , false, false}, // ub of one bucket is the same as lb of the other but closing criteria are different
			{5, 10, false, true, 0, 5, true, true, false, -1 , -1 , false, false}, // same as above but reversed
			{0, 5, true, true, 0, 5, false, true, true, 0, 5, false, true}, // exact match but only differ in closure of lb
			{0, 5, true, true, 0, 5, true, true, true, 0, 5, true, true}, // exact match with all bounds closed
			{0, 5, true, false, 0, 5, true, false, true, 0, 5, true, false}, // exact match with ubs open
			{0, 5, false, false, 0, 5, true, false, true, 0, 5, false, false}, // exact match with lbs differ in closure
			{0, 5, true, true, 0, 5, true, false, true, 0, 5, true, false}, // exact match with ubs differ in closure
		};

	const ULONG ulLen = GPOS_ARRAY_SIZE(rgBucketsIntersectTestElem);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CBucket *pbucket1 = Pbucket
								(
								pmp,
								rgBucketsIntersectTestElem[ul].m_iLb1,
								rgBucketsIntersectTestElem[ul].m_iUb1,
								rgBucketsIntersectTestElem[ul].m_fLb1Closed,
								rgBucketsIntersectTestElem[ul].m_fUb1Closed,
								CDouble(0.1),
								CDouble(100.0)
								);

		CBucket *pbucket2 = Pbucket
								(
								pmp,
								rgBucketsIntersectTestElem[ul].m_iLb2,
								rgBucketsIntersectTestElem[ul].m_iUb2,
								rgBucketsIntersectTestElem[ul].m_fLb2Closed,
								rgBucketsIntersectTestElem[ul].m_fUb2Closed,
								CDouble(0.1),
								CDouble(100.0)
								);

		BOOL fResult = pbucket1->FIntersects(pbucket2);

		GPOS_RESULT eres = GPOS_FAILED;
		if (rgBucketsIntersectTestElem[ul].fIntersect == fResult)
		{
			eres = GPOS_OK;
		}

		if (true == fResult)
		{
			CDouble dDummy1(0.0);
			CDouble dDummy2(0.0);
			CBucket *pbucketOuput = pbucket1->PbucketIntersect(pmp, pbucket2, &dDummy1, &dDummy2);
			CBucket *pbucketExpected = Pbucket
											(
											pmp,
											rgBucketsIntersectTestElem[ul].m_iLbOutput,
											rgBucketsIntersectTestElem[ul].m_iUbOutput,
											rgBucketsIntersectTestElem[ul].m_fLbOutputClosed,
											rgBucketsIntersectTestElem[ul].m_fUbOutputClosed,
											CDouble(0.1),
											CDouble(100.0)
											);

			BOOL fMatch = CStatisticsTest::FMatchBucketBoundary(pbucketOuput, pbucketExpected);

			if (!fMatch)
			{
				eres = GPOS_FAILED;

				CWStringDynamic str(pmp);
				COstreamString oss(&str);

				pbucketOuput->OsPrint(oss);
				oss << std::endl;
				pbucketExpected->OsPrint(oss);
				oss << std::endl;
				GPOS_TRACE(str.Wsz());

			}

			GPOS_DELETE(pbucketExpected);
			GPOS_DELETE(pbucketOuput);
		}
		// clean up
		GPOS_DELETE(pbucket1);
		GPOS_DELETE(pbucket2);

		if (GPOS_OK != eres)
		{
			return eres;
		}
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::FMatchBucketBoundary
//
//	@doc:
//		Do the bucket boundaries match
//
//---------------------------------------------------------------------------
BOOL
CStatisticsTest::FMatchBucketBoundary
	(
	CBucket *pbucket1,
	CBucket *pbucket2
	)
{
	GPOS_ASSERT(NULL != pbucket1);
	GPOS_ASSERT(NULL != pbucket2);

	if (pbucket1->FLowerClosed() != pbucket2->FLowerClosed())
	{
		return false;
	}

	if (pbucket1->FUpperClosed() != pbucket2->FUpperClosed())
	{
		return false;
	}

	if (pbucket1->PpLower()->FEqual(pbucket2->PpLower()) && pbucket1->PpUpper()->FEqual(pbucket2->PpUpper()))
	{
		return true;
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CBucketScale
//
//	@doc:
//		Scaling a bucket
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CBucketScale()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate integer point
	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 10);

	// bucket [1,100)
	CBucket *pbucket1 = Pbucket(pmp, 1, 100, CDouble(0.5), CDouble(20.0));

	CBucket *pbucket2 = pbucket1->PbucketScaleUpper(pmp, ppoint1, false /* fIncludeUpper */);

	// new bucket [1, 10) must not contain 10
	GPOS_RTL_ASSERT(!pbucket2->FContains(ppoint1));

	// new bucket [1, 10) must contain 9
	CPoint *ppoint2 = CTestUtils::PpointInt4(pmp, 9);
	GPOS_RTL_ASSERT(pbucket2->FContains(ppoint2));
	ppoint2->Release();

	// new bucket's frequency and distinct values must be lesser than original
	GPOS_RTL_ASSERT(pbucket2->DFrequency() < pbucket1->DFrequency());
	GPOS_RTL_ASSERT(pbucket2->DDistinct() < pbucket1->DDistinct());

	// scale lower
	CBucket *pbucket3 = pbucket1->PbucketScaleLower(pmp, ppoint1, true /* fIncludeLower */);
	GPOS_RTL_ASSERT(pbucket3->FContains(ppoint1));

	// scale to a singleton
	CBucket *pbucket4 = pbucket1->PbucketSingleton(pmp, ppoint1);
	GPOS_RTL_ASSERT(pbucket4->DDistinct() < 2.0);

	// clean up
	ppoint1->Release();
	GPOS_DELETE(pbucket1);
	GPOS_DELETE(pbucket2);
	GPOS_DELETE(pbucket3);
	GPOS_DELETE(pbucket4);

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CBucketDifference
//
//	@doc:
//		Difference op
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CBucketDifference()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// bucket [1,100)
	CBucket *pbucket1 = Pbucket(pmp, 1, 100, CDouble(1.0), CDouble(1.0));

	// bucket [50,75)
	CBucket *pbucket2 = Pbucket(pmp, 50, 60, CDouble(1.0), CDouble(1.0));

	// bucket [200, 300)
	CBucket *pbucket3 = Pbucket(pmp, 200, 300, CDouble(1.0), CDouble(1.0));

	CBucket *pbucket4 = NULL;
	CBucket *pbucket5 = NULL;
	pbucket1->Difference(pmp, pbucket2, &pbucket4, &pbucket5);
	GPOS_RTL_ASSERT(NULL != pbucket4);
	GPOS_RTL_ASSERT(NULL != pbucket5);
	Print(pmp, "pbucket4", pbucket4);
	Print(pmp, "pbucket5", pbucket4);

	CBucket *pbucket6 = NULL;
	CBucket *pbucket7 = NULL;
	pbucket1->Difference(pmp, pbucket3, &pbucket6, &pbucket7);
	GPOS_RTL_ASSERT(NULL != pbucket6);
	GPOS_RTL_ASSERT(NULL == pbucket7);
	Print(pmp, "pbucket6", pbucket6);

	GPOS_DELETE(pbucket1);
	GPOS_DELETE(pbucket2);
	GPOS_DELETE(pbucket3);
	GPOS_DELETE(pbucket4);
	GPOS_DELETE(pbucket5);
	GPOS_DELETE(pbucket6);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PpointNumeric
//
//	@doc:
//		Create a point from a encoded value of numeric datatype
//
//---------------------------------------------------------------------------
CPoint *
CStatisticsTest::PpointNumeric
	(
	IMemoryPool *pmp,
	CWStringDynamic *pstrEncodedValue,
	CDouble dValue
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidNumeric);
	const IMDType *pmdtype = pmda->Pmdtype(pmdid);

	ULONG ulbaSize = 0;
	BYTE *pba = CDXLUtils::PByteArrayFromStr(pmp, pstrEncodedValue, &ulbaSize);

	CDXLDatumStatsDoubleMappable *pdxldatum = GPOS_NEW(pmp) CDXLDatumStatsDoubleMappable
											(
											pmp,
											pmdid,
											pmdtype->FByValue() /*fConstByVal*/,
											false /*fConstNull*/,
											pba,
											ulbaSize,
											dValue
											);

	IDatum *pdatum = pmdtype->Pdatum(pmp, pdxldatum);
	CPoint *ppoint = GPOS_NEW(pmp) CPoint(pdatum);
	pdxldatum->Release();

	return ppoint;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PpointGeneric
//
//	@doc:
//		Create a point from an encoded value of specific datatype
//
//---------------------------------------------------------------------------
CPoint *
CStatisticsTest::PpointGeneric
	(
	IMemoryPool *pmp,
	OID oid,
	CWStringDynamic *pstrEncodedValue,
	LINT lValue
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	IMDId *pmdid = GPOS_NEW(pmp) CMDIdGPDB(oid);
	IDatum *pdatum = CTestUtils::PdatumGeneric(pmp, pmda, pmdid, pstrEncodedValue, lValue);
	CPoint *ppoint = GPOS_NEW(pmp) CPoint(pdatum);

	return ppoint;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Pbucket
//
//	@doc:
//		Create a bucket
//
//---------------------------------------------------------------------------
CBucket *
CStatisticsTest::Pbucket
	(
	IMemoryPool *pmp,
	INT iLower,
	INT iUpper,
	CDouble dFrequency,
	CDouble dDistinct
	)
{
	CPoint *ppLower = CTestUtils::PpointInt4(pmp, iLower);
	CPoint *ppUpper = CTestUtils::PpointInt4(pmp, iUpper);

	BOOL fUpperClosed = false;
	if (ppLower->FEqual(ppUpper))
	{
		fUpperClosed = true;
	}
	CBucket *pbucket = GPOS_NEW(pmp) CBucket
									(
									ppLower,
									ppUpper,
									true /* fLowerClosed */,
									fUpperClosed,
									dFrequency,
									dDistinct
									);

	return pbucket;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Pbucket
//
//	@doc:
//		Create a bucket
//
//---------------------------------------------------------------------------
CBucket *
CStatisticsTest::Pbucket
	(
	IMemoryPool *pmp,
	INT iLower,
	INT iUpper,
	BOOL fLowerClosed,
	BOOL fUpperClosed,
	CDouble dFrequency,
	CDouble dDistinct
	)
{
	CPoint *ppLower = CTestUtils::PpointInt4(pmp, iLower);
	CPoint *ppUpper = CTestUtils::PpointInt4(pmp, iUpper);

	CBucket *pbucket = GPOS_NEW(pmp) CBucket
									(
									ppLower,
									ppUpper,
									fLowerClosed,
									fUpperClosed,
									dFrequency,
									dDistinct
									);

	return pbucket;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Pbucket
//
//	@doc:
//		Create a bucket
//
//---------------------------------------------------------------------------
CBucket *
CStatisticsTest::Pbucket
	(
	IMemoryPool *pmp,
	BOOL fValue,
	CDouble dFrequency
	)
{
	CPoint *ppLower = CTestUtils::PpointBool(pmp, fValue);

	// lower bound is also upper bound
	ppLower->AddRef();
	CBucket *pbucket = GPOS_NEW(pmp) CBucket
									(
									ppLower,
									ppLower,
									true /* fClosedUpper */,
									true /* fClosedUpper */,
									dFrequency,
									1.0
									);

	return pbucket;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CHistogramInt4
//
//	@doc:
//		Histogram of int4
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CHistogramInt4()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// original histogram
	CHistogram *phist = PhistExampleInt4(pmp);
	Print(pmp, "phist", phist);

	// test edge case of PbucketGreaterThan
	CPoint *ppoint0 = CTestUtils::PpointInt4(pmp, 9);
	CHistogram *phist0 = phist->PhistFilter(pmp, CStatsPred::EstatscmptG, ppoint0);
	Print(pmp, "phist0", phist0);
	GPOS_RTL_ASSERT(phist0->UlBuckets() == 9);

	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 35);
	CHistogram *phist1 = phist->PhistFilter(pmp, CStatsPred::EstatscmptL, ppoint1);
	Print(pmp, "phist1", phist1);
	GPOS_RTL_ASSERT(phist1->UlBuckets() == 4);

	// edge case where point is equal to upper bound
	CPoint *ppoint2 = CTestUtils::PpointInt4(pmp, 50);
	CHistogram *phist2 = phist->PhistFilter(pmp, CStatsPred::EstatscmptL,ppoint2);
	Print(pmp, "phist2", phist2);
	GPOS_RTL_ASSERT(phist2->UlBuckets() == 5);

	// equality check
	CPoint *ppoint3 = CTestUtils::PpointInt4(pmp, 100);
	CHistogram *phist3 = phist->PhistFilter(pmp, CStatsPred::EstatscmptEq, ppoint3);
	Print(pmp, "phist3", phist3);
	GPOS_RTL_ASSERT(phist3->UlBuckets() == 1);

	// normalized output after filter
	CPoint *ppoint4 = CTestUtils::PpointInt4(pmp, 100);
	CDouble dScaleFactor(0.0);
	CHistogram *phist4 = phist->PhistFilterNormalized(pmp, CStatsPred::EstatscmptEq, ppoint4, &dScaleFactor);
	Print(pmp, "phist4", phist4);
	GPOS_RTL_ASSERT(phist4->FValid());

	// lasj
	CHistogram *phist5 = phist->PhistLASJ(pmp, CStatsPred::EstatscmptEq, phist2);
	Print(pmp, "phist5", phist5);
	GPOS_RTL_ASSERT(phist5->UlBuckets() == 5);

	// inequality check
	CHistogram *phist6 = phist->PhistFilter(pmp, CStatsPred::EstatscmptNEq, ppoint2);
	Print(pmp, "phist6", phist6);
	GPOS_RTL_ASSERT(phist6->UlBuckets() == 10);

	// histogram with null fraction and remaining tuples
	CHistogram *phist7 = PhistExampleInt4Remain(pmp);
	Print(pmp, "phist7", phist7);
	CPoint *ppoint5 = CTestUtils::PpointInt4(pmp, 20);

	// equality check, hitting remaining tuples
	CHistogram *phist8 = phist7->PhistFilter(pmp, CStatsPred::EstatscmptEq, ppoint3);
	GPOS_RTL_ASSERT(fabs((phist8->DFrequency() - 0.2).DVal()) < CStatistics::DEpsilon);
	GPOS_RTL_ASSERT(fabs((phist8->DDistinct() - 1.0).DVal()) < CStatistics::DEpsilon);

	// greater than, hitting remaining tuples
	CHistogram *phist9 = phist7->PhistFilter(pmp, CStatsPred::EstatscmptG, ppoint1);
	Print(pmp, "phist9", phist9);
	GPOS_RTL_ASSERT(fabs((phist9->DFrequency() - 0.26).DVal()) < CStatistics::DEpsilon);
	GPOS_RTL_ASSERT(fabs((phist9->DDistinct() - 1.8).DVal()) < CStatistics::DEpsilon);

	// equality join, hitting remaining tuples
	CHistogram *phist10 = phist7->PhistJoin(pmp, CStatsPred::EstatscmptEq, phist7);
	GPOS_RTL_ASSERT(phist10->UlBuckets() == 5);
	GPOS_RTL_ASSERT(fabs((phist10->DDistinctRemain() - 2.0).DVal()) < CStatistics::DEpsilon);
	GPOS_RTL_ASSERT(fabs((phist10->DFreqRemain() - 0.08).DVal()) < CStatistics::DEpsilon);

	// clean up
	ppoint0->Release();
	ppoint1->Release();
	ppoint2->Release();
	ppoint3->Release();
	ppoint4->Release();
	ppoint5->Release();
	GPOS_DELETE(phist);
	GPOS_DELETE(phist0);
	GPOS_DELETE(phist1);
	GPOS_DELETE(phist2);
	GPOS_DELETE(phist3);
	GPOS_DELETE(phist4);
	GPOS_DELETE(phist5);
	GPOS_DELETE(phist6);
	GPOS_DELETE(phist7);
	GPOS_DELETE(phist8);
	GPOS_DELETE(phist9);
	GPOS_DELETE(phist10);

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CHistogramBool
//
//	@doc:
//		Histogram on bool
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CHistogramBool()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate histogram of the form [false, false), [true,true)
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	CBucket *pbucketFalse = Pbucket(pmp, false, 0.1);
	CBucket *pbucketTrue = Pbucket(pmp, false, 0.9);
	pdrgppbucket->Append(pbucketFalse);
	pdrgppbucket->Append(pbucketTrue);
	CHistogram *phist =  GPOS_NEW(pmp) CHistogram(pdrgppbucket);

	// equality check
	CPoint *ppoint1 = CTestUtils::PpointBool(pmp, false);
	CDouble dScaleFactor(0.0);
	CHistogram *phist1 = phist->PhistFilterNormalized(pmp, CStatsPred::EstatscmptEq, ppoint1, &dScaleFactor);
	Print(pmp, "phist1", phist1);
	GPOS_RTL_ASSERT(phist1->UlBuckets() == 1);

	// clean up
	ppoint1->Release();
	GPOS_DELETE(phist);
	GPOS_DELETE(phist1);

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Print
//
//	@doc:
//		Dump bucket
//
//---------------------------------------------------------------------------
void
CStatisticsTest::Print
	(
	IMemoryPool *pmp,
	const char *pcPrefix,
	const CBucket *pbucket
	)
{
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	oss << pcPrefix << " = ";
	pbucket->OsPrint(oss);
	oss << std::endl;
	GPOS_TRACE(str.Wsz());
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::Print
//
//	@doc:
//		Dump histogram output
//
//---------------------------------------------------------------------------
void
CStatisticsTest::Print
	(
	IMemoryPool *pmp,
	const char *pcPrefix,
	const CHistogram *phist
	)
{
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	oss << pcPrefix << " = ";
	phist->OsPrint(oss);
	oss << std::endl;
	GPOS_TRACE(str.Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PhistExampleInt4
//
//	@doc:
//		Generates example int histogram
//
//---------------------------------------------------------------------------
CHistogram*
CStatisticsTest::PhistExampleInt4
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
		CDouble dDistinct(4.0);
		CBucket *pbucket = Pbucket(pmp, iLower, iUpper, dFrequency, dDistinct);
		pdrgppbucket->Append(pbucket);
	}

	// add an additional singleton bucket [100, 100]
	pdrgppbucket->Append(Pbucket(pmp, 100, 100, 0.1, 1.0));

	return  GPOS_NEW(pmp) CHistogram(pdrgppbucket);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PhistExampleInt4Remain
//
//	@doc:
//		Generates example int histogram having tuples not covered by buckets,
//		including null fraction and nDistinctRemain
//
//---------------------------------------------------------------------------
CHistogram*
CStatisticsTest::PhistExampleInt4Remain
	(
	IMemoryPool *pmp
	)
{
	// generate histogram of the form [0, 0], [10, 10], [20, 20] ...
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	for (ULONG ulIdx = 0; ulIdx < 5; ulIdx++)
	{
		INT iLower = INT(ulIdx * 10);
		INT iUpper = iLower;
		CDouble dFrequency(0.1);
		CDouble dDistinct(1.0);
		CBucket *pbucket = Pbucket(pmp, iLower, iUpper, dFrequency, dDistinct);
		pdrgppbucket->Append(pbucket);
	}

	return GPOS_NEW(pmp) CHistogram(pdrgppbucket, true, 0.1 /*dNullFreq*/, 2.0 /*dDistinctRemain*/, 0.4 /*dFreqRemain*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PhistInt4Remain
//
//	@doc:
//		Generate int histogram based on the NDV and bucket information provided
//
//---------------------------------------------------------------------------
CHistogram*
CStatisticsTest::PhistInt4Remain
	(
	IMemoryPool *pmp,
	ULONG ulBuckets,
	CDouble dNDVPerBucket,
	BOOL fNullFreq,
	CDouble dNDVRemain
	)
{
	// generate histogram of the form [0, 100), [100, 200), [200, 300) ...
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	for (ULONG ulIdx = 0; ulIdx < ulBuckets; ulIdx++)
	{
		INT iLower = INT(ulIdx * 100);
		INT iUpper = INT((ulIdx + 1) * 100);
		CDouble dFrequency(0.1);
		CDouble dDistinct = dNDVPerBucket;
		CBucket *pbucket = Pbucket(pmp, iLower, iUpper, dFrequency, dDistinct);
		pdrgppbucket->Append(pbucket);
	}

	CDouble dFreq = CStatisticsUtils::DFrequency(pdrgppbucket);
	CDouble dNullFreq(0.0);
	if (fNullFreq && 1 > dFreq)
	{
		dNullFreq = 0.1;
		dFreq = dFreq + dNullFreq;
	}

	CDouble dFreqRemain = (1 - dFreq);
	if (dFreqRemain < CStatistics::DEpsilon || dNDVRemain < CStatistics::DEpsilon)
	{
		dFreqRemain = CDouble(0.0);
	}

	return GPOS_NEW(pmp) CHistogram(pdrgppbucket, true, dNullFreq, dNDVRemain, dFreqRemain);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PhistExampleInt4Dim
//
//	@doc:
//		Generates example int histogram corresponding to dimension table
//
//---------------------------------------------------------------------------
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
		CBucket *pbucket = Pbucket(pmp, iLower, iUpper, dFrequency, dDistinct);
		pdrgppbucket->Append(pbucket);
	}

	return  GPOS_NEW(pmp) CHistogram(pdrgppbucket);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::PhistExampleBool
//
//	@doc:
//		Generates example bool histogram
//
//---------------------------------------------------------------------------
CHistogram*
CStatisticsTest::PhistExampleBool
	(
	IMemoryPool *pmp
	)
{
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	CBucket *pbucketFalse = Pbucket(pmp, false, 0.1);
	CBucket *pbucketTrue = Pbucket(pmp, true, 0.2);
	pdrgppbucket->Append(pbucketFalse);
	pdrgppbucket->Append(pbucketTrue);
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
													PpointNumeric(pmp, pstrNumeric, statsCmpValElem.m_dVal)
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
	CDouble dRowsInput = pstatsInput->DRows();

	GPOS_TRACE(GPOS_WSZ_LIT("Statistics before"));
	CStatisticsTest::Print(pmp, pstatsInput);

	CStatistics *pstatsOutput = pstatsInput->PstatsFilter(pmp, pstatspred, true /* fCapNdvs */);

	GPOS_TRACE(GPOS_WSZ_LIT("Statistics after"));
	CStatisticsTest::Print(pmp, pstatsOutput);

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
		CStatisticsTest::Print(pmp, pstatsOutput2);

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
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(1), PhistExampleBool(pmp));

	// generate int histogram for column 2
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(2), PhistExampleInt4(pmp));

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
	Print(pmp, pstats);

	// create a filter: column 1: [25,45), column 2: [true, true)
	DrgPstatspred *pdrgpstatspred = Pdrgpstatspred1(pmp);

	CStatsPredConj *pstatspred = GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred);
	CStatistics *pstats1 = pstats->PstatsFilter(pmp, pstatspred, true /* fCapNdvs */);
	pstats1->DRows();

	GPOS_TRACE(GPOS_WSZ_LIT("pstats1 after filter"));

	// after stats
	Print(pmp, pstats1);

	// create another statistics structure with a single int4 column with id 10
	HMUlHist *phmulhist2 = GPOS_NEW(pmp) HMUlHist(pmp);
	phmulhist2->FInsert(GPOS_NEW(pmp) ULONG(10), PhistExampleInt4Dim(pmp));

	HMUlDouble *phmuldoubleWidth2 = GPOS_NEW(pmp) HMUlDouble(pmp);
	phmuldoubleWidth2->FInsert(GPOS_NEW(pmp) ULONG(10), GPOS_NEW(pmp) CDouble(4.0));

	CStatistics *pstats2 = GPOS_NEW(pmp) CStatistics(pmp, phmulhist2, phmuldoubleWidth2, 100.0 /* dRows */, false /* fEmpty */);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats2"));
	Print(pmp, pstats2);

	// join pstats with pstats2
	CStatisticsJoin *pstatsjoin = GPOS_NEW(pmp) CStatisticsJoin(2, CStatsPred::EstatscmptEq, 10);
	DrgPstatsjoin *pdrgpstatsjoin = GPOS_NEW(pmp) DrgPstatsjoin(pmp);
	pdrgpstatsjoin->Append(pstatsjoin);
	CStatistics *pstats3 = pstats->PstatsInnerJoin(pmp, pstats2, pdrgpstatsjoin);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats3 = pstats JOIN pstats2 on (col2 = col10)"));
	// after stats
	Print(pmp, pstats3);

	// group by pstats on columns 1 and 2
	DrgPul *pdrgpulGC = GPOS_NEW(pmp) DrgPul(pmp);
	pdrgpulGC->Append(GPOS_NEW(pmp) ULONG(1));
	pdrgpulGC->Append(GPOS_NEW(pmp) ULONG(2));

	DrgPul *pdrgpulAgg = GPOS_NEW(pmp) DrgPul(pmp);
	CStatistics *pstats4 = pstats->PstatsGroupBy(pmp, pdrgpulGC, pdrgpulAgg, NULL /*pbsKeys*/);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats4 = pstats group by"));
	Print(pmp, pstats4);

	// LASJ stats
	CStatistics *pstats5 = pstats->PstatsLASJoin(pmp, pstats2, pdrgpstatsjoin, true /* fIgnoreLasjHistComputation */);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats5 = pstats LASJ pstats2 on (col2 = col10)"));
	Print(pmp, pstats5);

	// union all
	DrgPul *pdrgpulColIds = GPOS_NEW(pmp) DrgPul(pmp);
	pdrgpulColIds->Append(GPOS_NEW(pmp) ULONG(1));
	pdrgpulColIds->Append(GPOS_NEW(pmp) ULONG(2));
	pdrgpulColIds->AddRef();
	pdrgpulColIds->AddRef();
	pdrgpulColIds->AddRef();

	CStatistics *pstats6 = pstats->PstatsUnionAll(pmp, pstats, pdrgpulColIds, pdrgpulColIds, pdrgpulColIds);

	GPOS_TRACE(GPOS_WSZ_LIT("pstats6 = pstats1 union all pstats1"));
	Print(pmp, pstats6);

	CStatistics *pstats7 = pstats->PstatsLimit(pmp, CDouble(4.0));

	GPOS_TRACE(GPOS_WSZ_LIT("pstats7 = pstats limit 4"));
	Print(pmp, pstats7);

	pstats->Release();
	pstats1->Release();
	pstats2->Release();
	pstats3->Release();
	pstats4->Release();
	pstats5->Release();
	pstats6->Release();
	pstats7->Release();
	pstatspred->Release();
	pdrgpstatsjoin->Release();
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
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(142, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrS, 160588332)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(113, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj1));

	// predicate is a == 's' AND b == 2002
	DrgPstatspred *pdrgpstatspredConj2 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(142, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrS, 160588332)));
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(113, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2002)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj2));

	// predicate is a == 'w' AND b == 2001
	DrgPstatspred *pdrgpstatspredConj3 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(142, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(113, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj3));

	// predicate is a == 'w' AND b == 2002
	DrgPstatspred *pdrgpstatspredConj4 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj4->Append(GPOS_NEW(pmp) CStatsPredPoint(142, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
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
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(91, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrS, 160588332)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(61, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(90, CStatsPred::EstatscmptG, CTestUtils::PpointInt4(pmp, 0)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj1));

	// predicate is a == 's' AND b == 2002
	DrgPstatspred *pdrgpstatspredConj2 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(91, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrS, 160588332)));
	pdrgpstatspredConj2->Append(GPOS_NEW(pmp) CStatsPredPoint(61, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2002)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj2));

	// predicate is a == 'w' AND b == 2001 AND c > 0
	DrgPstatspred *pdrgpstatspredConj3 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(91, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(61, CStatsPred::EstatscmptEq, CTestUtils::PpointInt4(pmp, 2001)));
	pdrgpstatspredConj1->Append(GPOS_NEW(pmp) CStatsPredPoint(90, CStatsPred::EstatscmptG, CTestUtils::PpointInt4(pmp, 0)));
	pdrgpstatspredDisj->Append(GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspredConj3));

	// predicate is a == 'w' AND b == 2002
	DrgPstatspred *pdrgpstatspredConj4 = GPOS_NEW(pmp) DrgPstatspred(pmp);
	pdrgpstatspredConj4->Append(GPOS_NEW(pmp) CStatsPredPoint(91, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
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
	pdrgpstatspredConj3->Append(GPOS_NEW(pmp) CStatsPredPoint(594, CStatsPred::EstatscmptEq, PpointGeneric(pmp, GPDB_TEXT, pstrW, 160621100)));
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
//		CStatisticsTest::Print
//
//	@doc:
//		Dump statistics output
//
//---------------------------------------------------------------------------
void
CStatisticsTest::Print
	(
	IMemoryPool *pmp,
	const CStatistics *pstats
	)
{
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	oss << "Statistics = ";
	pstats->OsPrint(oss);
	oss << std::endl;
	GPOS_TRACE(str.Wsz());

}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_CHistogramValid
//
//	@doc:
//		Check for well-formed histogram. Expected to hit an assert.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_CHistogramValid()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);

	// generate histogram of the form [0, 10), [9, 20)
	CBucket *pbucket1 = Pbucket(pmp, 0, 10, 0.1, 2.0);
	pdrgppbucket->Append(pbucket1);
	CBucket *pbucket2 = Pbucket(pmp, 9, 20, 0.1, 2.0);
	pdrgppbucket->Append(pbucket2);

	// original histogram
	CHistogram *phist =  GPOS_NEW(pmp) CHistogram(pdrgppbucket);

	// create an auto object
	CAutoP<CHistogram> ahist;
	ahist = phist;

	GPOS_RTL_ASSERT(phist->FValid() && "Histogram must be well formed");

	return GPOS_FAILED;
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
												PpointNumeric(pmp, pstrLowerEncoded, dValLower)
												);

	CStatsPredPoint *pstatspred2 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptL,
												PpointNumeric(pmp, pstrUpperEncoded, dValUpper)
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
												PpointGeneric(pmp, oid, pstrLowerEncoded, lValueLower)
												);

	CStatsPredPoint *pstatspred2 = GPOS_NEW(pmp) CStatsPredPoint
												(
												ulColId,
												CStatsPred::EstatscmptL,
												PpointGeneric(pmp, oid, pstrUpperEncoded, lValueUpper)
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
//		CStatisticsTest::EresUnittest_Skew
//
//	@doc:
//		Basis skew test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_Skew()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CBucket *pbucket1 = Pbucket(pmp, 1, 100, CDouble(0.6), CDouble(100.0));
	CBucket *pbucket2 = Pbucket(pmp, 101, 200, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket3 = Pbucket(pmp, 201, 300, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket4 = Pbucket(pmp, 301, 400, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket5 = Pbucket(pmp, 401, 500, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket6 = Pbucket(pmp, 501, 600, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket7 = Pbucket(pmp, 601, 700, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket8 = Pbucket(pmp, 701, 800, CDouble(0.2), CDouble(100.0));

	DrgPbucket *pdrgppbucket1 = GPOS_NEW(pmp) DrgPbucket(pmp);
	pdrgppbucket1->Append(pbucket1);
	pdrgppbucket1->Append(pbucket2);
	pdrgppbucket1->Append(pbucket3);
	CHistogram *phist1 =  GPOS_NEW(pmp) CHistogram(pdrgppbucket1);

	DrgPbucket *pdrgppbucket2 = GPOS_NEW(pmp) DrgPbucket(pmp);
	pdrgppbucket2->Append(pbucket4);
	pdrgppbucket2->Append(pbucket5);
	pdrgppbucket2->Append(pbucket6);
	pdrgppbucket2->Append(pbucket7);
	pdrgppbucket2->Append(pbucket8);
	CHistogram *phist2 =  GPOS_NEW(pmp) CHistogram(pdrgppbucket2);
	GPOS_ASSERT(phist1->DSkew() > phist2->DSkew());

	{
		CAutoTrace at(pmp);
		phist1->OsPrint(at.Os());
		phist2->OsPrint(at.Os());
	}

	GPOS_DELETE(phist1);
	GPOS_DELETE(phist2);

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_SortInt4MCVs
//
//	@doc:
//		Test sorting of Int4 MCVs and their associated frequencies
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_SortInt4MCVs()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidInt4);
	const IMDType *pmdtype = pmda->Pmdtype(pmdid);

	// create three integer MCVs
	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 5);
	CPoint *ppoint2 = CTestUtils::PpointInt4(pmp, 1);
	CPoint *ppoint3 = CTestUtils::PpointInt4(pmp, 10);
	DrgPdatum *pdrgpdatumMCV = GPOS_NEW(pmp) DrgPdatum(pmp);
	IDatum *pdatum1 = ppoint1->Pdatum();
	IDatum *pdatum2 = ppoint2->Pdatum();
	IDatum *pdatum3 = ppoint3->Pdatum();
	pdatum1->AddRef();
	pdatum2->AddRef();
	pdatum3->AddRef();
	pdrgpdatumMCV->Append(pdatum1);
	pdrgpdatumMCV->Append(pdatum2);
	pdrgpdatumMCV->Append(pdatum3);

	// create corresponding frequencies
	DrgPdouble *pdrgpdFreq = GPOS_NEW(pmp) DrgPdouble(pmp);
	// in GPDB, MCVs are stored in descending frequencies
	pdrgpdFreq->Append(GPOS_NEW(pmp) CDouble(0.4));
	pdrgpdFreq->Append(GPOS_NEW(pmp) CDouble(0.2));
	pdrgpdFreq->Append(GPOS_NEW(pmp) CDouble(0.1));

	// exercise MCV sorting function
	CHistogram *phistMCV = CStatisticsUtils::PhistTransformMCV
								(
								pmp,
								pmdtype,
								pdrgpdatumMCV,
								pdrgpdFreq,
								pdrgpdatumMCV->UlLength()
								);

	// create hash map from colid -> histogram
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	// generate int histogram for column 1
	phmulhist->FInsert(GPOS_NEW(pmp) ULONG(1), phistMCV);

	// column width for int4
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);
	phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(1), GPOS_NEW(pmp) CDouble(4.0));

	CStatistics *pstats = GPOS_NEW(pmp) CStatistics
									(
									pmp,
									phmulhist,
									phmuldoubleWidth,
									1000.0 /* dRows */,
									false /* fEmpty */
									);

	// put stats object in an array in order to serialize
	DrgPstats *pdrgpstats = GPOS_NEW(pmp) DrgPstats(pmp);
	pdrgpstats->Append(pstats);

	// serialize stats object
	CWStringDynamic *pstrOutput = CDXLUtils::PstrSerializeStatistics(pmp, pmda, pdrgpstats, true, true);
	GPOS_TRACE(pstrOutput->Wsz());

	// get expected output
	CWStringDynamic str(pmp);
	COstreamString oss(&str);
	CHAR *szDXLExpected = CDXLUtils::SzRead(pmp, szMCVSortExpectedFileName);
	CWStringDynamic dstrExpected(pmp);
	dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXLExpected);

	GPOS_RESULT eres = CTestUtils::EresCompare
								(
								oss,
								pstrOutput,
								&dstrExpected,
								false // mismatch will not be ignored
								);

	// cleanup
	GPOS_DELETE(pstrOutput);
	GPOS_DELETE_ARRAY(szDXLExpected);
	pdrgpdatumMCV->Release();
	pdrgpdFreq->Release();
	pdrgpstats->Release();
	ppoint1->Release();
	ppoint2->Release();
	ppoint3->Release();
	pmdid->Release();

	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsTest::EresUnittest_MergeHistMCV
//
//	@doc:
//		Test merging MCVs and histogram
//
//---------------------------------------------------------------------------
GPOS_RESULT
CStatisticsTest::EresUnittest_MergeHistMCV()
{
	SMergeTestElem rgMergeTestElem[] =
	{
		{
		"../data/dxl/statistics/Merge-Input-MCV-Int.xml",
		"../data/dxl/statistics/Merge-Input-Histogram-Int.xml",
		"../data/dxl/statistics/Merge-Output-Int.xml"
		},

		{
		"../data/dxl/statistics/Merge-Input-MCV-Numeric.xml",
		"../data/dxl/statistics/Merge-Input-Histogram-Numeric.xml",
		"../data/dxl/statistics/Merge-Output-Numeric.xml"
		},

		{
		"../data/dxl/statistics/Merge-Input-MCV-BPChar.xml",
		"../data/dxl/statistics/Merge-Input-Histogram-BPChar.xml",
		"../data/dxl/statistics/Merge-Output-BPChar.xml"
		}
	};

	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG ulLen = GPOS_ARRAY_SIZE(rgMergeTestElem);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		// read input MCVs DXL file
		CHAR *szDXLInputMCV = CDXLUtils::SzRead(pmp, rgMergeTestElem[ul].szInputMCVFile);
		// read input histogram DXL file
		CHAR *szDXLInputHist = CDXLUtils::SzRead(pmp, rgMergeTestElem[ul].szInputHistFile);

		GPOS_CHECK_ABORT;

		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

		// parse the stats objects
		DrgPdxlstatsderrel *pdrgpdxlstatsderrelMCV = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInputMCV, NULL);
		DrgPdxlstatsderrel *pdrgpdxlstatsderrelHist = CDXLUtils::PdrgpdxlstatsderrelParseDXL(pmp, szDXLInputHist, NULL);

		GPOS_CHECK_ABORT;

		CDXLStatsDerivedRelation *pdxlstatsderrelMCV = (*pdrgpdxlstatsderrelMCV)[0];
		const DrgPdxlstatsdercol *pdrgpdxlstatsdercolMCV = pdxlstatsderrelMCV->Pdrgpdxlstatsdercol();
		CDXLStatsDerivedColumn *pdxlstatsdercolMCV = (*pdrgpdxlstatsdercolMCV)[0];
		DrgPbucket *pdrgppbucketMCV = CDXLUtils::Pdrgpbucket(pmp, pmda, pdxlstatsdercolMCV);
		CHistogram *phistMCV =  GPOS_NEW(pmp) CHistogram(pdrgppbucketMCV);

		CDXLStatsDerivedRelation *pdxlstatsderrelHist = (*pdrgpdxlstatsderrelHist)[0];
		const DrgPdxlstatsdercol *pdrgpdxlstatsdercolHist = pdxlstatsderrelHist->Pdrgpdxlstatsdercol();
		CDXLStatsDerivedColumn *pdxlstatsdercolHist = (*pdrgpdxlstatsdercolHist)[0];
		DrgPbucket *pdrgppbucketHist = CDXLUtils::Pdrgpbucket(pmp, pmda, pdxlstatsdercolHist);
		CHistogram *phistHist =  GPOS_NEW(pmp) CHistogram(pdrgppbucketHist);

		GPOS_CHECK_ABORT;

		// exercise the merge
		CHistogram *phistMerged = CStatisticsUtils::PhistMergeMcvHist(pmp, phistMCV, phistHist);

		// create hash map from colid -> histogram
		HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

		// generate int histogram for column 1
		ULONG ulColId = pdxlstatsdercolMCV->UlColId();
		phmulhist->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phistMerged);

		// column width for int4
		HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);
		CDouble dWidth = pdxlstatsdercolMCV->DWidth();
		phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColId), GPOS_NEW(pmp) CDouble(dWidth));

		CStatistics *pstats = GPOS_NEW(pmp) CStatistics
										(
										pmp,
										phmulhist,
										phmuldoubleWidth,
										pdxlstatsderrelMCV->DRows(),
										pdxlstatsderrelMCV->FEmpty()
										);

		// put stats object in an array in order to serialize
		DrgPstats *pdrgpstats = GPOS_NEW(pmp) DrgPstats(pmp);
		pdrgpstats->Append(pstats);

		// serialize stats object
		CWStringDynamic *pstrOutput = CDXLUtils::PstrSerializeStatistics(pmp, pmda, pdrgpstats, true, true);
		GPOS_TRACE(pstrOutput->Wsz());

		// get expected output
		CWStringDynamic str(pmp);
		COstreamString oss(&str);
		CHAR *szDXLExpected = CDXLUtils::SzRead(pmp, rgMergeTestElem[ul].szMergedFile);
		CWStringDynamic dstrExpected(pmp);
		dstrExpected.AppendFormat(GPOS_WSZ_LIT("%s"), szDXLExpected);

		GPOS_RESULT eres = CTestUtils::EresCompare
									(
									oss,
									pstrOutput,
									&dstrExpected,
									false // mismatch will not be ignored
									);

		// cleanup
		GPOS_DELETE_ARRAY(szDXLInputMCV);
		GPOS_DELETE_ARRAY(szDXLInputHist);
		GPOS_DELETE_ARRAY(szDXLExpected);
		GPOS_DELETE(pstrOutput);
		pdrgpdxlstatsderrelMCV->Release();
		pdrgpdxlstatsderrelHist->Release();
		GPOS_DELETE(phistMCV);
		GPOS_DELETE(phistHist);
		pdrgpstats->Release();

		if (GPOS_OK != eres)
		{
			return eres;
		}
	}

	return GPOS_OK;
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
		phmulhist->FInsert(GPOS_NEW(pmp) ULONG(ul), PhistExampleInt4(pmp));

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
	CDouble dRows = pstats->DRows();
	GPOS_TRACE(GPOS_WSZ_LIT("\nOriginal Stats:\n"));
	Print(pmp, pstats);

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
	Print(pmp, pstats1);

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
	Print(pmp, pstats2);

	pstatspredConj1->Release();

	GPOS_ASSERT(dRows1 - dRows2 < 10 && "Disjunctive filter and point filter have very different row estimates");

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
	Print(pmp, pstats3);

	pstatspredConj2->Release();
	GPOS_ASSERT(dRows3 < dRows2  && "Conjunctive filter passes more rows than than point filter");

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
	Print(pmp, pstats4);

	pstatspredDisj1->Release();

	GPOS_ASSERT(dRows4 < dRows2  && "Selective disjunctive filter passes more rows than than point filter");

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
	Print(pmp, pstats5);

	pstatspredConj3->Release();

	GPOS_ASSERT(dRows5 < dRows2  && "Selective conjunctive filter passes more rows than than point filter");

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
