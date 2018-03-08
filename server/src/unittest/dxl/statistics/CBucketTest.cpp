//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Inc.
//
//	@filename:
//		CBucketTest.cpp
//
//	@doc:
//		Testing operations on histogram buckets
//---------------------------------------------------------------------------

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include <stdint.h>

#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CStatisticsUtils.h"

#include "naucrates/base/CDatumGenericGPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/base/CDatumBoolGPDB.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "unittest/base.h"
#include "unittest/dxl/statistics/CCardinalityTestUtils.h"
#include "unittest/dxl/statistics/CBucketTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"

using namespace gpopt;

// unittest for statistics objects
GPOS_RESULT
CBucketTest::EresUnittest()
{
	// tests that use shared optimization context
	CUnittest rgutSharedOptCtxt[] =
		{
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketInt4),
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketBool),
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketScale),
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketDifference),
		GPOS_UNITTEST_FUNC(CBucketTest::EresUnittest_CBucketIntersect),
		};

	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc(pmp, &mda, NULL /* pceeval */, CTestUtils::Pcm(pmp));

	return CUnittest::EresExecute(rgutSharedOptCtxt, GPOS_ARRAY_SIZE(rgutSharedOptCtxt));
}

// basic int4 bucket tests;
GPOS_RESULT
CBucketTest::EresUnittest_CBucketInt4()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate integer points
	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 1);
	CPoint *ppoint2 = CTestUtils::PpointInt4(pmp, 2);
	CPoint *ppoint3 = CTestUtils::PpointInt4(pmp, 3);

	// bucket [1,1]
	CBucket *pbucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, 1, 1, CDouble(1.0), CDouble(1.0));
	CCardinalityTestUtils::PrintBucket(pmp, "b1", pbucket1);

	GPOS_RTL_ASSERT_MSG(pbucket1->FContains(ppoint1), "[1,1] must contain 1");
	GPOS_RTL_ASSERT_MSG(CDouble(1.0) == pbucket1->DOverlap(ppoint1),
						"overlap of 1 in [1,1] must 1.0");

	GPOS_RTL_ASSERT_MSG(!pbucket1->FContains(ppoint2), "[1,1] must not contain 2");

	// bucket [1,3)
	CBucket *pbucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, 1, 3, CDouble(1.0), CDouble(10.0));
	CCardinalityTestUtils::PrintBucket(pmp, "b2", pbucket2);

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
	CBucket *pbucket3 = CCardinalityTestUtils::PbucketInteger(pmp, 1, 2, true, true, CDouble(1.0), CDouble(1.0));
	CBucket *pbucket4 = CCardinalityTestUtils::PbucketInteger(pmp, 2, 4, false, false, CDouble(1.0), CDouble(1.0));

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

// basic BOOL bucket tests;
GPOS_RESULT
CBucketTest::EresUnittest_CBucketBool()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate boolean points
	CPoint *p1 = CTestUtils::PpointBool(pmp, true);
	CPoint *p2 = CTestUtils::PpointBool(pmp, false);

	// bucket for true
	CBucket *pbucket = CCardinalityTestUtils::PbucketSingletonBoolVal(pmp, true, CDouble(1.0));

	GPOS_RTL_ASSERT_MSG(pbucket->FContains(p1), "true bucket must contain true");
	GPOS_RTL_ASSERT_MSG(CDouble(1.0) == pbucket->DOverlap(p1), "overlap must 1.0");

	GPOS_RTL_ASSERT_MSG(!pbucket->FContains(p2), "true bucket must not contain false");
	GPOS_RTL_ASSERT_MSG(CDouble(0.0) == pbucket->DOverlap(p2), "overlap must 0.0");

	p1->Release();
	p2->Release();

	GPOS_DELETE(pbucket);

	return GPOS_OK;
}

// scaling a bucket
GPOS_RESULT
CBucketTest::EresUnittest_CBucketScale()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// generate integer point
	CPoint *ppoint1 = CTestUtils::PpointInt4(pmp, 10);

	// bucket [1,100)
	CBucket *pbucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, 1, 100, CDouble(0.5), CDouble(20.0));

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

// difference operation on buckets
GPOS_RESULT
CBucketTest::EresUnittest_CBucketDifference()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// bucket [1,100)
	CBucket *pbucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, 1, 100, CDouble(1.0), CDouble(1.0));

	// bucket [50,75)
	CBucket *pbucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, 50, 60, CDouble(1.0), CDouble(1.0));

	// bucket [200, 300)
	CBucket *pbucket3 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, 200, 300, CDouble(1.0), CDouble(1.0));

	CBucket *pbucket4 = NULL;
	CBucket *pbucket5 = NULL;
	pbucket1->Difference(pmp, pbucket2, &pbucket4, &pbucket5);
	GPOS_RTL_ASSERT(NULL != pbucket4);
	GPOS_RTL_ASSERT(NULL != pbucket5);
	CCardinalityTestUtils::PrintBucket(pmp, "pbucket4", pbucket4);
	CCardinalityTestUtils::PrintBucket(pmp, "pbucket5", pbucket4);

	CBucket *pbucket6 = NULL;
	CBucket *pbucket7 = NULL;
	pbucket1->Difference(pmp, pbucket3, &pbucket6, &pbucket7);
	GPOS_RTL_ASSERT(NULL != pbucket6);
	GPOS_RTL_ASSERT(NULL == pbucket7);
	CCardinalityTestUtils::PrintBucket(pmp, "pbucket6", pbucket6);

	GPOS_DELETE(pbucket1);
	GPOS_DELETE(pbucket2);
	GPOS_DELETE(pbucket3);
	GPOS_DELETE(pbucket4);
	GPOS_DELETE(pbucket5);
	GPOS_DELETE(pbucket6);

	return GPOS_OK;
}

// intersection of buckets
GPOS_RESULT
CBucketTest::EresUnittest_CBucketIntersect()
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
		CBucket *pbucket1 = CCardinalityTestUtils::PbucketInteger
								(
								pmp,
								rgBucketsIntersectTestElem[ul].m_iLb1,
								rgBucketsIntersectTestElem[ul].m_iUb1,
								rgBucketsIntersectTestElem[ul].m_fLb1Closed,
								rgBucketsIntersectTestElem[ul].m_fUb1Closed,
								CDouble(0.1),
								CDouble(100.0)
								);

		CBucket *pbucket2 = CCardinalityTestUtils::PbucketInteger
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
			CBucket *pbucketExpected = CCardinalityTestUtils::PbucketInteger
											(
											pmp,
											rgBucketsIntersectTestElem[ul].m_iLbOutput,
											rgBucketsIntersectTestElem[ul].m_iUbOutput,
											rgBucketsIntersectTestElem[ul].m_fLbOutputClosed,
											rgBucketsIntersectTestElem[ul].m_fUbOutputClosed,
											CDouble(0.1),
											CDouble(100.0)
											);

			BOOL fMatch = FMatchBucketBoundary(pbucketOuput, pbucketExpected);

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


// do the bucket boundaries match
BOOL
CBucketTest::FMatchBucketBoundary
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

// EOF

