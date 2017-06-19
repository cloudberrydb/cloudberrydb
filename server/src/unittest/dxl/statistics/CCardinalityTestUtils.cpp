//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	@filename:
//		CCardinalityTestUtils.cpp
//
//	@doc:
//		Utility functions used in the testing cardinality estimation
//---------------------------------------------------------------------------

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"
#include "naucrates/dxl/operators/CDXLDatumStatsDoubleMappable.h"
#include "naucrates/dxl/operators/CDXLDatumStatsLintMappable.h"

#include "unittest/dxl/statistics/CCardinalityTestUtils.h"
#include "unittest/gpopt/CTestUtils.h"


// create a bucket with closed integer bounds
CBucket *
CCardinalityTestUtils::PbucketIntegerClosedLowerBound
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

	return GPOS_NEW(pmp) CBucket(ppLower, ppUpper, true /* fLowerClosed */, fUpperClosed, dFrequency, dDistinct);
}

// create an integer bucket with the provider upper/lower bound, frequency and NDV information
CBucket *
CCardinalityTestUtils::PbucketInteger
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

	return GPOS_NEW(pmp) CBucket(ppLower, ppUpper, fLowerClosed, fUpperClosed, dFrequency, dDistinct);
}

// create a singleton bucket containing a boolean value
CBucket *
CCardinalityTestUtils::PbucketSingletonBoolVal
	(
	IMemoryPool *pmp,
	BOOL fValue,
	CDouble dFrequency
	)
{
	CPoint *ppLower = CTestUtils::PpointBool(pmp, fValue);

	// lower bound is also upper bound
	ppLower->AddRef();
	return GPOS_NEW(pmp) CBucket(ppLower, ppLower, true /* fClosedUpper */, true /* fClosedUpper */, dFrequency, 1.0);
}

// helper function to generate integer histogram based on the NDV and bucket information provided
CHistogram*
CCardinalityTestUtils::PhistInt4Remain
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
		CBucket *pbucket = PbucketIntegerClosedLowerBound(pmp, iLower, iUpper, dFrequency, dDistinct);
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

// helper function to generate an example int histogram
CHistogram*
CCardinalityTestUtils::PhistExampleInt4
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
		CBucket *pbucket = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, iLower, iUpper, dFrequency, dDistinct);
		pdrgppbucket->Append(pbucket);
	}

	// add an additional singleton bucket [100, 100]
	pdrgppbucket->Append(CCardinalityTestUtils::PbucketIntegerClosedLowerBound(pmp, 100, 100, 0.1, 1.0));

	return  GPOS_NEW(pmp) CHistogram(pdrgppbucket);
}

// helper function to generates example bool histogram
CHistogram*
CCardinalityTestUtils::PhistExampleBool
	(
	IMemoryPool *pmp
	)
{
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	CBucket *pbucketFalse = CCardinalityTestUtils::PbucketSingletonBoolVal(pmp, false, 0.1);
	CBucket *pbucketTrue = CCardinalityTestUtils::PbucketSingletonBoolVal(pmp, true, 0.2);
	pdrgppbucket->Append(pbucketFalse);
	pdrgppbucket->Append(pbucketTrue);
	return  GPOS_NEW(pmp) CHistogram(pdrgppbucket);
}

// helper function to generate a point from an encoded value of specific datatype
CPoint *
CCardinalityTestUtils::PpointGeneric
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

// helper function to generate a point of numeric datatype
CPoint *
CCardinalityTestUtils::PpointNumeric
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

// helper function to print the bucket object
void
CCardinalityTestUtils::PrintBucket
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

// helper function to print histogram object
void
CCardinalityTestUtils::PrintHist
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

// helper function to print the statistics object
void
CCardinalityTestUtils::PrintStats
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


// EOF
