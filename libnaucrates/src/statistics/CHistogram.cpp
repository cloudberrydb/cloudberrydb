//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CHistogram.cpp
//
//	@doc:
//		Implementation of single-dimensional histogram
//---------------------------------------------------------------------------


#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/common/syslibwrapper.h"

#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CScaleFactorUtils.h"

#include "gpopt/base/CColRef.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;

// default histogram selectivity
const CDouble CHistogram::DDefaultSelectivity(0.4);

// default scale factor when there is no filtering of input
const CDouble CHistogram::DNeutralScaleFactor(1.0);

// the minimum number of distinct values in a column
const CDouble CHistogram::DMinDistinct(1.0);

// default Null frequency
const CDouble CHistogram::DDefaultNullFreq(0.0);

// default NDV remain
const CDouble CHistogram::DDefaultNDVRemain(0.0);

// default frequency of NDV remain
const CDouble CHistogram::DDefaultNDVFreqRemain(0.0);

// sample size used to estimate skew
#define GPOPT_SKEW_SAMPLE_SIZE 1000

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::CHistogram
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CHistogram::CHistogram
	(
	DrgPbucket *pdrgppbucket,
	BOOL fWellDefined
	)
	:
	m_pdrgppbucket(pdrgppbucket),
	m_fWellDefined(fWellDefined),
	m_dNullFreq(CHistogram::DDefaultNullFreq),
	m_dDistinctRemain(DDefaultNDVRemain),
	m_dFreqRemain(DDefaultNDVFreqRemain),
	m_fSkewMeasured(false),
	m_dSkew(1.0),
	m_fNDVScaled(false),
	m_fColStatsMissing(false)
{
	GPOS_ASSERT(NULL != pdrgppbucket);
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::CHistogram
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CHistogram::CHistogram
	(
	DrgPbucket *pdrgppbucket,
	BOOL fWellDefined,
	CDouble dNullFreq,
	CDouble dDistinctRemain,
	CDouble dFreqRemain,
	BOOL fColStatsMissing
	)
	:
	m_pdrgppbucket(pdrgppbucket),
	m_fWellDefined(fWellDefined),
	m_dNullFreq(dNullFreq),
	m_dDistinctRemain(dDistinctRemain),
	m_dFreqRemain(dFreqRemain),
	m_fSkewMeasured(false),
	m_dSkew(1.0),
	m_fNDVScaled(false),
	m_fColStatsMissing(fColStatsMissing)
{
	GPOS_ASSERT(m_pdrgppbucket);
	GPOS_ASSERT(CDouble(0.0) <= dNullFreq);
	GPOS_ASSERT(CDouble(1.0) >= dNullFreq);
	GPOS_ASSERT(CDouble(0.0) <= dDistinctRemain);
	GPOS_ASSERT(CDouble(0.0) <= dFreqRemain);
	GPOS_ASSERT(CDouble(1.0) >= dFreqRemain);
	// if dDistinctRemain is 0, dFreqRemain must be 0 too
	GPOS_ASSERT_IMP(dDistinctRemain < CStatistics::DEpsilon, dFreqRemain < CStatistics::DEpsilon);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::SetNullFrequency
//
//	@doc:
//		Set histograms null frequency
//
//---------------------------------------------------------------------------
void
CHistogram::SetNullFrequency
	(
	CDouble dNullFreq
	)
{
	GPOS_ASSERT(CDouble(0.0) <= dNullFreq && CDouble(1.0) >= dNullFreq);
	m_dNullFreq = dNullFreq;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream&
CHistogram::OsPrint
	(
	IOstream &os
	)
	const
{
	os << std::endl << "[" << std::endl;

	ULONG ulNumBuckets = m_pdrgppbucket->UlLength();
	for (ULONG ulBucketIdx = 0; ulBucketIdx < ulNumBuckets; ulBucketIdx++)
	{
		os << "b" << ulBucketIdx << " = ";
		(*m_pdrgppbucket)[ulBucketIdx]->OsPrint(os);
		os << std::endl;
	}
	os << "]" << std::endl;

	os << "Null fraction: " << m_dNullFreq << std::endl;

	os << "Remaining NDV: " << m_dDistinctRemain << std::endl;

	os << "Remaining frequency: " << m_dFreqRemain << std::endl;

	if (m_fSkewMeasured)
	{
		os << "Skew: " << m_dSkew << std::endl;
	}
	else
	{
		os << "Skew: not measured" << std::endl;
	}

	os << "Was NDVs re-scaled Based on Row Estimate: " << m_fNDVScaled << std::endl;

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::FEmpty
//
//	@doc:
//		Check if histogram is empty
//
//---------------------------------------------------------------------------
BOOL
CHistogram::FEmpty
	()
	const
{
	return (0 == m_pdrgppbucket->UlLength() && CStatistics::DEpsilon > m_dNullFreq && CStatistics::DEpsilon > m_dDistinctRemain);
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistLessThanOrLessThanEqual
//
//	@doc:
//		Construct new histogram with less than or less than equal to filter
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistLessThanOrLessThanEqual
	(
	IMemoryPool *pmp,
	CStatsPred::EStatsCmpType escmpt,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(CStatsPred::EstatscmptL == escmpt || CStatsPred::EstatscmptLEq == escmpt);

	DrgPbucket *pdrgppbucketNew = GPOS_NEW(pmp) DrgPbucket(pmp);
	const ULONG ulNumBuckets = m_pdrgppbucket->UlLength();

	for (ULONG ulBucketIdx = 0; ulBucketIdx < ulNumBuckets; ulBucketIdx++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIdx];
		if (pbucket->FBefore(ppoint))
		{
			break;
		}
		else if (pbucket->FAfter(ppoint))
		{
			pdrgppbucketNew->Append(pbucket->PbucketCopy(pmp));
		}
		else
		{
			GPOS_ASSERT(pbucket->FContains(ppoint));
			CBucket *pbucketLast = pbucket->PbucketScaleUpper(pmp, ppoint, CStatsPred::EstatscmptLEq == escmpt /*fIncludeUpper*/);
			if (NULL != pbucketLast)
			{
				pdrgppbucketNew->Append(pbucketLast);
			}
			break;
		}
	}

	CDouble dDistinctRemain = 0.0;
	CDouble dFreqRemain = 0.0;
	if (CStatistics::DEpsilon < m_dDistinctRemain * DDefaultSelectivity)
	{
		dDistinctRemain = m_dDistinctRemain * DDefaultSelectivity;
		dFreqRemain = m_dFreqRemain * DDefaultSelectivity;
	}

	return GPOS_NEW(pmp) CHistogram
					(
					pdrgppbucketNew,
					true, // fWellDefined
					CDouble(0.0), // fNullFreq
					dDistinctRemain,
					dFreqRemain
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PdrgppbucketNEqual
//
//	@doc:
//		Return an array buckets after applying non equality filter on the histogram buckets
//
//---------------------------------------------------------------------------
DrgPbucket *
CHistogram::PdrgppbucketNEqual
	(
	IMemoryPool *pmp,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(NULL != ppoint);
	DrgPbucket *pdrgppbucketNew = GPOS_NEW(pmp) DrgPbucket(pmp);
	const ULONG ulNumBuckets = m_pdrgppbucket->UlLength();
	bool fPointNull = ppoint->Pdatum()->FNull();

	for (ULONG ulBucketIdx = 0; ulBucketIdx < ulNumBuckets; ulBucketIdx++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIdx];

		if (pbucket->FContains(ppoint) && !fPointNull)
		{
			CBucket *pbucketLT = pbucket->PbucketScaleUpper(pmp, ppoint, false /*fIncludeUpper */);
			if (NULL != pbucketLT)
			{
				pdrgppbucketNew->Append(pbucketLT);
			}
			CBucket *pbucketGT = pbucket->PbucketGreaterThan(pmp, ppoint);
			if (NULL != pbucketGT)
			{
				pdrgppbucketNew->Append(pbucketGT);
			}
		}
		else
		{
			pdrgppbucketNew->Append(pbucket->PbucketCopy(pmp));
		}
	}

	return pdrgppbucketNew;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistNEqual
//
//	@doc:
//		Construct new histogram with not equal filter
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistNEqual
	(
	IMemoryPool *pmp,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(NULL != ppoint);

	DrgPbucket *pdrgppbucket = PdrgppbucketNEqual(pmp, ppoint);
	CDouble dNullFreq(0.0);

	return GPOS_NEW(pmp) CHistogram(pdrgppbucket, true /*fWellDefined*/, dNullFreq, m_dDistinctRemain, m_dFreqRemain);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistNEqual
//
//	@doc:
//		Construct new histogram with IDF filter
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistIDF
	(
	IMemoryPool *pmp,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(NULL != ppoint);

	DrgPbucket *pdrgppbucket =  PdrgppbucketNEqual(pmp, ppoint);
	CDouble dNullFreq(0.0);
	if (!ppoint->Pdatum()->FNull())
	{
		// (col IDF NOT-NULL-CONSTANT) means that null values will also be returned
		dNullFreq = m_dNullFreq;
	}

	return GPOS_NEW(pmp) CHistogram(pdrgppbucket, true /*fWellDefined*/, dNullFreq, m_dDistinctRemain, m_dFreqRemain);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PdrgppbucketEqual
//
//	@doc:
//		Return an array buckets after applying equality filter on the histogram buckets
//
//---------------------------------------------------------------------------
DrgPbucket *
CHistogram::PdrgppbucketEqual
	(
	IMemoryPool *pmp,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(NULL != ppoint);

	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);

	if (ppoint->Pdatum()->FNull())
	{
		return pdrgppbucket;
	}

	const ULONG ulNumBuckets = m_pdrgppbucket->UlLength();
	ULONG ulBucketIdx = 0;

	for (ulBucketIdx = 0; ulBucketIdx < ulNumBuckets; ulBucketIdx++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIdx];

		if (pbucket->FContains(ppoint))
		{
			if (pbucket->FSingleton())
			{
				// reuse existing bucket
				pdrgppbucket->Append(pbucket->PbucketCopy(pmp));
			}
			else
			{
				// scale containing bucket
				CBucket *pbucketLast = pbucket->PbucketSingleton(pmp, ppoint);
				pdrgppbucket->Append(pbucketLast);
			}
			break; // only one bucket can contain point
		}
	}

	return pdrgppbucket;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistEqual
//
//	@doc:
//		Construct new histogram with equality filter
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistEqual
	(
	IMemoryPool *pmp,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(NULL != ppoint);

	if (ppoint->Pdatum()->FNull())
	{
		return GPOS_NEW(pmp) CHistogram
							(
							GPOS_NEW(pmp) DrgPbucket(pmp),
							true /* fWellDefined */,
							m_dNullFreq,
							DDefaultNDVRemain,
							DDefaultNDVFreqRemain
							);
	}
	
	DrgPbucket *pdrgppbucket =  PdrgppbucketEqual(pmp, ppoint);

	if (CStatistics::DEpsilon < m_dDistinctRemain && 0 == pdrgppbucket->UlLength()) // no match is found in the buckets
	{
		return GPOS_NEW(pmp) CHistogram
						(
						pdrgppbucket,
						true, // fWellDefined
						0.0, // dNullFreq
						1.0, // dDistinctRemain
						std::min(CDouble(1.0), m_dFreqRemain / m_dDistinctRemain) // dFreqRemain
						);
	}

	return GPOS_NEW(pmp) CHistogram(pdrgppbucket);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistINDF
//
//	@doc:
//		Construct new histogram with INDF filter
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistINDF
	(
	IMemoryPool *pmp,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(NULL != ppoint);

	DrgPbucket *pdrgppbucket =  PdrgppbucketEqual(pmp, ppoint);
	const ULONG ulBuckets = pdrgppbucket->UlLength();
	CDouble dNullFreq(0.0);
	if (ppoint->Pdatum()->FNull())
	{
		GPOS_ASSERT(0 == ulBuckets);
		// (col INDF NULL) means that only the null values will be returned
		dNullFreq = m_dNullFreq;
	}

	if (CStatistics::DEpsilon < m_dDistinctRemain && 0 == ulBuckets) // no match is found in the buckets
	{
		return GPOS_NEW(pmp) CHistogram
						(
						pdrgppbucket,
						true, // fWellDefined
						dNullFreq,
						1.0, // dDistinctRemain
						std::min(CDouble(1.0), m_dFreqRemain / m_dDistinctRemain) // dFreqRemain
						);
	}

	return GPOS_NEW(pmp) CHistogram
					(
					pdrgppbucket,
					true /* fWellDefined */,
					dNullFreq,
					CHistogram::DDefaultNDVRemain,
					CHistogram::DDefaultNDVFreqRemain
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistGreaterThanOrGreaterThanEqual
//
//	@doc:
//		Construct new histogram with greater than or greater than equal filter
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistGreaterThanOrGreaterThanEqual
	(
	IMemoryPool *pmp,
	CStatsPred::EStatsCmpType escmpt,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(CStatsPred::EstatscmptGEq == escmpt || CStatsPred::EstatscmptG == escmpt);

	DrgPbucket *pdrgppbucketNew = GPOS_NEW(pmp) DrgPbucket(pmp);
	const ULONG ulNumBuckets = m_pdrgppbucket->UlLength();

	// find first bucket that contains ppoint
	ULONG ulBucketIdx = 0;
	for (ulBucketIdx = 0; ulBucketIdx < ulNumBuckets; ulBucketIdx++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIdx];
		if (pbucket->FBefore(ppoint))
		{
			break;
		}
		if (pbucket->FContains(ppoint))
		{
			if (CStatsPred::EstatscmptGEq == escmpt)
			{
				// first bucket needs to be scaled down
				CBucket *pbucketFirst = pbucket->PbucketScaleLower(pmp, ppoint,  true /* fIncludeLower */);
				pdrgppbucketNew->Append(pbucketFirst);
			}
			else
			{
				CBucket *pbucketGT = pbucket->PbucketGreaterThan(pmp, ppoint);
				if (NULL != pbucketGT)
				{
					pdrgppbucketNew->Append(pbucketGT);
				}
			}
			ulBucketIdx++;
			break;
		}
	}

	// add rest of the buckets
	for (; ulBucketIdx < ulNumBuckets; ulBucketIdx++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIdx];
		pdrgppbucketNew->Append(pbucket->PbucketCopy(pmp));
	}

	CDouble dDistinctRemain = 0.0;
	CDouble dFreqRemain = 0.0;
	if (CStatistics::DEpsilon < m_dDistinctRemain * DDefaultSelectivity)
	{
		dDistinctRemain = m_dDistinctRemain * DDefaultSelectivity;
		dFreqRemain = m_dFreqRemain * DDefaultSelectivity;
	}

	return GPOS_NEW(pmp) CHistogram
					(
					pdrgppbucketNew,
					true, // fWellDefined
					CDouble(0.0), // fNullFreq
					dDistinctRemain,
					dFreqRemain
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::DFrequency
//
//	@doc:
//		Sum of frequencies from buckets.
//
//---------------------------------------------------------------------------
CDouble
CHistogram::DFrequency
	()
	const
{
	CDouble dFrequency(0.0);
	const ULONG ulBuckets = m_pdrgppbucket->UlLength();
	for (ULONG ulBucketIdx = 0; ulBucketIdx < ulBuckets; ulBucketIdx++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIdx];
		dFrequency = dFrequency + pbucket->DFrequency();
	}

	if (CStatistics::DEpsilon < m_dNullFreq)
	{
		dFrequency = dFrequency + m_dNullFreq;
	}

	return dFrequency + m_dFreqRemain;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::DDistinct
//
//	@doc:
//		Sum of number of distinct values from buckets
//
//---------------------------------------------------------------------------
CDouble
CHistogram::DDistinct
	()
	const
{
	CDouble dDistinct(0.0);
	const ULONG ulBuckets = m_pdrgppbucket->UlLength();
	for (ULONG ulBucketIdx = 0; ulBucketIdx < ulBuckets; ulBucketIdx++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIdx];
		dDistinct = dDistinct + pbucket->DDistinct();
	}
	CDouble dDistinctNull(0.0);
	if (CStatistics::DEpsilon < m_dNullFreq)
	{
		dDistinctNull = 1.0;
	}

	return dDistinct + dDistinctNull + m_dDistinctRemain;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::CapNDVs
//
//	@doc:
//		Cap the total number of distinct values (NDVs) in buckets to the number of rows
//
//---------------------------------------------------------------------------
void
CHistogram::CapNDVs
	(
	CDouble dRows
	)
{
	const ULONG ulBuckets = m_pdrgppbucket->UlLength();
	CDouble dDistinct = DDistinct();
	if (dRows >= dDistinct)
	{
		// no need for capping
		return;
	}

	m_fNDVScaled = true;

	CDouble dScaleRatio = (dRows / dDistinct).DVal();
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ul];
		CDouble dDistinctBucket = pbucket->DDistinct();
		pbucket->SetDistinct(std::max(CHistogram::DMinDistinct.DVal(), (dDistinctBucket * dScaleRatio).DVal()));
	}

	m_dDistinctRemain = m_dDistinctRemain * dScaleRatio;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::FNormalized
//
//	@doc:
//		Sum of frequencies is approx 1.0
//
//---------------------------------------------------------------------------
BOOL
CHistogram::FNormalized
	()
	const
{
	CDouble dFrequency = DFrequency();

	return (dFrequency > CDouble(1.0) - CStatistics::DEpsilon
			&& dFrequency < CDouble(1.0) + CStatistics::DEpsilon);
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::FValid
//
//	@doc:
//		Is histogram well formed? Checks are:
//		1. Buckets should be in order (if applicable)
//		2. Buckets should not overlap
//		3. Frequencies should add up to less than 1.0
//
//---------------------------------------------------------------------------
BOOL
CHistogram::FValid
	()
	const
{
	// frequencies should not add up to more than 1.0
	if (DFrequency() > CDouble(1.0) + CStatistics::DEpsilon)
	{
		return false;
	}

	for (ULONG ulBucketIdx = 1; ulBucketIdx < m_pdrgppbucket->UlLength(); ulBucketIdx++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIdx];
		CBucket *pbucketPrev = (*m_pdrgppbucket)[ulBucketIdx - 1];

		// the later bucket's lower point must be greater than or equal to
		// earlier bucket's upper point. Even if the underlying datum does not
		// support ordering, the check is safe.
		if (pbucket->PpLower()->FLessThan(pbucketPrev->PpUpper()))
		{
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistFilterNormalized
//
//	@doc:
//		Construct new histogram with filter and normalize
//		output histogram
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistFilterNormalized
	(
	IMemoryPool *pmp,
	CStatsPred::EStatsCmpType escmpt,
	CPoint *ppoint,
	CDouble *pdScaleFactor
	)
	const
{
	// if histogram is not well-defined, then result is not well defined
	if (!FWellDefined())
	{
		CHistogram *phistAfter =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp), false /* fWellDefined */);
		*pdScaleFactor = CDouble(1.0) / CHistogram::DDefaultSelectivity;
		return phistAfter;
	}

	CHistogram *phistAfter = PhistFilter(pmp, escmpt, ppoint);
	*pdScaleFactor = phistAfter->DNormalize();
	GPOS_ASSERT(phistAfter->FValid());

	return phistAfter;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistJoinNormalized
//
//	@doc:
//		Construct new histogram by joining with another and normalize
//		output histogram. If the join is not an equality join the function
//		returns an empty histogram
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistJoinNormalized
	(
	IMemoryPool *pmp,
	CStatsPred::EStatsCmpType escmpt,
	CDouble dRows,
	const CHistogram *phistOther,
	CDouble dRowsOther,
	CDouble *pdScaleFactor
	)
	const
{
	GPOS_ASSERT(NULL != phistOther);

	BOOL fEqOrINDF = (CStatsPred::EstatscmptEq == escmpt || CStatsPred::EstatscmptINDF == escmpt);
	if (!fEqOrINDF)
	{
		*pdScaleFactor = CScaleFactorUtils::DDefaultScaleFactorNonEqualityJoin;

		if (CStatsPred::EstatscmptNEq == escmpt || CStatsPred::EstatscmptIDF == escmpt)
		{
			*pdScaleFactor = DInEqualityJoinScaleFactor(pmp, dRows, phistOther, dRowsOther);
		}

		return PhistJoin(pmp, escmpt, phistOther);
	}

	// if either histogram is not well-defined, the result is not well defined
	if (!FWellDefined() || !phistOther->FWellDefined())
	{
		CHistogram *phistAfter =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp), false /* fWellDefined */);
		(*pdScaleFactor) = CDouble(std::min(dRows.DVal(), dRowsOther.DVal()));

		return phistAfter;
	}
	
	CHistogram *phistAfter = PhistJoin(pmp, escmpt, phistOther);
	*pdScaleFactor = phistAfter->DNormalize();

	// based on Ramakrishnan and Gehrke, "Database Management Systems, Third Ed", page 484
	// the scaling factor of equality join is the MAX of the number of distinct
	// values in each of the inputs

	*pdScaleFactor = std::max
						(
						std::max(DMinDistinct.DVal(), DDistinct().DVal()),
						std::max(DMinDistinct.DVal(), phistOther->DDistinct().DVal())
						);

	CDouble dCartesianProduct = dRows * dRowsOther;
	if (phistAfter->FEmpty())
	{
		// if join histogram is empty for equality join condition
		// use Cartesian product size as scale factor
		*pdScaleFactor = dCartesianProduct;
	}

	if (CStatsPred::EstatscmptINDF == escmpt)
	{
		// if the predicate is INDF then we must count for the cartesian
		// product of NULL values in both the histograms
		CDouble dExpectedEqJoin = dCartesianProduct / *pdScaleFactor;
		CDouble dNulls = dRows * m_dNullFreq;
		CDouble dNullsOther = dRowsOther * phistOther->DNullFreq();
		CDouble dExpectedINDF = dExpectedEqJoin + (dNulls * dNullsOther);
		*pdScaleFactor = dCartesianProduct / dExpectedINDF;
	}

	// bound scale factor by cross product
	*pdScaleFactor = std::min((*pdScaleFactor).DVal(), dCartesianProduct.DVal());

	GPOS_ASSERT(phistAfter->FValid());
	return phistAfter;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::DInEqualityJoinScaleFactor
//
//	@doc:
//		Scalar factor of inequality (!=) join condition
//
//---------------------------------------------------------------------------
CDouble
CHistogram::DInEqualityJoinScaleFactor
	(
	IMemoryPool *pmp,
	CDouble dRows,
	const CHistogram *phistOther,
	CDouble dRowsOther
	)
	const
{
	GPOS_ASSERT(NULL != phistOther);

	CDouble dScaleFactor(1.0);

	// we compute the scale factor of the inequality join (!= aka <>)
	// from the scale factor of equi-join
	CHistogram *phistJoin = PhistJoinNormalized
								(
								pmp,
								CStatsPred::EstatscmptEq,
								dRows,
								phistOther,
								dRowsOther,
								&dScaleFactor
								);
	GPOS_DELETE(phistJoin);

	CDouble dCartesianProduct = dRows * dRowsOther;

	GPOS_ASSERT(CDouble(1.0) <= dScaleFactor);
	CDouble dSelectivityEq = 1 / dScaleFactor;
	CDouble dSelectivityNEq = (1 - dSelectivityEq);

	dScaleFactor = dCartesianProduct;
	if (CStatistics::DEpsilon < dSelectivityNEq)
	{
		dScaleFactor = 1 / dSelectivityNEq;
	}

	return dScaleFactor;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistLASJoinNormalized
//
//	@doc:
//		Construct new histogram by left anti semi join with another and normalize
//		output histogram
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistLASJoinNormalized
	(
	IMemoryPool *pmp,
	CStatsPred::EStatsCmpType escmpt,
	CDouble dRows,
	const CHistogram *phistOther,
	CDouble *pdScaleFactor,
	BOOL fIgnoreLasjHistComputation
	)
	const
{
	// if either histogram is not well-defined, the result is not well defined
	if (!FWellDefined() || !phistOther->FWellDefined())
	{
		CHistogram *phistAfter =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp), false /* fWellDefined */);
		(*pdScaleFactor) = CDouble(1.0);

		return phistAfter;
	}

	if (fIgnoreLasjHistComputation)
	{
		// TODO:  04/14/2012 : LASJ derivation is pretty aggressive.
		// simply return a copy of the histogram with a scale factor corresponding to default selectivity.
		CHistogram *phistAfter = PhistCopy(pmp);
		*pdScaleFactor = CDouble(1.0) / CHistogram::DDefaultSelectivity;
		GPOS_ASSERT(phistAfter->FValid());

		return phistAfter;
	}

	CHistogram *phistAfter = PhistLASJ(pmp, escmpt, phistOther);
	*pdScaleFactor = phistAfter->DNormalize();

	if (CStatsPred::EstatscmptEq != escmpt && CStatsPred::EstatscmptINDF != escmpt)
	{
		// TODO: , June 6 2014, we currently only support join histogram computation
		// for equality and INDF predicates, we cannot accurately estimate
		// the scale factor of the other predicates
		*pdScaleFactor = CDouble(1.0) / CHistogram::DDefaultSelectivity;
	}
	*pdScaleFactor = std::min((*pdScaleFactor).DVal(), dRows.DVal());
	GPOS_ASSERT(phistAfter->FValid());

	return phistAfter;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistFilter
//
//	@doc:
//		Construct new histogram after applying the filter (no normalization)
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistFilter
	(
	IMemoryPool *pmp,
	CStatsPred::EStatsCmpType escmpt,
	CPoint *ppoint
	)
	const
{
	CHistogram *phistAfter = NULL;
	GPOS_ASSERT(FSupportsFilter(escmpt));

	switch(escmpt)
	{
		case CStatsPred::EstatscmptEq:
		{
			phistAfter = PhistEqual(pmp, ppoint);
			break;
		}
		case CStatsPred::EstatscmptINDF:
		{
			phistAfter = PhistINDF(pmp, ppoint);
			break;
		}
		case CStatsPred::EstatscmptL:
		case CStatsPred::EstatscmptLEq:
		{
			phistAfter = PhistLessThanOrLessThanEqual(pmp, escmpt, ppoint);
			break;
		}
		case CStatsPred::EstatscmptG:
		case CStatsPred::EstatscmptGEq:
		{
			phistAfter = PhistGreaterThanOrGreaterThanEqual(pmp, escmpt, ppoint);
			break;
		}
		case CStatsPred::EstatscmptNEq:
		{
			phistAfter = PhistNEqual(pmp, ppoint);
			break;
		}
		case CStatsPred::EstatscmptIDF:
		{
			phistAfter = PhistIDF(pmp, ppoint);
			break;
		}
		default:
		{
			GPOS_RTL_ASSERT_MSG(false, "Not supported. Must not be called.");
			break;
		}
	}
	return phistAfter;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistJoin
//
//	@doc:
//		Construct new histogram by joining with another histogram, no normalization
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistJoin
	(
	IMemoryPool *pmp,
	CStatsPred::EStatsCmpType escmpt,
	const CHistogram *phist
	)
	const
{
	GPOS_ASSERT(FSupportsJoin(escmpt));

	if (CStatsPred::EstatscmptEq == escmpt)
	{
		return PhistJoinEquality(pmp, phist);
	}

	if (CStatsPred::EstatscmptINDF == escmpt)
	{
		return PhistJoinINDF(pmp, phist);
	}

	// TODO:  Feb 24 2014, We currently only support creation of histogram for equi join
	return  GPOS_NEW(pmp) CHistogram( GPOS_NEW(pmp) DrgPbucket(pmp), false /* fWellDefined */);
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistLASJ
//
//	@doc:
//		Construct new histogram by LASJ with another histogram, no normalization
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistLASJ
	(
	IMemoryPool *pmp,
	CStatsPred::EStatsCmpType escmpt,
	const CHistogram *phist
	)
	const
{
	GPOS_ASSERT(NULL != phist);

	if (CStatsPred::EstatscmptEq != escmpt && CStatsPred::EstatscmptINDF != escmpt)
	{
		// TODO: , June 6 2014, we currently only support join histogram computation
		// for equality and INDF predicates, we return the original histogram
		return PhistCopy(pmp);
	}

	DrgPbucket *pdrgppbucketNew = GPOS_NEW(pmp) DrgPbucket(pmp);

	CBucket *pbucketLowerSplit = NULL;
	CBucket *pbucketUpperSplit = NULL;

	ULONG ul1 = 0; // index into this histogram
	ULONG ul2 = 0; // index into other histogram

	CBucket *pbucketCandidate = NULL;

	const ULONG ulBuckets1 = UlBuckets();
	const ULONG ulBuckets2 = phist->UlBuckets();

	while (ul1 < ulBuckets1 && ul2 < ulBuckets2)
	{
		// bucket from other histogram
		CBucket *pbucket2 = (*phist->m_pdrgppbucket) [ul2];

		// yet to choose a candidate
		GPOS_ASSERT(NULL == pbucketCandidate);

		if (NULL != pbucketUpperSplit)
		{
			pbucketCandidate = pbucketUpperSplit;
		}
		else
		{
			pbucketCandidate = (*m_pdrgppbucket)[ul1]->PbucketCopy(pmp); // candidate bucket in result histogram
			ul1++;
		}

		pbucketLowerSplit = NULL;
		pbucketUpperSplit = NULL;

		pbucketCandidate->Difference(pmp, pbucket2, &pbucketLowerSplit, &pbucketUpperSplit);

		if (NULL != pbucketLowerSplit)
		{
			pdrgppbucketNew->Append(pbucketLowerSplit);
		}

		// need to find a new candidate
		GPOS_DELETE(pbucketCandidate);
		pbucketCandidate = NULL;

		ul2++;
	}

	pbucketCandidate = pbucketUpperSplit;

	// if we looked at all the buckets from the other histogram, then add remaining buckets from
	// this histogram
	if (ul2 == ulBuckets2)
	{
		// candidate is part of the result
		if (NULL != pbucketCandidate)
		{
			pdrgppbucketNew->Append(pbucketCandidate);
		}

		CStatisticsUtils::AddRemainingBuckets(pmp, m_pdrgppbucket, pdrgppbucketNew, &ul1);
	}
	else
	{
		GPOS_DELETE(pbucketCandidate);
	}

	CDouble dNullFreq = CStatisticsUtils::DNullFreqLASJ(escmpt, this, phist);

	return GPOS_NEW(pmp) CHistogram(pdrgppbucketNew, true /*fWellDefined*/, dNullFreq, m_dDistinctRemain, m_dFreqRemain);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::DNormalize
//
//	@doc:
//		Scales frequencies on histogram so that they add up to 1.0.
//		Returns the scaling factor that was employed. Should not be
//		called on empty histogram.
//
//---------------------------------------------------------------------------
CDouble
CHistogram::DNormalize()
{
	// trivially normalized
	if (UlBuckets() == 0 && CStatistics::DEpsilon > m_dNullFreq && CStatistics::DEpsilon > m_dDistinctRemain)
	{
		return CDouble(GPOS_FP_ABS_MAX);
	}

	CDouble dScaleFactor = std::max(DOUBLE(1.0), (CDouble(1.0) / DFrequency()).DVal());

	for (ULONG ul = 0; ul < m_pdrgppbucket->UlLength(); ul++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ul];
		pbucket->SetFrequency(pbucket->DFrequency() * dScaleFactor);
	}

	m_dNullFreq = m_dNullFreq * dScaleFactor;

	CDouble dFreqRemain = std::min((m_dFreqRemain * dScaleFactor).DVal(), DOUBLE(1.0));
	if (CStatistics::DEpsilon > m_dDistinctRemain)
	{
		dFreqRemain = 0.0;
	}
	m_dFreqRemain = dFreqRemain;

	return dScaleFactor;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistCopy
//
//	@doc:
//		Deep copy of histogram
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistCopy
	(
	IMemoryPool *pmp
	)
	const
{
	DrgPbucket *pdrgpbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
	for (ULONG ul = 0; ul < m_pdrgppbucket->UlLength(); ul++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ul];
		pdrgpbucket->Append(pbucket->PbucketCopy(pmp));
	}

	CHistogram *phistCopy = GPOS_NEW(pmp) CHistogram(pdrgpbucket, m_fWellDefined, m_dNullFreq, m_dDistinctRemain, m_dFreqRemain);
	if (FScaledNDV())
	{
		phistCopy->SetNDVScaled();
	}

	return phistCopy;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::FSupportsFilter
//
//	@doc:
//		Is statistics comparison type supported for filter?
//
//---------------------------------------------------------------------------
BOOL
CHistogram::FSupportsFilter
	(
	CStatsPred::EStatsCmpType escmpt
	)
{
	// is the scalar comparison type one of <, <=, =, >=, >, <>, IDF, INDF
	switch (escmpt)
	{
		case CStatsPred::EstatscmptL:
		case CStatsPred::EstatscmptLEq:
		case CStatsPred::EstatscmptEq:
		case CStatsPred::EstatscmptGEq:
		case CStatsPred::EstatscmptG:
		case CStatsPred::EstatscmptNEq:
		case CStatsPred::EstatscmptIDF:
		case CStatsPred::EstatscmptINDF:
			return true;
		default:
			return false;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::FSupportsJoin
//
//	@doc:
//		Is comparison type supported for join?
//
//---------------------------------------------------------------------------
BOOL
CHistogram::FSupportsJoin
	(
	CStatsPred::EStatsCmpType escmpt
	)
{
	return (CStatsPred::EstatscmptOther != escmpt);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistJoinEquality
//
//	@doc:
//		Construct a new histogram with equality join
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistJoinEquality
	(
	IMemoryPool *pmp,
	const CHistogram *phist
	)
	const
{
	DrgPbucket *pdrgppbucketJoin = GPOS_NEW(pmp) DrgPbucket(pmp);
	ULONG ul1 = 0; // index on buckets from this histogram
	ULONG ul2 = 0; // index on buckets from other histogram

	const ULONG ulBuckets1 = UlBuckets();
	const ULONG ulBuckets2 = phist->UlBuckets();

	CDouble dFreqJoinBuckets1(0.0);
	CDouble dFreqJoinBuckets2(0.0);

	while (ul1 < ulBuckets1 && ul2 < ulBuckets2)
	{
		CBucket *pbucket1 = (*m_pdrgppbucket)[ul1];
		CBucket *pbucket2 = (*phist->m_pdrgppbucket)[ul2];

		if (pbucket1->FIntersects(pbucket2))
		{
			CDouble dFreqIntersect1(0.0);
			CDouble dFreqIntersect2(0.0);

			CBucket *pbucketNew = pbucket1->PbucketIntersect(pmp, pbucket2, &dFreqIntersect1, &dFreqIntersect2);
			pdrgppbucketJoin->Append(pbucketNew);

			dFreqJoinBuckets1 = dFreqJoinBuckets1 + dFreqIntersect1;
			dFreqJoinBuckets2 = dFreqJoinBuckets2 + dFreqIntersect2;

			INT iRes = CBucket::ICompareUpperBounds(pbucket1, pbucket2);
			if (0 == iRes)
			{
				// both ubs are equal
				ul1++;
				ul2++;
			}
			else if (1 > iRes)
			{
				// pbucket1's ub is smaller than that of the ub of pbucket2
				ul1++;
			}
			else
			{
				ul2++;
			}
		}
		else if (pbucket1->FBefore(pbucket2))
		{
			// buckets do not intersect there one bucket is before the other
			ul1++;
		}
		else
		{
			GPOS_ASSERT(pbucket2->FBefore(pbucket1));
			ul2++;
		}
	}

	CDouble dDistinctRemain = 0.0;
	CDouble dFreqRemain = 0.0;

	ComputeJoinNDVRemainInfo
		(
		this,
		phist,
		pdrgppbucketJoin,
		dFreqJoinBuckets1,
		dFreqJoinBuckets2,
		&dDistinctRemain,
		&dFreqRemain
		);

	return GPOS_NEW(pmp) CHistogram(pdrgppbucketJoin, true /*fWellDefined*/, 0.0 /*dNullFreq*/, dDistinctRemain, dFreqRemain);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistJoinINDF
//
//	@doc:
//		Construct a new histogram for an INDF join predicate
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistJoinINDF
	(
	IMemoryPool *pmp,
	const CHistogram *phist
	)
	const
{
	CHistogram *phistJoin = PhistJoinEquality(pmp, phist);

	// compute the null frequency is the same means as how we perform equi-join
	// see CBuckets::PbucketIntersect for details
	CDouble dNullFreq = DNullFreq() * phist->DNullFreq() *
			DOUBLE(1.0) / std::max(DDistinct(), phist->DDistinct());
	phistJoin->SetNullFrequency(dNullFreq);

	return phistJoin;
}



//---------------------------------------------------------------------------
//	@function:
//		CHistogram::FCanComputeJoinNDVRemain
//
//	@doc:
//		Check if we can compute NDVRemain for JOIN histogram for the given
//		input histograms
//---------------------------------------------------------------------------
BOOL
CHistogram::FCanComputeJoinNDVRemain
	(
	const CHistogram *phist1,
	const CHistogram *phist2
	)
{
	GPOS_ASSERT(NULL != phist1);
	GPOS_ASSERT(NULL != phist2);

	BOOL fHasBuckets1 = (0 != phist1->UlBuckets());
	BOOL fHasBuckets2 = (0 != phist2->UlBuckets());
	BOOL fHasDistinctRemain1 = CStatistics::DEpsilon < phist1->DDistinctRemain();
	BOOL fHasDistinctRemain2 = CStatistics::DEpsilon < phist2->DDistinctRemain();

	if (!fHasDistinctRemain1 && !fHasDistinctRemain2)
	{
		// no remaining tuples information hence no need compute NDVRemain for join histogram
		return false;
	}

	if ((fHasDistinctRemain1 || fHasBuckets1) && (fHasDistinctRemain2 || fHasBuckets2))
	{
		//
		return true;
	}

	// insufficient information to compute JoinNDVRemain, we need:
	// 1. for each input either have a histograms or NDVRemain
	// 2. at least one inputs must have NDVRemain
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::ComputeJoinNDVRemainInfo
//
//	@doc:
//		Compute the effects of the NDV and frequency of the tuples not captured
//		by the histogram
//---------------------------------------------------------------------------
void
CHistogram::ComputeJoinNDVRemainInfo
	(
	const CHistogram *phist1,
	const CHistogram *phist2,
	DrgPbucket *pdrgppbucketJoin,
	CDouble dFreqJoinBuckets1,
	CDouble dFreqJoinBuckets2,
	CDouble *pdDistinctRemain,
	CDouble *pdFreqRemain
	)
{
	GPOS_ASSERT(NULL != phist1);
	GPOS_ASSERT(NULL != phist2);
	GPOS_ASSERT(NULL != pdrgppbucketJoin);

	CDouble dNDVJoin = CStatisticsUtils::DDistinct(pdrgppbucketJoin);
	CDouble dFreqJoin = CStatisticsUtils::DFrequency(pdrgppbucketJoin);

	*pdDistinctRemain = 0.0;
	*pdFreqRemain = 0.0;

	if (!FCanComputeJoinNDVRemain(phist1, phist2))
	{
		return;
	}

	if (CStatistics::DEpsilon >= (1 - dFreqJoin))
	{
		return;
	}

	// we compute the following information for the resulting join histogram
	// 1. NDVRemain and 2. Frequency of the NDVRemain

	// compute the number of non-null distinct values in each input histogram
	CDouble dDistinct1 = phist1->DDistinct();
	CDouble dDistinctNonNull1 = dDistinct1;
	if (CStatistics::DEpsilon < phist1->DNullFreq())
	{
		dDistinctNonNull1 = dDistinctNonNull1 - 1.0;
	}

	CDouble dDistinct2 = phist2->DDistinct();
	CDouble dDistinctNonNull2 = dDistinct2;
	if (CStatistics::DEpsilon < phist2->DNullFreq())
	{
		dDistinctNonNull2 = dDistinctNonNull2 - 1.0;
	}

	// the estimated final number of distinct value for the join is the minimum of the non-null
	// distinct values of the two inputs. This follows the principle of used to estimate join
	// scaling factor -- defined as the maximum NDV of the two inputs
	CDouble dNDVJoinFinal = std::min(dDistinctNonNull1, dDistinctNonNull2);
	CDouble dNDVJoinRemain = dNDVJoinFinal - dNDVJoin;

	// compute the frequency of the non-joining buckets in each input histogram
	CDouble dFreqBuckets1 =  CStatisticsUtils::DFrequency(phist1->Pdrgpbucket());
	CDouble dFreqBuckets2 =  CStatisticsUtils::DFrequency(phist2->Pdrgpbucket());
	CDouble dFreqNonJoinBuckets1 = std::max(CDouble(0), (dFreqBuckets1 - dFreqJoinBuckets1));
	CDouble dFreqNonJoinBuckets2 = std::max(CDouble(0), (dFreqBuckets2 - dFreqJoinBuckets2));

	// compute the NDV of the non-joining buckets
	CDouble dNDVNonJoinBuckets1 = CStatisticsUtils::DDistinct(phist1->Pdrgpbucket()) - dNDVJoin;
	CDouble dNDVNonJoinBuckets2 = CStatisticsUtils::DDistinct(phist2->Pdrgpbucket()) - dNDVJoin;

	CDouble dFreqRemain1 = phist1->DFreqRemain();
	CDouble dFreqRemain2 = phist2->DFreqRemain();

	// the frequency of the NDVRemain of the join is made of:
	// 1. frequency of the NDV as a result of joining NDVRemain1 and NDVRemain2
	// 2. frequency of the NDV as a results of joining NDVRemain1 and dNDVNonJoinBuckets2
	// 3. frequency of the NDV as a results of joining NDVRemain2 and dNDVNonJoinBuckets1

	CDouble dFreqJoinRemain = dFreqRemain1 * dFreqRemain2 / std::max(phist1->DDistinctRemain(), phist2->DDistinctRemain());
	dFreqJoinRemain = dFreqJoinRemain + dFreqRemain1 * dFreqNonJoinBuckets2 / std::max(dDistinctNonNull1, dNDVNonJoinBuckets2);
	dFreqJoinRemain = dFreqJoinRemain + dFreqRemain2 * dFreqNonJoinBuckets1 / std::max(dDistinctNonNull2, dNDVNonJoinBuckets1);

	*pdDistinctRemain = dNDVJoinRemain;
	*pdFreqRemain = dFreqJoinRemain;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistGroupByNormalized
//
//	@doc:
//		Construct new histogram by removing dups and normalize
//		output histogram
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistGroupByNormalized
	(
	IMemoryPool *pmp,
	CDouble, // dRows,
	CDouble *pdDistinctValues
	)
	const
{
	// if either histogram is not well-defined, the result is not well defined
	if (!FWellDefined())
	{
		CHistogram *phistAfter =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp), false /* fWellDefined */);
		*pdDistinctValues = DMinDistinct / CHistogram::DDefaultSelectivity;
		return phistAfter;
	}

	// total number of distinct values
	CDouble dDistinct = DDistinct();

	DrgPbucket *pdrgppbucketNew = GPOS_NEW(pmp) DrgPbucket(pmp);

	const ULONG ulBuckets = m_pdrgppbucket->UlLength();
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ul];
		CPoint *ppLower = pbucket->PpLower();
		CPoint *ppUpper = pbucket->PpUpper();
		ppLower->AddRef();
		ppUpper->AddRef();

		BOOL fUpperClosed = false;
		if (ppLower->FEqual(ppUpper))
		{
			fUpperClosed = true;
		}
		CBucket *pbucketNew = GPOS_NEW(pmp) CBucket
										(
										ppLower,
										ppUpper, 
										true /* fClosedLower */, 
										fUpperClosed,
										pbucket->DDistinct() / dDistinct,
										pbucket->DDistinct()
										);
		pdrgppbucketNew->Append(pbucketNew);
	}

	CDouble dNullFreqNew = CDouble(0.0);
	if (CStatistics::DEpsilon < m_dNullFreq)
	{
		dNullFreqNew = std::min(CDouble(1.0), CDouble(1.0)/dDistinct);
	}


	CDouble dFreqRemain = 0.0;
	if (CStatistics::DEpsilon < m_dDistinctRemain)
	{
		// TODO:  11/22/2013 - currently the NDV of a histogram could be 0 or a decimal number.
		// We may not need the capping if later dDistinct is lower bounded at 1.0 for non-empty histogram
		dFreqRemain = std::min(CDouble(1.0), m_dDistinctRemain/dDistinct);
	}

	CHistogram *phistAfter = GPOS_NEW(pmp) CHistogram(pdrgppbucketNew, true /*fWellDefined*/, dNullFreqNew, m_dDistinctRemain, dFreqRemain);
	*pdDistinctValues = phistAfter->DDistinct();

	return phistAfter;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistUnionAllNormalized
//
//	@doc:
//		Construct new histogram based on union all operation
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistUnionAllNormalized
	(
	IMemoryPool *pmp,
	CDouble dRows,
	const CHistogram *phist,
	CDouble dRowsOther
	)
	const
{
	DrgPbucket *pdrgppbucketNew = GPOS_NEW(pmp) DrgPbucket(pmp);
	ULONG ul1 = 0; // index on buckets from this histogram
	ULONG ul2 = 0; // index on buckets from other histogram
	CBucket *pbucket1 = (*this) [ul1];
	CBucket *pbucket2 = (*phist) [ul2];
	CDouble dRowsNew = (dRowsOther + dRows);

	// flags to determine if the buckets where residue of the bucket-merge operation
	BOOL fbucket1Residual = false;
	BOOL fbucket2Residual = false;

	while (NULL != pbucket1 && NULL != pbucket2)
	{
		if (pbucket1->FBefore(pbucket2))
		{
			pdrgppbucketNew->Append(pbucket1->PbucketUpdateFrequency(pmp, dRows, dRowsNew));
			CleanupResidualBucket(pbucket1, fbucket1Residual);
			ul1++;
			pbucket1 = (*this) [ul1];
			fbucket1Residual = false;
		}
		else if (pbucket2->FBefore(pbucket1))
		{
			pdrgppbucketNew->Append(pbucket2->PbucketUpdateFrequency(pmp, dRowsOther, dRowsNew));
			CleanupResidualBucket(pbucket2, fbucket2Residual);
			ul2++;
			pbucket2 = (*phist) [ul2];
			fbucket2Residual = false;
		}
		else
		{
			GPOS_ASSERT(pbucket1->FIntersects(pbucket2));
			CBucket *pbucket1New = NULL;
			CBucket *pbucket2New = NULL;
			CBucket *pbucketMerge = pbucket1->PbucketMerge(pmp, pbucket2, dRows, dRowsOther, &pbucket1New, &pbucket2New);
			pdrgppbucketNew->Append(pbucketMerge);

			GPOS_ASSERT(NULL == pbucket1New || NULL == pbucket2New);
			CleanupResidualBucket(pbucket1, fbucket1Residual);
			CleanupResidualBucket(pbucket2, fbucket2Residual);

			pbucket1 = PbucketNext(this, pbucket1New, &fbucket1Residual, &ul1);
			pbucket2 = PbucketNext(phist, pbucket2New, &fbucket2Residual, &ul2);
		}
	}

	const ULONG ulBuckets1 = UlBuckets();
	const ULONG ulBuckets2 = phist->UlBuckets();

	GPOS_ASSERT_IFF(NULL == pbucket1, ul1 == ulBuckets1);
	GPOS_ASSERT_IFF(NULL == pbucket2, ul2 == ulBuckets2);

	ul1 = UlAddResidualUnionAllBucket(pmp, pdrgppbucketNew, pbucket1, dRowsOther, dRowsNew, fbucket1Residual, ul1);
	ul2 = UlAddResidualUnionAllBucket(pmp, pdrgppbucketNew, pbucket2, dRows, dRowsNew, fbucket2Residual, ul2);

	CleanupResidualBucket(pbucket1, fbucket1Residual);
	CleanupResidualBucket(pbucket2, fbucket2Residual);

	// add any leftover buckets from other histogram
	AddBuckets(pmp, phist->m_pdrgppbucket, pdrgppbucketNew, dRowsOther, dRowsNew, ul2, ulBuckets2);

	// add any leftover buckets from this histogram
	AddBuckets(pmp, m_pdrgppbucket, pdrgppbucketNew, dRows, dRowsNew, ul1, ulBuckets1);

	CDouble dNullFreqNew = (m_dNullFreq * dRows + phist->m_dNullFreq * dRowsOther) / dRowsNew;

	CDouble dDistinctRemain = std::max(m_dDistinctRemain, phist->m_dDistinctRemain);
	CDouble dFreqRemain = (m_dFreqRemain * dRows + phist->m_dFreqRemain * dRowsOther) / dRowsNew;

	CHistogram *phistResult = GPOS_NEW(pmp) CHistogram(pdrgppbucketNew, true /*fWellDefined*/, dNullFreqNew, dDistinctRemain, dFreqRemain);
	(void) phistResult->DNormalize();

	return phistResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::UlAddResidualUnionAllBucket
//
//	@doc:
//		Add residual bucket in the union all operation to the array of
//		buckets in the histogram
//---------------------------------------------------------------------------
ULONG
CHistogram::UlAddResidualUnionAllBucket
	(
	IMemoryPool *pmp,
	DrgPbucket *pdrgppbucket,
	CBucket *pbucket,
	CDouble dRowsOld,
	CDouble dRowsNew,
	BOOL fbucketResidual,
	ULONG ulIndex
	)
	const
{
	GPOS_ASSERT(NULL != pdrgppbucket);

	if (fbucketResidual)
	{
		pdrgppbucket->Append(pbucket->PbucketUpdateFrequency(pmp, dRowsOld, dRowsNew));
		return ulIndex + 1;
	}

	return ulIndex;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::AddBuckets
//
//	@doc:
//		Add buckets from one array to another
//
//---------------------------------------------------------------------------
void
CHistogram::AddBuckets
	(
	IMemoryPool *pmp,
	DrgPbucket *pdrgppbucketSrc,
	DrgPbucket *pdrgppbucketDest,
	CDouble dRowsOld,
	CDouble dRowsNew,
	ULONG ulBegin,
	ULONG ulEnd
	)
{
	GPOS_ASSERT(NULL != pdrgppbucketSrc);
	GPOS_ASSERT(NULL != pdrgppbucketDest);
	GPOS_ASSERT(ulBegin <= ulEnd);
	GPOS_ASSERT(ulEnd <= pdrgppbucketSrc->UlLength());

	for (ULONG ul = ulBegin; ul < ulEnd; ul++)
	{
		pdrgppbucketDest->Append(((*pdrgppbucketSrc)[ul])->PbucketUpdateFrequency(pmp, dRowsOld, dRowsNew));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::CleanupResidualBucket
//
//	@doc:
//		Cleanup residual buckets
//
//---------------------------------------------------------------------------
void
CHistogram::CleanupResidualBucket
	(
	CBucket *pbucket,
	BOOL fbucketResidual
	)
	const
{
	if (NULL != pbucket && fbucketResidual)
	{
		GPOS_DELETE(pbucket);
		pbucket = NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PbucketNext
//
//	@doc:
//		Get the next bucket for union / union all
//
//---------------------------------------------------------------------------
CBucket *
CHistogram::PbucketNext
	(
	const CHistogram *phist,
	CBucket *pbucketNew,
	BOOL *pfbucketResidual,
	ULONG *pulBucketIndex
	)
	const
{
	GPOS_ASSERT(NULL != phist);
	if (NULL != pbucketNew)
	{
		*pfbucketResidual = true;
		return pbucketNew;
	}

	*pulBucketIndex = *pulBucketIndex  + 1;
	*pfbucketResidual = false;

	return (*phist) [*pulBucketIndex];
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistUnionNormalized
//
//	@doc:
//		Construct new histogram based on union operation
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistUnionNormalized
	(
	IMemoryPool *pmp,
	CDouble dRows,
	const CHistogram *phistOther,
	CDouble dRowsOther,
	CDouble *pdRowsOutput
	)
	const
{
	GPOS_ASSERT(NULL != phistOther);

	ULONG ul1 = 0; // index on buckets from this histogram
	ULONG ul2 = 0; // index on buckets from other histogram
	CBucket *pbucket1 = (*this) [ul1];
	CBucket *pbucket2 = (*phistOther) [ul2];

	// flags to determine if the buckets where residue of the bucket-merge operation
	BOOL fbucket1Residual = false;
	BOOL fbucket2Residual = false;

	// array of buckets in the resulting histogram
	DrgPbucket *pdrgppbucket = GPOS_NEW(pmp) DrgPbucket(pmp);

	// number of tuples in each bucket of the resulting histogram
	DrgPdouble *pdrgpdoubleRows = GPOS_NEW(pmp) DrgPdouble(pmp);

	CDouble dRowCummulative(0.0);
	while (NULL != pbucket1 && NULL != pbucket2)
	{
		if (pbucket1->FBefore(pbucket2))
		{
			pdrgppbucket->Append(pbucket1->PbucketCopy(pmp));
			pdrgpdoubleRows->Append(GPOS_NEW(pmp) CDouble(pbucket1->DFrequency() * dRows));
			CleanupResidualBucket(pbucket1, fbucket1Residual);
			ul1++;
			pbucket1 = (*this) [ul1];
			fbucket1Residual = false;
		}
		else if (pbucket2->FBefore(pbucket1))
		{
			pdrgppbucket->Append(pbucket2->PbucketCopy(pmp));
			pdrgpdoubleRows->Append(GPOS_NEW(pmp) CDouble(pbucket2->DFrequency() * dRowsOther));
			CleanupResidualBucket(pbucket2, fbucket2Residual);
			ul2++;
			pbucket2 = (*phistOther) [ul2];
			fbucket2Residual = false;
		}
		else
		{
			GPOS_ASSERT(pbucket1->FIntersects(pbucket2));
			CBucket *pbucket1New = NULL;
			CBucket *pbucket2New = NULL;
			CBucket *pbucketMerge = NULL;

			pbucketMerge = pbucket1->PbucketMerge
									(
									pmp,
									pbucket2,
									dRows,
									dRowsOther,
									&pbucket1New,
									&pbucket2New,
									false /* fUnionAll */
									);

			// add the estimated number of rows in the merged bucket
			pdrgpdoubleRows->Append(GPOS_NEW(pmp) CDouble(pbucketMerge->DFrequency() * dRows));
			pdrgppbucket->Append(pbucketMerge);

			GPOS_ASSERT(NULL == pbucket1New || NULL == pbucket2New);
			CleanupResidualBucket(pbucket1, fbucket1Residual);
			CleanupResidualBucket(pbucket2, fbucket2Residual);

			pbucket1 = PbucketNext(this, pbucket1New, &fbucket1Residual, &ul1);
			pbucket2 = PbucketNext(phistOther, pbucket2New, &fbucket2Residual, &ul2);
		}
	}

	const ULONG ulBuckets1 = UlBuckets();
	const ULONG ulBuckets2 = phistOther->UlBuckets();

	GPOS_ASSERT_IFF(NULL == pbucket1, ul1 == ulBuckets1);
	GPOS_ASSERT_IFF(NULL == pbucket2, ul2 == ulBuckets2);

	ul1 = UlAddResidualUnionBucket(pmp, pdrgppbucket, pbucket1, dRowsOther, fbucket1Residual, ul1, pdrgpdoubleRows);
	ul2 = UlAddResidualUnionBucket(pmp, pdrgppbucket, pbucket2, dRows, fbucket2Residual, ul2, pdrgpdoubleRows);

	CleanupResidualBucket(pbucket1, fbucket1Residual);
	CleanupResidualBucket(pbucket2, fbucket2Residual);

	// add any leftover buckets from other histogram
	AddBuckets(pmp, phistOther->m_pdrgppbucket, pdrgppbucket, dRowsOther, pdrgpdoubleRows, ul2, ulBuckets2);

	// add any leftover buckets from this histogram
	AddBuckets(pmp, m_pdrgppbucket, pdrgppbucket, dRows, pdrgpdoubleRows, ul1, ulBuckets1);

	// compute the total number of null values from both histograms
	CDouble dNullRows= std::max( (this->DNullFreq() * dRows), (phistOther->DNullFreq() * dRowsOther));

	// compute the total number of distinct values (NDV) that are not captured by the buckets in both the histograms
	CDouble dNDVRemain = std::max(m_dDistinctRemain, phistOther->DDistinctRemain());

	// compute the total number of rows having distinct values not captured by the buckets in both the histograms
	CDouble dNDVRemainRows = std::max( (this->DFreqRemain() * dRows), (phistOther->DFreqRemain() * dRowsOther));

	CHistogram *phistResult = PhistUpdatedFrequency
									(
									pmp,
									pdrgppbucket,
									pdrgpdoubleRows,
									pdRowsOutput,
									dNullRows,
									dNDVRemain,
									dNDVRemainRows
									);
	// clean up
	pdrgppbucket->Release();
	pdrgpdoubleRows->Release();

	return phistResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistUpdatedFrequency
//
//	@doc:
//		Create a new histogram with updated bucket frequency
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistUpdatedFrequency
	(
	IMemoryPool *pmp,
	DrgPbucket *pdrgppbucket,
	DrgPdouble *pdrgpdouble,
	CDouble *pdRowOutput,
	CDouble dNullRows,
	CDouble dNDVRemain,
	CDouble dNDVRemainRows
	)
	const
{
	GPOS_ASSERT(NULL != pdrgppbucket);
	GPOS_ASSERT(NULL != pdrgpdouble);

	const ULONG ulLen = pdrgpdouble->UlLength();
	GPOS_ASSERT(ulLen == pdrgppbucket->UlLength());

	CDouble dRowCummulative = dNullRows + dNDVRemainRows;
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CDouble dRows = *(*pdrgpdouble)[ul];
		dRowCummulative = dRowCummulative + dRows;
	}

	*pdRowOutput = std::max(CStatistics::DMinRows, dRowCummulative);

	DrgPbucket *pdrgppbucketNew = GPOS_NEW(pmp) DrgPbucket(pmp);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CDouble dRows = *(*pdrgpdouble)[ul];
		CBucket *pbucket = (*pdrgppbucket)[ul];

		// reuse the points
		pbucket->PpLower()->AddRef();
		pbucket->PpUpper()->AddRef();

		CDouble dFrequency = dRows / *pdRowOutput;

		CBucket *pbucketNew = GPOS_NEW(pmp) CBucket
										(
										pbucket->PpLower(),
										pbucket->PpUpper(),
										pbucket->FLowerClosed(),
										pbucket->FUpperClosed(),
										dFrequency,
										pbucket->DDistinct()
										);

		pdrgppbucketNew->Append(pbucketNew);
	}

	CDouble dNullFreq = dNullRows / *pdRowOutput ;
	CDouble dNDVRemainFreq =  dNDVRemainRows / *pdRowOutput ;

	return GPOS_NEW(pmp) CHistogram
							(
							pdrgppbucketNew,
							true /* fWellDefined */,
							dNullFreq,
							dNDVRemain,
							dNDVRemainFreq,
							false /* fColStatsMissing */
							);
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::UlAddResidualUnionBucket
//
//	@doc:
//		Add residual bucket in an union operation to the array of buckets
//		in the histogram
//---------------------------------------------------------------------------
ULONG
CHistogram::UlAddResidualUnionBucket
	(
	IMemoryPool *pmp,
	DrgPbucket *pdrgppbucket,
	CBucket *pbucket,
	CDouble dRows,
	BOOL fbucketResidual,
	ULONG ulIndex,
	DrgPdouble *pdrgpdouble
	)
	const
{
	GPOS_ASSERT(NULL != pdrgppbucket);
	GPOS_ASSERT(NULL != pdrgpdouble);

	if (!fbucketResidual)
	{
		return ulIndex;
	}

	CBucket *pbucketNew = pbucket->PbucketCopy(pmp);
	pdrgppbucket->Append(pbucketNew);
	pdrgpdouble->Append(GPOS_NEW(pmp) CDouble(pbucketNew->DFrequency() * dRows));

	return ulIndex + 1;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::AddBuckets
//
//	@doc:
//		Add buckets from one array to another
//
//---------------------------------------------------------------------------
void
CHistogram::AddBuckets
	(
	IMemoryPool *pmp,
	DrgPbucket *pdrgppbucketSrc,
	DrgPbucket *pdrgppbucketDest,
	CDouble dRows,
	DrgPdouble *pdrgpdouble,
	ULONG ulBegin,
	ULONG ulEnd
	)
{
	GPOS_ASSERT(NULL != pdrgppbucketSrc);
	GPOS_ASSERT(NULL != pdrgppbucketDest);
	GPOS_ASSERT(ulBegin <= ulEnd);
	GPOS_ASSERT(ulEnd <= pdrgppbucketSrc->UlLength());

	for (ULONG ul = ulBegin; ul < ulEnd; ul++)
	{
		CBucket *pbucketNew = ((*pdrgppbucketSrc)[ul])->PbucketCopy(pmp);
		pdrgppbucketDest->Append(pbucketNew);
		pdrgpdouble->Append(GPOS_NEW(pmp) CDouble(pbucketNew->DFrequency() * dRows));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::operator []
//
//	@doc:
//		Accessor for n-th bucket. Returns NULL if outside bounds
//
//---------------------------------------------------------------------------
CBucket *
CHistogram::operator []
	(
	ULONG ulPos
	)
	const
{
	if (ulPos < UlBuckets())
	{
		return (*m_pdrgppbucket) [ulPos];
	}
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CHistogram::Pdxlstatsdercol
//
//	@doc:
//		Translate the histogram into a the dxl derived column statistics
//
//---------------------------------------------------------------------------
CDXLStatsDerivedColumn *
CHistogram::Pdxlstatsdercol
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	ULONG ulColId,
	CDouble dWidth
	)
	const
{
	DrgPdxlbucket *pdrgpdxlbucket = GPOS_NEW(pmp) DrgPdxlbucket(pmp);

	const ULONG ulBuckets = m_pdrgppbucket->UlLength();
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ul];

		CDouble dFreq = pbucket->DFrequency();
		CDouble dDistinct = pbucket->DDistinct();

		CDXLDatum *pdxldatumLower = pbucket->PpLower()->Pdxldatum(pmp, pmda);
		CDXLDatum *pdxldatumUpper = pbucket->PpUpper()->Pdxldatum(pmp, pmda);

		CDXLBucket *pdxlbucket = GPOS_NEW(pmp) CDXLBucket
											(
											pdxldatumLower,
											pdxldatumUpper,
											pbucket->FLowerClosed(),
											pbucket->FUpperClosed(),
											dFreq,
											dDistinct
											);

		pdrgpdxlbucket->Append(pdxlbucket);
	}

	return GPOS_NEW(pmp) CDXLStatsDerivedColumn(ulColId, dWidth, m_dNullFreq, m_dDistinctRemain, m_dFreqRemain, pdrgpdxlbucket);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::UlRandomBucketIndex
//
//	@doc:
//		Randomly pick a bucket index based on bucket frequency values
//
//---------------------------------------------------------------------------
ULONG
CHistogram::UlRandomBucketIndex
	(
	ULONG *pulSeed
	)
	const
{
	const ULONG ulSize = m_pdrgppbucket->UlLength();
	GPOS_ASSERT(0 < ulSize);

	DOUBLE dRandVal = ((DOUBLE) clib::UlRandR(pulSeed)) / RAND_MAX;
	CDouble dAccFreq = 0;
	for (ULONG ul = 0; ul < ulSize - 1; ul++)
	{
		CBucket *pbucket = (*m_pdrgppbucket)[ul];
		dAccFreq = dAccFreq + pbucket->DFrequency();

		// we compare generated random value with accumulated frequency,
		// this will result in picking a bucket based on its frequency,
		// example: bucket freq {0.1, 0.3, 0.6}
		//			random value in [0,0.1] --> pick bucket 1
		//			random value in [0.1,0.4] --> pick bucket 2
		//			random value in [0.4,1.0] --> pick bucket 3

		if (dRandVal <= dAccFreq)
		{
			return ul;
		}
	}

	return ulSize - 1;
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::ComputeSkew
//
//	@doc:
//		Estimate data skew by sampling histogram buckets,
//		the estimate value is >= 1.0, where 1.0 indicates no skew
//
//		skew is estimated by computing the second and third moments of
//		sample distribution: for a sample of size n, where x_bar is sample
//		mean, skew is estimated as (m_3/m_2^(1.5)),
//		where m_2 = 1/n Sum ((x -x_bar)^2), and
//		m_3 = 1/n Sum ((x -x_bar)^3)
//
//		since we use skew as multiplicative factor in cost model, this function
//		returns (1.0 + |skew estimate|)
//
//
//---------------------------------------------------------------------------
void
CHistogram::ComputeSkew()
{
	m_fSkewMeasured = true;

	if (!FNormalized() || 0 == m_pdrgppbucket->UlLength() || !(*m_pdrgppbucket)[0]->FCanSample())
	{
		return;
	}

	// generate randomization seed from system time
	TIMEVAL tv;
	syslib::GetTimeOfDay(&tv, NULL/*timezone*/);
	ULONG ulSeed = UlCombineHashes((ULONG) tv.tv_sec, (ULONG)tv.tv_usec);

	// generate a sample from histogram data, and compute sample mean
	DOUBLE dSampleMean = 0;
	DOUBLE rgdSamples[GPOPT_SKEW_SAMPLE_SIZE];
	for (ULONG ul = 0; ul < GPOPT_SKEW_SAMPLE_SIZE; ul++)
	{
		ULONG ulBucketIndex = UlRandomBucketIndex(&ulSeed);
		CBucket *pbucket = (*m_pdrgppbucket)[ulBucketIndex];
		rgdSamples[ul] = pbucket->DSample(&ulSeed).DVal();
		dSampleMean = dSampleMean + rgdSamples[ul];
	}
	dSampleMean = (DOUBLE) dSampleMean / GPOPT_SKEW_SAMPLE_SIZE;

	// compute second and third moments of sample distribution
	DOUBLE d2 = 0;
	DOUBLE d3 = 0;
	for (ULONG ul = 0; ul < GPOPT_SKEW_SAMPLE_SIZE; ul++)
	{
		d2 = d2 + pow((rgdSamples[ul] - dSampleMean) , 2.0);
		d3 = d3 + pow((rgdSamples[ul] - dSampleMean) , 3.0);
	}
	DOUBLE dm2 = (DOUBLE)(d2 / GPOPT_SKEW_SAMPLE_SIZE);
	DOUBLE dm3 = (DOUBLE)(d3 / GPOPT_SKEW_SAMPLE_SIZE);

	// set skew measure
	m_dSkew =  CDouble(1.0 + fabs(dm3 / pow(dm2, 1.5)));
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistDefault
//
//	@doc:
//		Create the default histogram for a given column reference
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistDefault
	(
	IMemoryPool *pmp,
	CColRef *pcr,
	BOOL fEmpty
	)
{
	GPOS_ASSERT(NULL != pcr);

	if (IMDType::EtiBool == pcr->Pmdtype()->Eti() && !fEmpty)
	{
		return CHistogram::PhistDefaultBoolColStats(pmp);
	}

	return GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp), false /* fWellDefined */);
}


//---------------------------------------------------------------------------
//	@function:
//		CHistogram::PhistDefaultBoolColStats
//
//	@doc:
//		Create the default non-empty histogram for a boolean column
//
//---------------------------------------------------------------------------
CHistogram *
CHistogram::PhistDefaultBoolColStats
	(
	IMemoryPool *pmp
	)
{
	DrgPbucket *pdrgpbucket = GPOS_NEW(pmp) DrgPbucket(pmp);

	// a boolean column can at most have 3 values (true, false, and NULL).
	CDouble dDistinctRemain = CDouble(3.0);
	CDouble dFreqRemain = CDouble(1.0);
	CDouble dNullFreq = CDouble(0.0);

	return GPOS_NEW(pmp) CHistogram
						(
						pdrgpbucket,
						true /* fWellDefined */,
						dNullFreq,
						dDistinctRemain,
						dFreqRemain,
						true /*fColStatsMissing */
						);
}

// EOF

