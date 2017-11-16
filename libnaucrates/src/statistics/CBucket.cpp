//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CBucket.cpp
//
//	@doc:
//		Implementation of histogram bucket
//---------------------------------------------------------------------------
#include <stdlib.h>
#include "gpos/base.h"

#include "naucrates/base/IDatumStatisticsMappable.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CStatistics.h"

#include "gpopt/base/COptCtxt.h"

using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CBucket::CBucket
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CBucket::CBucket
	(
	CPoint *ppointLower,
	CPoint *ppointUpper,
	BOOL fLowerClosed,
	BOOL fUpperClosed,
	CDouble dFrequency,
	CDouble dDistinct
	)
	:
	m_ppointLower(ppointLower),
	m_ppointUpper(ppointUpper),
	m_fLowerClosed(fLowerClosed),
	m_fUpperClosed(fUpperClosed),
	m_dFrequency(dFrequency),
	m_dDistinct(dDistinct)
{
	GPOS_ASSERT(NULL != m_ppointLower);
	GPOS_ASSERT(NULL != m_ppointUpper);
	GPOS_ASSERT(0.0 <= m_dFrequency && 1.0 >= m_dFrequency);
	GPOS_ASSERT(0.0 <= m_dDistinct);

	// singleton bucket lower and upper bound are closed
	GPOS_ASSERT_IMP(FSingleton(), fLowerClosed && fUpperClosed);

	// null values should be in null fraction of the histogram
	GPOS_ASSERT(!m_ppointLower->Pdatum()->FNull());
	GPOS_ASSERT(!m_ppointUpper->Pdatum()->FNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::~CBucket
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CBucket::~CBucket()
{
	m_ppointLower->Release();
	m_ppointLower = NULL;
	m_ppointUpper->Release();
	m_ppointUpper = NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::FContains
//
//	@doc:
//		Does bucket contain the point?
//
//---------------------------------------------------------------------------
BOOL
CBucket::FContains
	(
	const CPoint *ppoint
	)
	const
{
	// special case for singleton bucket
	if (FSingleton())
	{
		return m_ppointLower->FEqual(ppoint);
	}

	// special case if point equal to lower bound
	if (m_fLowerClosed && m_ppointLower->FEqual(ppoint))
	{
		return true;
	}

	// special case if point equal to upper bound
	if (m_fUpperClosed && m_ppointUpper->FEqual(ppoint))
	{
		return true;
	}

	return m_ppointLower->FLessThan(ppoint) && m_ppointUpper->FGreaterThan(ppoint);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::FBefore
//
//	@doc:
//		Is the point before the lower bound of the bucket
//
//---------------------------------------------------------------------------
BOOL
CBucket::FBefore
	(
	const CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(NULL != ppoint);

	return (m_fLowerClosed && m_ppointLower->FGreaterThan(ppoint)) || (!m_fLowerClosed && m_ppointLower->FGreaterThanOrEqual(ppoint));
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::FAfter
//
//	@doc:
//		Is the point after the upper bound of the bucket
//
//---------------------------------------------------------------------------
BOOL
CBucket::FAfter
	(
	const CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(NULL != ppoint);

	return ((m_fUpperClosed && m_ppointUpper->FLessThan(ppoint)) || (!m_fUpperClosed && m_ppointUpper->FLessThanOrEqual(ppoint)));
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::DOverlap
//
//	@doc:
//		What percentage of the bucket is covered by [lower bound, point]
//
//---------------------------------------------------------------------------
CDouble
CBucket::DOverlap
	(
	const CPoint *ppoint
	)
	const
{
	// special case of upper bound equal to ppoint
	if (this->PpUpper()->FLessThanOrEqual(ppoint))
	{
		return CDouble(1.0);
	}
	// if point is not contained, then no overlap
	if (!this->FContains(ppoint))
	{
		return CDouble(0.0);
	}

	// special case for singleton bucket
	if (FSingleton())
	{
		GPOS_ASSERT(this->m_ppointLower->FEqual(ppoint));

		return CDouble(1.0);
	}

	// general case, compute distance ratio
	CDouble dDistanceUpper = m_ppointUpper->DDistance(m_ppointLower);
	GPOS_ASSERT(dDistanceUpper > 0.0);
	CDouble dDistanceMiddle = ppoint->DDistance(m_ppointLower);
	GPOS_ASSERT(dDistanceMiddle >= 0.0);

	CDouble dRes = 1 / dDistanceUpper;
	if (dDistanceMiddle > 0.0)
	{
		dRes = dRes * dDistanceMiddle;
	}

	return CDouble(std::min(dRes.DVal(), DOUBLE(1.0)));

}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream&
CBucket::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "CBucket(";

	if (m_fLowerClosed)
	{
		os << " [";
	}
	else
	{
		os << " (";
	}

	m_ppointLower->OsPrint(os);
	os << ", ";
	m_ppointUpper->OsPrint(os);

	if (m_fUpperClosed)
	{
		os << "]";
	}
	else
	{
		os << ")";
	}

	os << " ";
	os << m_dFrequency << ", " << m_dDistinct ;
	os << ")";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketGreaterThan
//
//	@doc:
//		Construct new bucket with lower bound greater than given point and
//		the new bucket's upper bound is the upper bound of the current bucket
//---------------------------------------------------------------------------
CBucket *
CBucket::PbucketGreaterThan
	(
	IMemoryPool *pmp,
	CPoint *ppoint
	)
	const
{
	GPOS_ASSERT(FContains(ppoint));

	if (FSingleton() || PpUpper()->FEqual(ppoint))
	{
		return NULL;
	}

	CBucket *pbucketGT = NULL;
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	CPoint *ppointNew = CStatisticsUtils::PpointNext(pmp, pmda, ppoint);

	if (NULL != ppointNew)
	{
		if (FContains(ppointNew))
		{
			pbucketGT = PbucketScaleLower(pmp, ppointNew, true /* fIncludeLower */);
		}
		ppointNew->Release();
	}
	else
	{
		pbucketGT = PbucketScaleLower(pmp, ppoint,  false /* fIncludeLower */);
	}

	return pbucketGT;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketScaleUpper
//
//	@doc:
//		Create a new bucket that is a scaled down version
//		of this bucket with the upper boundary adjusted.
//
//---------------------------------------------------------------------------
CBucket*
CBucket::PbucketScaleUpper
	(
	IMemoryPool *pmp,
	CPoint *ppointUpperNew,
	BOOL fIncludeUpper
	)
	const
{
	GPOS_ASSERT(pmp);
	GPOS_ASSERT(ppointUpperNew);

	GPOS_ASSERT(this->FContains(ppointUpperNew));

	// scaling upper to be same as lower is identical to producing a singleton bucket
	if (this->m_ppointLower->FEqual(ppointUpperNew))
	{
		// invalid bucket, e.g. if bucket is [5,10) and
		// ppointUpperNew is 5 open, null should be returned
		if (false == fIncludeUpper)
		{
			return NULL;
		}
		return PbucketSingleton(pmp, ppointUpperNew);
	}

	CDouble dFrequencyNew = this->DFrequency();
	CDouble dDistinctNew = this->DDistinct();

	if (!this->m_ppointUpper->FEqual(ppointUpperNew))
	{
		CDouble dOverlap = this->DOverlap(ppointUpperNew);
		dFrequencyNew = dFrequencyNew * dOverlap;
		dDistinctNew = dDistinctNew * dOverlap;
	}


	// reuse the lower from this bucket
	this->m_ppointLower->AddRef();
	ppointUpperNew->AddRef();

	return GPOS_NEW(pmp) CBucket
						(
						this->m_ppointLower,
						ppointUpperNew,
						this->m_fLowerClosed,
						fIncludeUpper,
						dFrequencyNew,
						dDistinctNew
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketScaleLower
//
//	@doc:
//		Create a new bucket that is a scaled down version
//		of this bucket with the Lower boundary adjusted
//
//---------------------------------------------------------------------------
CBucket*
CBucket::PbucketScaleLower
	(
	IMemoryPool *pmp,
	CPoint *ppointLowerNew,
	BOOL fIncludeLower
	)
	const
{
	GPOS_ASSERT(pmp);
	GPOS_ASSERT(ppointLowerNew);

	GPOS_ASSERT(this->FContains(ppointLowerNew));

	// scaling lower to be same as upper is identical to producing a singleton bucket
	if (this->m_ppointUpper->FEqual(ppointLowerNew))
	{
		return PbucketSingleton(pmp, ppointLowerNew);
	}

	CDouble dFrequencyNew = this->DFrequency();
	CDouble dDistinctNew = this->DDistinct();

	if (!this->PpLower()->FEqual(ppointLowerNew))
	{
		CDouble dOverlap = CDouble(1.0) - this->DOverlap(ppointLowerNew);
		dFrequencyNew = this->DFrequency() * dOverlap;
		dDistinctNew = this->DDistinct() * dOverlap;
	}

	// reuse the lower from this bucket
	this->m_ppointUpper->AddRef();
	ppointLowerNew->AddRef();

	return GPOS_NEW(pmp) CBucket
						(
						ppointLowerNew,
						this->m_ppointUpper,
						fIncludeLower,
						this->m_fUpperClosed,
						dFrequencyNew,
						dDistinctNew
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketSingleton
//
//	@doc:
//		Create a new bucket that is a scaled down version
//		singleton
//
//---------------------------------------------------------------------------
CBucket*
CBucket::PbucketSingleton
	(
	IMemoryPool *pmp,
	CPoint *ppointSingleton
	)
	const
{
	GPOS_ASSERT(pmp);
	GPOS_ASSERT(ppointSingleton);
	GPOS_ASSERT(this->FContains(ppointSingleton));

	// assume that this point is one of the ndistinct values
	// in the bucket
	CDouble dDistinctRatio = CDouble(1.0) / this->m_dDistinct;

	CDouble dFrequencyNew = std::min(DOUBLE(1.0), (this->m_dFrequency * dDistinctRatio).DVal());
	CDouble dDistinctNew = CDouble(1.0);

	// singleton point is both lower and upper
	ppointSingleton->AddRef();
	ppointSingleton->AddRef();

	return GPOS_NEW(pmp) CBucket
						(
						ppointSingleton,
						ppointSingleton,
						true /* fLowerClosed */,
						true /* fUpperClosed */,
						dFrequencyNew,
						dDistinctNew
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketCopy
//
//	@doc:
//		Copy of bucket. Points are shared.
//
//---------------------------------------------------------------------------
CBucket *
CBucket::PbucketCopy
	(
	IMemoryPool *pmp
	)
{
	// reuse the points
	m_ppointLower->AddRef();
	m_ppointUpper->AddRef();

	return GPOS_NEW(pmp) CBucket(m_ppointLower, m_ppointUpper, m_fLowerClosed, m_fUpperClosed, m_dFrequency, m_dDistinct);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketUpdateFrequency
//
//	@doc:
//		Create copy of bucket with a copy of the bucket with updated frequency
//		based on the new total number of rows
//---------------------------------------------------------------------------
CBucket *
CBucket::PbucketUpdateFrequency
	(
	IMemoryPool *pmp,
	CDouble dRowsOld,
	CDouble dRowsNew
	)
{
	// reuse the points
	m_ppointLower->AddRef();
	m_ppointUpper->AddRef();

	CDouble dFrequencyNew = (this->m_dFrequency * dRowsOld) / dRowsNew;

	return GPOS_NEW(pmp) CBucket(m_ppointLower, m_ppointUpper, m_fLowerClosed, m_fUpperClosed, dFrequencyNew, m_dDistinct);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::ICompareLowerBounds
//
//	@doc:
//		Compare lower bounds of the buckets, return 0 if they match, 1 if
//		lb of bucket1 is greater than lb of bucket2 and -1 otherwise.
//
//---------------------------------------------------------------------------
INT
CBucket::ICompareLowerBounds
	(
	const CBucket *pbucket1,
	const CBucket *pbucket2
	)
{
	GPOS_ASSERT(NULL != pbucket1);
	GPOS_ASSERT(NULL != pbucket2);

	CPoint *ppoint1 = pbucket1->PpLower();
	CPoint *ppoint2 = pbucket2->PpLower();

	BOOL fClosedPoint1 = pbucket1->FLowerClosed();
	BOOL fClosedPoint2 = pbucket1->FLowerClosed();

	if (ppoint1->FEqual(ppoint2))
	{
		if (fClosedPoint1 == fClosedPoint2)
		{
			return 0;
		}

		if (fClosedPoint1)
		{
			// pbucket1 contains the lower bound (lb), while pbucket2 contain all
			// values between (lb + delta) and upper bound (ub)
			return -1;
		}

		return 1;
	}

	if (ppoint1->FLessThan(ppoint2))
	{
		return -1;
	}

	return 1;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::ICompareLowerBoundToUpperBound
//
//	@doc:
//		Compare lb of the first bucket to the ub of the second bucket,
//		return 0 if they match, 1 if lb of bucket1 is greater
//		than ub of bucket2 and -1 otherwise.
//---------------------------------------------------------------------------
INT
CBucket::ICompareLowerBoundToUpperBound
	(
	const CBucket *pbucket1,
	const CBucket *pbucket2
	)
{
	CPoint *ppoint1Lower = pbucket1->PpLower();
	CPoint *ppoint2Upper = pbucket2->PpUpper();

	if (ppoint1Lower->FGreaterThan(ppoint2Upper))
	{
		return 1;
	}

	if (ppoint1Lower->FLessThan(ppoint2Upper))
	{
		return -1;
	}

	// equal
	if (pbucket1->FLowerClosed() && pbucket2->FUpperClosed())
	{
		return 0;
	}

	return 1; // points not comparable
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::ICompareUpperBounds
//
//	@doc:
//		Compare upper bounds of the buckets, return 0 if they match, 1 if
//		ub of bucket1 is greater than that of bucket2 and -1 otherwise.
//
//---------------------------------------------------------------------------
INT
CBucket::ICompareUpperBounds
	(
	const CBucket *pbucket1,
	const CBucket *pbucket2
	)
{
	GPOS_ASSERT(NULL != pbucket1);
	GPOS_ASSERT(NULL != pbucket2);

	CPoint *ppoint1 = pbucket1->PpUpper();
	CPoint *ppoint2 = pbucket2->PpUpper();

	BOOL fClosedPoint1 = pbucket1->FUpperClosed();
	BOOL fClosedPoint2 = pbucket1->FUpperClosed();

	if (ppoint1->FEqual(ppoint2))
	{
		if (fClosedPoint1 == fClosedPoint2)
		{
			return 0;
		}

		if (fClosedPoint1)
		{
			// pbucket2 contains all values less than upper bound not including upper bound point
			// therefore pbucket1 upper bound greater than pbucket2 upper bound
			return 1;
		}

		return -1;
	}

	if (ppoint1->FLessThan(ppoint2))
	{
		return -1;
	}

	return 1;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::FIntersects
//
//	@doc:
//		Does this bucket intersect with another?
//
//---------------------------------------------------------------------------
BOOL
CBucket::FIntersects
	(
	const CBucket *pbucket
	)
	const
{

	if (this->FSingleton() && pbucket->FSingleton())
	{
		return PpLower()->FEqual(pbucket->PpLower());
	}

	if (this->FSingleton())
	{
		return pbucket->FContains(PpLower());
	}

	if (pbucket->FSingleton())
	{
		return FContains(pbucket->PpLower());
	}

	if (this->FSubsumes(pbucket) || pbucket->FSubsumes(this))
	{
		return true;
	}

	if (0 >= ICompareLowerBounds(this, pbucket))
	{
		// current bucket starts before the other bucket
		if (0 >= ICompareLowerBoundToUpperBound(pbucket, this))
		{
			// other bucket starts before current bucket ends
			return true;
		}

		return false;
	}
	
	if (0 >= ICompareLowerBoundToUpperBound(this, pbucket))
	{
		// current bucket starts before the other bucket ends
		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::FSubsumes
//
//	@doc:
//		Does this bucket subsume another?
//
//---------------------------------------------------------------------------
BOOL
CBucket::FSubsumes
	(
	const CBucket *pbucket
	)
	const
{

	// both are singletons
	if (this->FSingleton()
		&& pbucket->FSingleton())
	{
		return PpLower()->FEqual(pbucket->PpLower());
	}

	// other one is a singleton
	if (pbucket->FSingleton())
	{
		return this->FContains(pbucket->PpLower());
	}

	INT iPoint1LowerPoint2Lower = ICompareLowerBounds(this, pbucket);
	INT iPoint1UpperPoint2Upper = ICompareUpperBounds(this, pbucket);

	return (0 >= iPoint1LowerPoint2Lower && 0 <= iPoint1UpperPoint2Upper);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketIntersect
//
//	@doc:
//		Create a new bucket by intersecting with another
//		and return the percentage of each of the buckets that intersect.
// 		Points will be shared
//---------------------------------------------------------------------------
CBucket *
CBucket::PbucketIntersect
	(
	IMemoryPool *pmp,
	CBucket *pbucket,
	CDouble *pdFreqIntersect1,
	CDouble *pdFreqIntersect2
	)
	const
{
	// should only be called on intersecting bucket
	GPOS_ASSERT(FIntersects(pbucket));

	CPoint *ppNewLower = CPoint::PpointMax(this->PpLower(), pbucket->PpLower());
	CPoint *ppNewUpper = CPoint::PpointMin(this->PpUpper(), pbucket->PpUpper());

	BOOL fNewLowerClosed = true;
	BOOL fNewUpperClosed = true;

	CDouble dDistanceNew = 1.0;
	if (!ppNewLower->FEqual(ppNewUpper))
	{
		fNewLowerClosed = this->m_fLowerClosed;
		fNewUpperClosed = this->m_fUpperClosed;

		if (ppNewLower->FEqual(pbucket->PpLower()))
		{
			fNewLowerClosed = pbucket->FLowerClosed();
			if (ppNewLower->FEqual(this->PpLower()))
			{
				fNewLowerClosed = this->FLowerClosed() && pbucket->FLowerClosed();
			}
		}

		if (ppNewUpper->FEqual(pbucket->PpUpper()))
		{
			fNewUpperClosed = pbucket->FUpperClosed();
			if (ppNewUpper->FEqual(this->PpUpper()))
			{
				fNewUpperClosed = this->FUpperClosed() && pbucket->FUpperClosed();
			}
		}

		dDistanceNew = ppNewUpper->DDistance(ppNewLower);
	}

	// TODO: , May 1 2013, distance function for data types such as bpchar/varchar
	// that require binary comparison
	GPOS_ASSERT_IMP(!ppNewUpper->Pdatum()->FSupportsBinaryComp(ppNewLower->Pdatum()), dDistanceNew <= DWidth());
	GPOS_ASSERT_IMP(!ppNewUpper->Pdatum()->FSupportsBinaryComp(ppNewLower->Pdatum()), dDistanceNew <= pbucket->DWidth());

	CDouble dRatio1 = dDistanceNew / DWidth();
	CDouble dRatio2 = dDistanceNew / pbucket->DWidth();

	// edge case
	if (FSingleton() && pbucket->FSingleton())
	{
		dRatio1 = CDouble(1.0);
		dRatio2 = CDouble(1.0);
	}

	CDouble dDistinctNew
					(
					std::min
						(
						dRatio1.DVal() * m_dDistinct.DVal(),
						dRatio2.DVal() * pbucket->m_dDistinct.DVal()
						)
					);

	// For computing frequency, we assume that the bucket with larger number
	// of distinct values corresponds to the dimension. Therefore, selectivity
	// of the join is:
	// 1. proportional to the modified frequency values of both buckets
	// 2. inversely proportional to the max number of distinct values in both buckets

	CDouble dFreqIntersect1 = dRatio1 * m_dFrequency;
	CDouble dFreqIntersect2 = dRatio2 * pbucket->m_dFrequency;

	CDouble dFrequencyNew
					(
					dFreqIntersect1 *
					dFreqIntersect2 *
					DOUBLE(1.0) /
					std::max
						(
						dRatio1.DVal() * m_dDistinct.DVal(),
						dRatio2.DVal() * pbucket->DDistinct().DVal()
						)
					);

	ppNewLower->AddRef();
	ppNewUpper->AddRef();

	*pdFreqIntersect1 = dFreqIntersect1;
	*pdFreqIntersect2 = dFreqIntersect2;

	return GPOS_NEW(pmp) CBucket
						(
						ppNewLower,
						ppNewUpper,
						fNewLowerClosed,
						fNewUpperClosed,
						dFrequencyNew,
						dDistinctNew
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::DWidth
//
//	@doc:
//		Width of bucket
//
//---------------------------------------------------------------------------
CDouble
CBucket::DWidth() const
{
	if (FSingleton())
	{
		return CDouble(1.0);
	}
	else
	{
		return m_ppointUpper->DDistance(m_ppointLower);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::Difference
//
//	@doc:
//		Remove a bucket range. This produces an upper and lower split either
//		of which may be NULL.
//
//
//---------------------------------------------------------------------------
void
CBucket::Difference
	(
	IMemoryPool *pmp,
	CBucket *pbucketOther,
	CBucket **ppbucketLower,
	CBucket **ppbucketUpper
	)
{
	// we shouldn't be overwriting anything important
	GPOS_ASSERT(NULL == *ppbucketLower);
	GPOS_ASSERT(NULL == *ppbucketUpper);

	// if other bucket subsumes this bucket, then result is NULL, NULL
	if (pbucketOther->FSubsumes(this))
	{
		*ppbucketLower = NULL;
		*ppbucketUpper = NULL;
		return;
	}

	// if this bucket is below the other bucket, then return this, NULL
	if (this->FBefore(pbucketOther))
	{
		*ppbucketLower = this->PbucketCopy(pmp);
		*ppbucketUpper = NULL;
		return;
	}

	// if other bucket is "below" this bucket, then return NULL, this
	if (pbucketOther->FBefore(this))
	{
		*ppbucketLower = NULL;
		*ppbucketUpper = this->PbucketCopy(pmp);
		return;
	}

	// if other bucket's LB is after this bucket's LB, then we get a valid first split
	if (this->PpLower()->FLessThan(pbucketOther->PpLower()))
	{
		*ppbucketLower = this->PbucketScaleUpper(pmp, pbucketOther->PpLower(), !pbucketOther->FLowerClosed());
	}

	// if other bucket's UB is lesser than this bucket's LB, then we get a valid split
	if (pbucketOther->PpUpper()->FLessThan(this->PpUpper()))
	{
		*ppbucketUpper = this->PbucketScaleLower(pmp, pbucketOther->PpUpper(), !pbucketOther->FUpperClosed());
	}

	return;
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::FBefore
//
//	@doc:
//		Does this bucket occur before other? E.g. [1,2) is before [3,4)
//
//---------------------------------------------------------------------------
BOOL
CBucket::FBefore
	(
	const CBucket *pbucket
	)
	const
{
	if (this->FIntersects(pbucket))
	{
		return false;
	}

	return this->PpUpper()->FLessThanOrEqual(pbucket->PpLower());
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::FAfter
//
//	@doc:
//		Does this bucket occur after other? E.g. [2,4) is after [1,2)
//
//---------------------------------------------------------------------------
BOOL
CBucket::FAfter
	(
	const CBucket *pbucket
	)
	const
{
	if (this->FIntersects(pbucket))
	{
		return false;
	}

	return this->PpLower()->FGreaterThanOrEqual(pbucket->PpUpper());
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketMerge
//
//	@doc:
//		Merges with another bucket. Returns merged bucket that should be part
//		of the output. It also returns what is leftover from the merge.
//		E.g.
//		merge of [1,100) and [50,150) produces [1, 100), NULL, [100, 150)
//		merge of [1,100) and [50,75) produces [1, 75), [75,100), NULL
//		merge of [1,1) and [1,1) produces [1,1), NULL, NULL
//
//---------------------------------------------------------------------------
CBucket *
CBucket::PbucketMerge
	(
	IMemoryPool *pmp,
	CBucket *pbucketOther,
	CDouble dRows,
	CDouble dRowsOther,
	CBucket **ppbucket1New,
	CBucket **ppbucket2New,
	BOOL fUnionAll
	)
{
	// we shouldn't be overwriting anything important
	GPOS_ASSERT(NULL == *ppbucket1New);
	GPOS_ASSERT(NULL == *ppbucket2New);

	CPoint *ppLowerNew = CPoint::PpointMin(this->PpLower(), pbucketOther->PpLower());
	CPoint *ppUpperNew = CPoint::PpointMin(this->PpUpper(), pbucketOther->PpUpper());

	CDouble dOverlap = this->DOverlap(ppUpperNew);
	CDouble dDistinct = this->DDistinct() * dOverlap;
	CDouble dRowNew = dRows * this->DFrequency() * dOverlap;

	CDouble dFrequency = this->DFrequency() * this->DOverlap(ppUpperNew);
	if (fUnionAll)
	{
		CDouble dRowsOutput = (dRowsOther + dRows);
		CDouble dOverlapOther = pbucketOther->DOverlap(ppUpperNew);
		dDistinct = dDistinct + (pbucketOther->DDistinct() * dOverlapOther);
		dRowNew = dRowsOther * pbucketOther->DFrequency() * dOverlapOther;
		dFrequency = dRowNew / dRowsOutput;
	}

	BOOL fUpperClosed = ppLowerNew->FEqual(ppUpperNew);

	if (ppUpperNew->FLessThan(this->PpUpper()))
	{
		*ppbucket1New = this->PbucketScaleLower(pmp, ppUpperNew, !fUpperClosed);
	}

	if (ppUpperNew->FLessThan(pbucketOther->PpUpper()))
	{
		*ppbucket2New = pbucketOther->PbucketScaleLower(pmp, ppUpperNew, !fUpperClosed);
	}

	ppLowerNew->AddRef();
	ppUpperNew->AddRef();

	return GPOS_NEW(pmp) CBucket(ppLowerNew, ppUpperNew, true /* fLowerClosed */, fUpperClosed, dFrequency, dDistinct);
}

//---------------------------------------------------------------------------
//	@function:
//		CBucket::DSample
//
//	@doc:
//		Generate a random data point within bucket boundaries
//
//---------------------------------------------------------------------------
CDouble
CBucket::DSample
	(
	ULONG *pulSeed
	)
	const
{
	GPOS_ASSERT(FCanSample());

	IDatumStatisticsMappable *pdatumstatsmapableLower = dynamic_cast<IDatumStatisticsMappable *>(m_ppointLower->Pdatum());
	IDatumStatisticsMappable *pdatumstatsmapableUpper = dynamic_cast<IDatumStatisticsMappable *>(m_ppointUpper->Pdatum());

	DOUBLE dLowerVal = pdatumstatsmapableLower->DMappingVal().DVal();
	if (FSingleton())
	{
		return CDouble(dLowerVal);
	}

	DOUBLE dUpperVal = pdatumstatsmapableUpper->DMappingVal().DVal();
	DOUBLE dRandVal = ((DOUBLE) clib::UlRandR(pulSeed)) / RAND_MAX;

	return CDouble(dLowerVal + dRandVal * (dUpperVal - dLowerVal));
}


//---------------------------------------------------------------------------
//	@function:
//		CBucket::PbucketSingleton
//
//	@doc:
//		Create a new singleton bucket with the given datum as it lower
//		and upper bounds
//
//---------------------------------------------------------------------------
CBucket*
CBucket::PbucketSingleton
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
{
	GPOS_ASSERT(NULL != pdatum);

	pdatum->AddRef();
	pdatum->AddRef();

	return GPOS_NEW(pmp) CBucket
						(
						GPOS_NEW(pmp) CPoint(pdatum),
						GPOS_NEW(pmp) CPoint(pdatum),
						true /* fLowerClosed */,
						true /* fUpperClosed */,
						CDouble(1.0),
						CDouble(1.0)
						);
}


// EOF

