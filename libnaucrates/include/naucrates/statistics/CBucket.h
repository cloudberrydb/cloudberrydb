//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CBucket.h
//
//	@doc:
//		Bucket in a histogram
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPNAUCRATES_CBucket_H
#define GPNAUCRATES_CBucket_H

#include "gpos/base.h"
#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/IBucket.h"

#define GPOPT_BUCKET_DEFAULT_FREQ 1.0
#define GPOPT_BUCKET_DEFAULT_DISTINCT 1.0

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpmd;

	// forward decl
	class CBucket;

	// dynamic array of buckets
	typedef CDynamicPtrArray<CBucket, CleanupDelete> DrgPbucket;

	//---------------------------------------------------------------------------
	//	@class:
	//		CBucket
	//
	//	@doc:
	//		Represents a bucket in a histogram
	//
	//---------------------------------------------------------------------------

	class CBucket: public IBucket
	{
		private:
			// lower bound of bucket
			CPoint *m_ppointLower;

			// upper bound of bucket
			CPoint *m_ppointUpper;

			// is lower bound closed (does bucket include boundary value)
			BOOL m_fLowerClosed;

			// is upper bound closed (does bucket includes boundary value)
			BOOL m_fUpperClosed;

			// frequency corresponding to bucket
			CDouble m_dFrequency;

			// number of distinct elements in bucket
			CDouble m_dDistinct;

			// private copy constructor
			CBucket(const CBucket &);

			// private assignment operator
			CBucket& operator=(const CBucket &);

		public:

			// ctor
			CBucket
				(
				CPoint *ppointLower, 
				CPoint *ppointUpper,
				BOOL fLowerClosed,
				BOOL fUpperClosed,
				CDouble dFrequency, 
				CDouble dDistinct
				);
			
			// dtor
			virtual
			~CBucket();

			// does bucket contain point
			BOOL FContains(const CPoint *ppoint) const;

			// is the point before the lower bound of the bucket
			BOOL FBefore(const CPoint *ppoint) const;

			// is the point after the upper bound of the bucket
			BOOL FAfter(const CPoint *ppoint) const;

			// what percentage of bucket is covered by [lb,pp]
			CDouble DOverlap(const CPoint *ppoint) const;

			// frequency associated with bucket
			CDouble DFrequency() const
			{
				return m_dFrequency;
			}

			// width of bucket
			CDouble DWidth() const;

			// set frequency
			void SetFrequency
				(
				CDouble dFrequency
				)
			{
				GPOS_ASSERT(CDouble(0) <= dFrequency);
				GPOS_ASSERT(CDouble(1.0) >= dFrequency);
				m_dFrequency = dFrequency;
			}

			// set number of distinct values
			void SetDistinct
				(
				CDouble dDistinct
				)
			{
				GPOS_ASSERT(CDouble(0) <= dDistinct);
				m_dDistinct = dDistinct;
			}

			// number of distinct values in bucket
			CDouble DDistinct() const
			{
				return m_dDistinct;
			}

			// lower point
			CPoint *PpLower() const
			{
				return m_ppointLower;
			}

			// upper point
			CPoint *PpUpper() const
			{
				return m_ppointUpper;
			}

			// is lower bound closed (does bucket includes boundary value)
			BOOL FLowerClosed() const
			{
				return m_fLowerClosed;
			}

			// is upper bound closed (does bucket includes boundary value)
			BOOL FUpperClosed() const
			{
				return m_fUpperClosed;
			}

			// does bucket's range intersect another's
			BOOL FIntersects(const CBucket *pbucket) const;

			// does bucket's range subsume another's
			BOOL FSubsumes(const CBucket *pbucket) const;

			// does bucket occur before another
			BOOL FBefore(const CBucket *pbucket) const;

			// does bucket occur after another
			BOOL FAfter(const CBucket *pbucket) const;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// construct new bucket with lower bound greater than given point
			CBucket *PbucketGreaterThan(IMemoryPool *pmp, CPoint *ppoint) const;

			// scale down version of bucket adjusting upper boundary
			CBucket *PbucketScaleUpper(IMemoryPool *pmp, CPoint *ppointUpper, BOOL fIncludeUpper) const;

			// scale down version of bucket adjusting lower boundary
			CBucket *PbucketScaleLower(IMemoryPool *pmp, CPoint *ppointLower, BOOL fIncludeLower) const;

			// extract singleton bucket at given point
			CBucket *PbucketSingleton(IMemoryPool *pmp, CPoint *ppointSingleton) const;

			// create a new bucket by intersecting with another and return the percentage of each of the buckets that intersect
			CBucket *PbucketIntersect(IMemoryPool *pmp, CBucket *pbucket, CDouble *pdFreqIntersect1, CDouble *pdFreqIntersect2) const;

			// Remove a bucket range. This produces lower and upper split
			void Difference(IMemoryPool *pmp, CBucket *pbucketOther, CBucket **pbucketLower, CBucket **pbucketUpper);

			// return copy of bucket
			CBucket *PbucketCopy(IMemoryPool *pmp);

			// return a copy of the bucket with updated frequency based on the new total number of rows
			CBucket *PbucketUpdateFrequency(IMemoryPool *pmp, CDouble dRowsOld, CDouble dRowsNew);

			// Merge with another bucket and return leftovers
			CBucket *PbucketMerge
					(
					IMemoryPool *pmp,
					CBucket *pbucketOther,
					CDouble dRows,
					CDouble dRowsOther,
					CBucket **pbucket1New,
					CBucket **pbucket2New,
					BOOL fUnionAll = true
					);

			// does bucket support sampling
			BOOL FCanSample() const
			{
				return m_ppointLower->Pdatum()->FStatsMappable();
			}

			// generate a random data point within bucket boundaries
			CDouble DSample(ULONG *pulSeed) const;

			// compare lower bucket boundaries
			static
			INT ICompareLowerBounds(const CBucket *pbucket1, const CBucket *pbucket2);

			// compare upper bucket boundaries
			static
			INT ICompareUpperBounds(const CBucket *pbucket1, const CBucket *pbucket2);
			
			// compare lower bound of first bucket to upper bound of second bucket
			static
			INT ICompareLowerBoundToUpperBound(const CBucket *pbucket1, const CBucket *pbucket2);

			// create a new singleton bucket with the given datum as it lower and upper bounds
			static
			CBucket *PbucketSingleton(IMemoryPool *pmp, IDatum *pdatum);
	};
}

#endif // !GPNAUCRATES_CBucket_H

// EOF
