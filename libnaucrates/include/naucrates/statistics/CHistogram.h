//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CHistogram.h
//
//	@doc:
//		Histogram implementation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CHistogram_H
#define GPNAUCRATES_CHistogram_H

#include "gpos/base.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CStatsPred.h"

namespace gpopt
{
	class CColRef;
}

namespace gpnaucrates
{
	// type definitions
	// array of doubles
	typedef CDynamicPtrArray<CDouble, CleanupDelete> DrgPdouble;

	//---------------------------------------------------------------------------
	//	@class:
	//		CHistogram
	//
	//	@doc:
	//
	//---------------------------------------------------------------------------
	class CHistogram
	{

		private:
			// all the buckets in the histogram
			DrgPbucket *m_pdrgppbucket;

			// well-defined histogram. if false, then bounds are unknown
			BOOL m_fWellDefined;

			// null fraction
			CDouble m_dNullFreq;

			// ndistinct of tuples not covered in the buckets
			CDouble m_dDistinctRemain;

			// frequency of tuples not covered in the buckets
			CDouble m_dFreqRemain;

			// has histogram skew been measures
			BOOL m_fSkewMeasured;

			// skew estimate
			CDouble m_dSkew;

			// was the NDVs in histogram scaled
			BOOL m_fNDVScaled;

			// is column statistics missing in the database
			BOOL m_fColStatsMissing;

			// private copy ctor
			CHistogram(const CHistogram &);

			// private assignment operator
			CHistogram& operator=(const CHistogram &);

			// return an array buckets after applying equality filter on the histogram buckets
			DrgPbucket *PdrgppbucketEqual(IMemoryPool *pmp, CPoint *ppoint) const;

			// return an array buckets after applying non equality filter on the histogram buckets
			DrgPbucket *PdrgppbucketNEqual(IMemoryPool *pmp, CPoint *ppoint) const;

			// less than or less than equal filter
			CHistogram *PhistLessThanOrLessThanEqual(IMemoryPool *pmp, CStatsPred::EStatsCmpType escmpt, CPoint *ppoint) const;

			// greater than or greater than equal filter
			CHistogram *PhistGreaterThanOrGreaterThanEqual(IMemoryPool *pmp, CStatsPred::EStatsCmpType escmpt, CPoint *ppoint) const;

			// equal filter
			CHistogram *PhistEqual(IMemoryPool *pmp, CPoint *ppoint) const;

			// not equal filter
			CHistogram *PhistNEqual(IMemoryPool *pmp, CPoint *ppoint) const;

			// IDF filter
			CHistogram *PhistIDF(IMemoryPool *pmp, CPoint *ppoint) const;

			// INDF filter
			CHistogram *PhistINDF(IMemoryPool *pmp, CPoint *ppoint) const;

			// equality join
			CHistogram *PhistJoinEquality(IMemoryPool *pmp, const CHistogram *phist) const;

			// generate histogram based on NDV
			CHistogram *PhistJoinEqualityNDV(IMemoryPool *pmp, const CHistogram *phist) const;

			// construct a new histogram for an INDF join predicate
			CHistogram *PhistJoinINDF(IMemoryPool *pmp, const CHistogram *phist) const;

			// accessor for n-th bucket
			CBucket *operator [] (ULONG) const;

			// compute skew estimate
			void ComputeSkew();

			// helper to add buckets from one histogram to another
			static
			void AddBuckets
					(
					IMemoryPool *pmp,
					DrgPbucket *pdrgppbucketSrc,
					DrgPbucket *pdrgppbucketDest,
					CDouble dRowsOld,
					CDouble dRowsNew,
					ULONG ulBegin,
					ULONG ulEnd
					);

			static
			void AddBuckets
					(
					IMemoryPool *pmp,
					DrgPbucket *pdrgppbucketSrc,
					DrgPbucket *pdrgppbucketDest,
					CDouble dRows,
					DrgPdouble *pdrgpdouble,
					ULONG ulBegin,
					ULONG ulEnd
					);

			// check if we can compute NDVRemain for JOIN histogram for the given input histograms
			static
			BOOL FCanComputeJoinNDVRemain(const CHistogram *phist1, const CHistogram *phist2);

			// compute the effects of the NDV and frequency of the tuples not captured
			// by the histogram
			static
			void ComputeJoinNDVRemainInfo
					(
					const CHistogram *phist1,
					const CHistogram *phist2,
					DrgPbucket *pdrgppbucketJoin, // join buckets
					CDouble dFreqJoinBuckets1, // frequency of the buckets in input1 that contributed to the join
					CDouble dFreqJoinBuckets2, // frequency of the buckets in input2 that contributed to the join
					CDouble *pdDistinctRemain,
					CDouble *pFreqRemain
					);

		public:

			// ctors
			explicit
			CHistogram(DrgPbucket *pdrgppbucket, BOOL fWellDefined = true);

			CHistogram
					(
					DrgPbucket *pdrgppbucket,
					BOOL fWellDefined,
					CDouble dNullFreq,
					CDouble dDistinctRemain,
					CDouble dFreqRemain,
					BOOL fColStatsMissing = false
					);

			// set null frequency
			void SetNullFrequency(CDouble dNullFreq);

			// set information about the scaling of NDVs
			void SetNDVScaled()
			{
				m_fNDVScaled = true;
			}

			// have the NDVs been scaled
			BOOL FScaledNDV() const
			{
				return m_fNDVScaled;
			}

			// filter by comparing with point
			CHistogram *PhistFilter
						(
						IMemoryPool *pmp,
						CStatsPred::EStatsCmpType escmpt,
						CPoint *ppoint
						)
						const;

			// filter by comparing with point and normalize
			CHistogram *PhistFilterNormalized
						(
						IMemoryPool *pmp,
						CStatsPred::EStatsCmpType escmpt,
						CPoint *ppoint,
						CDouble *pdScaleFactor
						)
						const;

			// join with another histogram
			CHistogram *PhistJoin
						(
						IMemoryPool *pmp,
						CStatsPred::EStatsCmpType escmpt,
						const CHistogram *phist
						)
						const;

			// LASJ with another histogram
			CHistogram *PhistLASJ
						(
						IMemoryPool *pmp,
						CStatsPred::EStatsCmpType escmpt,
						const CHistogram *phist
						)
						const;

			// join with another histogram and normalize it.
			// If the join is not an equality join the function returns an empty histogram
			CHistogram *PhistJoinNormalized
						(
						IMemoryPool *pmp,
						CStatsPred::EStatsCmpType escmpt,
						CDouble dRows,
						const CHistogram *phistOther,
						CDouble dRowsOther,
						CDouble *pdScaleFactor
						)
						const;

			// scale factor of inequality (!=) join
			CDouble DInEqualityJoinScaleFactor
						(
						IMemoryPool *pmp,
						CDouble dRows,
						const CHistogram *phistOther,
						CDouble dRowsOther
						)
						const;

			// left anti semi join with another histogram and normalize
			CHistogram *PhistLASJoinNormalized
						(
						IMemoryPool *pmp,
						CStatsPred::EStatsCmpType escmpt,
						CDouble dRows,
						const CHistogram *phistOther,
						CDouble *pdScaleFactor,
						BOOL fIgnoreLasjHistComputation // except for the case of LOJ cardinality estimation this flag is always
						                                // "true" since LASJ stats computation is very aggressive
						)
						const;

			// group by and normalize
			CHistogram *PhistGroupByNormalized
						(
						IMemoryPool *pmp,
						CDouble dRows,
						CDouble *pdScaleFactor
						)
						const;

			// union all and normalize
			CHistogram *PhistUnionAllNormalized
						(
						IMemoryPool *pmp,
						CDouble dRows,
						const CHistogram *phistOther,
						CDouble dRowsOther
						)
						const;

			// union and normalize
			CHistogram *PhistUnionNormalized
						(
						IMemoryPool *pmp,
						CDouble dRows,
						const CHistogram *phistOther,
						CDouble dRowsOther,
						CDouble *pdRowsOutput
						)
						const;

			// cleanup residual buckets
			void CleanupResidualBucket(CBucket *pbucket, BOOL fbucketResidual) const;

			// get the next bucket for union / union all
			CBucket *PbucketNext
					(
					const CHistogram *phist,
					CBucket *pbucketNew,
					BOOL *pfbucket2Residual,
					ULONG *pulBucketIndex
					)
					const;

			// create a new histogram with updated bucket frequency
			CHistogram *PhistUpdatedFrequency
						(
						IMemoryPool *pmp,
						DrgPbucket *pdrgppbucket,
						DrgPdouble *pdrgpdouble,
						CDouble *pdRowsOutput,
						CDouble dNullRows,
						CDouble dNDVRemain,
						CDouble dNDVRemainRows
						)
						const;
				
			// add residual union all buckets after the merge
			ULONG UlAddResidualUnionAllBucket
				(
				IMemoryPool *pmp,
				DrgPbucket *pdrgppbucket,
				CBucket *pbucket,
				CDouble dRowsOld,
				CDouble dRowsNew,
				BOOL fbucketResidual,
				ULONG ulIndex
				)
				const;

			// add residual union buckets after the merge
			ULONG UlAddResidualUnionBucket
				(
				IMemoryPool *pmp,
				DrgPbucket *pdrgppbucket,
				CBucket *pbucket,
				CDouble dRows,
				BOOL fbucketResidual,
				ULONG ulIndex,
				DrgPdouble *pdrgpdouble
				)
				const;

			// number of buckets
			ULONG UlBuckets() const
			{
				return m_pdrgppbucket->UlLength();
			}

			// buckets accessor
			const DrgPbucket *Pdrgpbucket() const
			{
				return m_pdrgppbucket;
			}

			// well defined
			BOOL FWellDefined() const
			{
				return m_fWellDefined;
			}

			// is the column statistics missing in the database
			BOOL FColStatsMissing() const
			{
				return m_fColStatsMissing;
			}

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// total frequency from buckets and null fraction
			CDouble DFrequency() const;

			// total number of distinct values
			CDouble DDistinct() const;

			// is histogram well formed
			BOOL FValid() const;

			// return copy of histogram
			CHistogram *PhistCopy(IMemoryPool *pmp) const;

			// destructor
			virtual
			~CHistogram()
			{
				m_pdrgppbucket->Release();
			}

			// normalize histogram and return scaling factor
			CDouble DNormalize();

			// is histogram normalized
			BOOL FNormalized() const;

			// translate the histogram into a derived column stats
			CDXLStatsDerivedColumn *Pdxlstatsdercol
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				ULONG ulColId,
				CDouble dWidth
				)
				const;

			// randomly pick a bucket index
			ULONG UlRandomBucketIndex(ULONG *pulSeed) const;

			// estimate of data skew
			CDouble DSkew()
			{
				if (!m_fSkewMeasured)
				{
					ComputeSkew();
				}

				return m_dSkew;
			}

			// accessor of null fraction
			CDouble DNullFreq() const
			{
				return m_dNullFreq;
			}

			// accessor of remaining number of tuples
			CDouble DDistinctRemain() const
			{
				return m_dDistinctRemain;
			}

			// accessor of remaining frequency
			CDouble DFreqRemain() const
			{
				return m_dFreqRemain;
			}

			// check if histogram is empty
			BOOL FEmpty() const;

			// cap the total number of distinct values (NDVs) in buckets to the number of rows
			void CapNDVs(CDouble dRows);

			// is comparison type supported for filters
			static
			BOOL FSupportsFilter(CStatsPred::EStatsCmpType escmpt);

			// is the join predicate's comparison type supported
			static
			BOOL FSupportsJoinPred(CStatsPred::EStatsCmpType escmpt);

			// create the default histogram for a given column reference
			static
			CHistogram *PhistDefault(IMemoryPool *pmp, CColRef *pcr, BOOL fEmpty);

			// create the default non empty histogram for a boolean column
			static
			CHistogram *PhistDefaultBoolColStats(IMemoryPool *pmp);

			// default histogram selectivity
			static const CDouble DDefaultSelectivity;

			// minimum number of distinct values in a column
			static const CDouble DMinDistinct;

			// default scale factor when there is no filtering of input
			static const CDouble DNeutralScaleFactor;

			// default Null frequency
			static const CDouble DDefaultNullFreq;

			// default NDV remain
			static const CDouble DDefaultNDVRemain;

			// default frequency of NDV remain
			static const CDouble DDefaultNDVFreqRemain;
	}; // class CHistogram

}

#endif // !GPNAUCRATES_CHistogram_H

// EOF
