//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 Pivotal, Inc.
//
//	@filename:
//		CStatisticsUtils.h
//
//	@doc:
//		Utility functions for statistics
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatisticsUtils_H
#define GPOPT_CStatisticsUtils_H

#include "gpos/base.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CPartFilterMap.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/engine/CStatisticsConfig.h"

#include "naucrates/statistics/CStatsPredUtils.h"
#include "naucrates/statistics/CStatsPredDisj.h"
#include "naucrates/statistics/CStatsPredUnsupported.h"

#include "naucrates/base/IDatum.h"

namespace gpopt
{
	class CTableDescriptor;
}

namespace gpnaucrates
{

	using namespace gpos;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatisticsUtils
	//
	//	@doc:
	//		Utility functions for statistics
	//
	//---------------------------------------------------------------------------
	class CStatisticsUtils
	{

		private:

			// MCV value and its corresponding frequency, used for sorting MCVs
			struct SMcvPair
			{
				// MCV datum
				IDatum *m_pdatumMCV;

				// the frequency of this MCV
				CDouble m_dMcvFreq;

				// ctor
				SMcvPair
					(
					IDatum *pdatumMCV,
					CDouble dMcvFreq
					)
					:
					m_pdatumMCV(pdatumMCV),
					m_dMcvFreq(dMcvFreq)
				{}

				// dtor
				~SMcvPair()
				{
					m_pdatumMCV->Release();
				}
			};

			// array of SMcvVPairs
			typedef CDynamicPtrArray<SMcvPair, CleanupDelete> DrgPsmcvpair;

			// private ctor
			CStatisticsUtils();

			// private dtor
			virtual
			~CStatisticsUtils();

			// private copy ctor
			CStatisticsUtils(const CStatisticsUtils &);

			// given MCVs and histogram buckets, merge them into buckets of a single histogram
			static
			DrgPbucket *PdrgpbucketMergeBuckets
						(
						IMemoryPool *pmp,
						const DrgPbucket *pdrgpbucketMCV,
						const DrgPbucket *pdrgpbucketHist
						);

			// split a histogram bucket given an MCV bucket
			static
			DrgPbucket *PdrgpbucketSplitHistBucket
						(
						IMemoryPool *pmp,
						const CBucket *pbucketHist,
						const DrgPbucket *pdrgpbucketMCV
						);

			// given lower and upper bound information and their closedness, return a bucket if they can form a valid bucket
			static
			CBucket *PbucketCreateValidBucket
					(
					IMemoryPool *pmp,
					CPoint *ppointLower,
					CPoint *ppointUpper,
					BOOL fLowerClosed,
					BOOL fUpperClosed
					);

			// given lower and upper bound information and their closedness, test if they can form a valid bucket
			static
			BOOL FValidBucket
					(
					CPoint *ppointLower,
					CPoint *ppointUpper,
					BOOL fLowerClosed,
					BOOL fUpperClosed
					);

			// find the MCVs that fall within the same histogram bucket and perform the split
			static
			void SplitHistDriver
					(
					IMemoryPool *pmp,
					const CBucket *pbucketHist,
					const DrgPbucket *pdrgpbucketMCV,
					DrgPbucket *pdrgpbucketMerged,
					ULONG *pulMCVIdx,
					ULONG ulMCV
					);

			// distribute total dDistinct and dFrequency of the histogram bucket into the new buckets
			static
			void DistributeBucketProperties
					(
					CDouble dFrequencyTotal,
					CDouble dDistinctTotal,
					DrgPbucket *pdrgpbucket
					);

			// add the NDVs for all of the grouping columns
			static
			void AddNdvForAllGrpCols
					(
					IMemoryPool *pmp,
					const CStatistics *pstatsInput,
					const DrgPul *pdrgpulGrpCol,
					DrgPdouble *pdrgpdNDV // output array of NDV
					);

			// compute max number of groups when grouping on columns from the given source
			static
			CDouble DMaxGroupsFromSource
						(
						IMemoryPool *pmp,
						const CStatisticsConfig *pstatsconf,
						CStatistics *pstatsInput,
						const DrgPul *pdrgpulPerSrc
						);

			// check to see if any one of the grouping columns has been capped
			static
			BOOL FExistsCappedGrpCol(const CStatistics *pstats, const DrgPul *pdrgpulGrpCol);

			// return the maximum NDV given an array of grouping columns
			static
			CDouble DMaxNdv(const CStatistics *pstats,const DrgPul *pdrgpulGrpCol);

		public:

			// get the next data point for generating new bucket boundary
			static
			CPoint *PpointNext
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CPoint *ppoint
				);

			// transform mcv information to optimizer's histogram structure
			static
			CHistogram *PhistTransformMCV
				(
				IMemoryPool *pmp,
				const IMDType *pmdtype,
				DrgPdatum *pdrgpdatumMCV,
				DrgPdouble *pdrgpdFreq,
				ULONG ulNumMCVValues
				);

			// merge MCVs and histogram
			static
			CHistogram *PhistMergeMcvHist
				(
				IMemoryPool *pmp,
				const CHistogram *phistGPDBMcv,
				const CHistogram *phistGPDBHist
				);

			// comparison function for sorting MCVs
			static
			inline INT IMcvPairCmp(const void *pv1, const void *pv2);

			// utility function to print column stats before/after applying filter
			static
			void PrintColStats
					(
					IMemoryPool *pmp,
					CStatsPred *pstatspred,
					ULONG ulColIdCond,
					CHistogram *phist,
					CDouble dScaleFactorLast,
					BOOL fBefore
					);

			// extract all the column identifiers used in the statistics filter
			static
			void ExtractUsedColIds
					(
					IMemoryPool *pmp,
					CBitSet *pbsColIds, 
					CStatsPred *pstatspred,
					DrgPul *pdrgpulColIds
					);

			// given the previously generated histogram, update the intermediate
			// result of the disjunction
			static
			void UpdateDisjStatistics
					(
					IMemoryPool *pmp,
					CBitSet *pbsStatsNonUpdateableCols,
					CDouble dRowsInputDisj,
					CDouble dRowsLocal,
					CHistogram *phistPrev,
					HMUlHist *phmulhistResultDisj,
					ULONG ulColIdLast
					);

			// given a disjunction filter, generate a bit set of columns whose
			// histogram buckets cannot be changed by applying the predicates in the
			// disjunction
			static
			CBitSet *PbsNonUpdatableHistForDisj
						(
						IMemoryPool *pmp,
						CStatsPredDisj *pstatspred
						);

			// helper method to add a histogram to a map
			static
			void AddHistogram
					(
					IMemoryPool *pmp,
					ULONG ulColId,
					const CHistogram *phist,
					HMUlHist *phmulhist,
					BOOL fReplaceOld = false
					);

			// create a new hash map of histograms after merging
			// the histograms generated by the child of disjunctive predicate
			static
			HMUlHist *PhmulhistMergeAfterDisjChild
						(
						IMemoryPool *pmp,
						CBitSet *pbsStatsNonUpdateableCols,
						HMUlHist *phmulhistPrev,
						HMUlHist *phmulhistDisjChild,
						CDouble dRowsCummulative,
						CDouble dRowsDisjChild
						);

			// helper method to copy the hash map of histograms
			static
			HMUlHist *PhmulhistCopy(IMemoryPool *pmp, HMUlHist *phmulhist);

			// return the column identifier of the filter if the predicate is
			// on a single column else	return gpos::ulong_max
			static
			ULONG UlColId(const DrgPstatspred *pdrgpstatspred);

			// add remaining buckets from one array of buckets to the other
			static
			void AddRemainingBuckets
					(
					IMemoryPool *pmp,
					const DrgPbucket *pdrgpbucketSrc,
					DrgPbucket *pdrgpbucketDest,
					ULONG *pulStart
					);

			// generate a null datum with the type of passed colref
			static
			IDatum *PdatumNull(const CColRef *pcr);

#ifdef GPOS_DEBUG
			// helper method to print the hash map of histograms
			static
			void PrintHistogramMap(IOstream &os, HMUlHist *phmulhist);
#endif // GPOS_DEBUG

			// derive statistics of dynamic scan based on part-selector stats in the given map
			static
			IStatistics *PstatsDynamicScan(IMemoryPool *pmp, CExpressionHandle &exprhdl, ULONG ulPartIndexId, CPartFilterMap *ppfm);

			// derive statistics of (dynamic) index-get
			static
			IStatistics *PstatsIndexGet(IMemoryPool *pmp, CExpressionHandle &exprhdl, DrgPstat *pdrgpstatCtxt);

			// derive statistics of bitmap table-get
			static
			IStatistics *PstatsBitmapTableGet(IMemoryPool *pmp, CExpressionHandle &exprhdl, DrgPstat *pdrgpstatCtxt);

			// compute the cumulative number of distinct values (NDV) of the group by operator
			// from the array of NDV of the individual grouping columns
			static
			CDouble DNumOfDistinctVal(const CStatisticsConfig *pstatsconf, DrgPdouble *pdrgpdNDV);

			// return the mapping between the table column used for grouping to the logical operator id where it was defined.
			// If the grouping column is not a table column then the logical op id is initialized to gpos::ulong_max
			static
			HMUlPdrgpul *PhmpuldrgpulTblOpIdToGrpColsMap
							(
							IMemoryPool *pmp,
							CStatistics *pstats,
							const CColRefSet *pcrsGrpCols,
							CBitSet *pbsKeys
							);

			// extract NDVs for the given array of grouping columns
			static
			DrgPdouble *PdrgPdoubleNDV
				(
				IMemoryPool *pmp,
				const CStatisticsConfig *pstatsconf,
				const IStatistics *pstats,
				CColRefSet *pcrsGrpCols, // grouping columns
				CBitSet *pbsKeys // keys derived during optimization
				);

			// compute the cumulative number of groups for the given set of grouping columns
			static
			CDouble DGroups
					(
					IMemoryPool *pmp, 
					IStatistics *pstats, 
					const CStatisticsConfig *pstatsconf,
					DrgPul *pdrgpulGC, 
					CBitSet *pbsKeys
					);
			
			// return the default number of distinct values
			static
			CDouble DDefaultDistinctVals
					(
					CDouble dRows
					)
			{
				return std::min(CStatistics::DDefaultDistinctValues.DVal(), dRows.DVal());
			}

			// add the statistics (histogram and width) of the grouping columns
			static
			void AddGrpColStats
					(
					IMemoryPool *pmp,
					const CStatistics *pstatsInput,
					CColRefSet *pcrsGrpCols,
					HMUlHist *phmulhistOutput,
					HMUlDouble *phmuldoubleWidthOutput
					);

			// return the set of grouping columns for statistics computation;
			static
			CColRefSet *PcrsGrpColsForStats
							(
							IMemoryPool *pmp, 
							const DrgPul *pdrgpulGrpCol, 
							CColRefSet *pcrsGrpColComputed // output set of grouping columns that are computed attributes
							);



			// return the total number of distinct values in the given array of buckets
			static
			CDouble DDistinct(const DrgPbucket *pdrgppbucket);

			// return the cumulative frequency in the given array of buckets
			static
			CDouble DFrequency(const DrgPbucket *pdrgppbucket);

			// true if the given operator increases risk of cardinality misestimation
			static
			BOOL FIncreasesRisk(CLogical *popLogical);

			// return the default column width
			static
			CDouble DDefaultColumnWidth(const IMDType *pmdtype);

			// helper method to add width information
			static
			void AddWidthInfo(IMemoryPool *pmp, HMUlDouble *phmuldoubleSrc, HMUlDouble *phmuldoubleDest);


			// for the output stats object, compute its upper bound cardinality mapping based on the bounding method
			// estimated output cardinality and information maintained in the current stats object
			static
			void ComputeCardUpperBounds
				(
				IMemoryPool *pmp, // memory pool
				const CStatistics *pstatsInput,
				CStatistics *pstatsOutput, // output statistics object that is to be updated
				CDouble dRowsOutput, // estimated output cardinality of the operator
				CStatistics::ECardBoundingMethod ecbm // technique used to estimate max source cardinality in the output stats object
				);

	}; // class CStatisticsUtils

	// comparison function for sorting MCVs
	INT CStatisticsUtils::IMcvPairCmp
		(
		const void *pv1,
		const void *pv2
		)
	{
		GPOS_ASSERT(NULL != pv1);
		GPOS_ASSERT(NULL != pv2);
		const SMcvPair *psmcvpair1 = *(const SMcvPair **) (pv1);
		const SMcvPair *psmcvpair2 = *(const SMcvPair **) (pv2);

		const IDatum *pdatum1 = psmcvpair1->m_pdatumMCV;
		const IDatum *pdatum2 = psmcvpair2->m_pdatumMCV;

		if (pdatum1->FStatsEqual(pdatum2))
		{
			return 0;
		}

		if (pdatum1->FStatsComparable(pdatum2) && pdatum1->FStatsLessThan(pdatum2))
		{
			return -1;
		}

		return 1;
	}
}


#endif // !GPOPT_CStatisticsUtils_H

// EOF
