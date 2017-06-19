//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CHistogramUtils.h
//
//	@doc:
//		Utility functions that operate on histograms
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CHistogramUtils_H
#define GPNAUCRATES_CHistogramUtils_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/sync/CMutex.h"

#include "naucrates/statistics/CStatsPredDisj.h"
#include "naucrates/statistics/CStatsPredConj.h"
#include "naucrates/statistics/CStatsPredLike.h"
#include "naucrates/statistics/CStatsPredUnsupported.h"

#include "naucrates/statistics/CHistogram.h"
#include "gpos/common/CBitSet.h"

namespace gpopt
{
	class CStatisticsConfig;
	class CColumnFactory;
}

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpopt;

	//---------------------------------------------------------------------------
	//	@class:
	//		CHistogramUtils
	//
	//	@doc:
	//		Utility functions that operate on histograms
	//---------------------------------------------------------------------------
	class CHistogramUtils
	{
		public:

			// helper method to append histograms from one map to the other
			static
			void AddHistograms(IMemoryPool *pmp, HMUlHist *phmulhistSrc, HMUlHist *phmulhistDest);

			// add dummy histogram buckets and column width for the array of columns
			static
			void AddDummyHistogramAndWidthInfo
				(
				IMemoryPool *pmp,
				CColumnFactory *pcf,
				HMUlHist *phmulhistOutput,
				HMUlDouble *phmuldoubleWidthOutput,
				const DrgPul *pdrgpul,
				BOOL fEmpty
				);

			// add dummy histogram buckets for the columns in the input histogram
			static
			void AddEmptyHistogram(IMemoryPool *pmp, HMUlHist *phmulhistOutput, HMUlHist *phmulhistInput);

			// create a new histogram after applying the filter that is not an AND/OR predicate
			static
			CHistogram *PhistSimpleFilter
				(
				IMemoryPool *pmp,
				CStatsPred *pstatspred,
				CBitSet *pbsFilterColIds,
				CHistogram *phistBefore,
				CDouble *pdScaleFactorLast,
				ULONG *pulColIdLast
				);

			// create a new histogram after applying a point filter
			static
			CHistogram *PhistPointFilter
				(
				IMemoryPool *pmp,
				CStatsPredPoint *pstatspred,
				CBitSet *pbsFilterColIds,
				CHistogram *phistBefore,
				CDouble *pdScaleFactorLast,
				ULONG *pulColIdLast
				);

			// create a new histogram after applying a LIKE filter
			static
			CHistogram *PhistLikeFilter
				(
				IMemoryPool *pmp,
				CStatsPredLike *pstatspred,
				CBitSet *pbsFilterColIds,
				CHistogram *phistBefore,
				CDouble *pdScaleFactorLast,
				ULONG *pulColIdLast
				);

			// create a new histogram for an unsupported predicate
			static
			CHistogram *PhistUnsupportedPred
				(
				IMemoryPool *pmp,
				CStatsPredUnsupported *pstatspred,
				CBitSet *pbsFilterColIds,
				CHistogram *phistBefore,
				CDouble *pdScaleFactorLast,
				ULONG *pulColIdLast
				);

			// create a new hash map of histograms after applying a conjunctive or disjunctive filter
			static
			HMUlHist *PhmulhistApplyConjOrDisjFilter
				(
				IMemoryPool *pmp,
				CStatisticsConfig *pstatsconf,
				HMUlHist *phmulhistInput,
				CDouble dRowsInput,
				CStatsPred *pstatspred,
				CDouble *pdScaleFactor
				);

			// create new hash map of histograms after applying the conjunction predicate
			static
			HMUlHist *PhmulhistApplyConjFilter
				(
				IMemoryPool *pmp,
				CStatisticsConfig *pstatsconf,
				HMUlHist *phmulhistIntermediate,
				CDouble dRowsInput,
				CStatsPredConj *pstatspredConj,
				CDouble *pdScaleFactor
				);

			// create new hash map of histograms after applying the disjunctive predicate
			static
			HMUlHist *PhmulhistApplyDisjFilter
				(
				IMemoryPool *pmp,
				CStatisticsConfig *pstatsconf,
				HMUlHist *phmulhistInput,
				CDouble dRowsInput,
				CStatsPredDisj *pstatspred,
				CDouble *pdScaleFactor
				);

			// check if the column is a new column for statistic calculation
			static
			BOOL FNewStatsColumn(ULONG ulColId, ULONG ulColIdLast);

			// check if the cardinality estimation should be done only via NDVs
			static
			BOOL FNDVBasedCardEstimation(const CHistogram *phist);

	}; // class CHistogramUtils
}

#endif // !GPNAUCRATES_CHistogramUtils_H

// EOF
