//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CJoinStatsProcessor.h
//
//	@doc:
//		Compute statistics for all joins
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CJoinStatsProcessor_H
#define GPNAUCRATES_CJoinStatsProcessor_H

#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CGroupByStatsProcessor.h"
#include "naucrates/statistics/CStatisticsUtils.h"

namespace gpnaucrates
{

	// Parent class for computing statistics for all joins
	class CJoinStatsProcessor
	{
		protected:

			// return join cardinality based on scaling factor and join type
			static
			CDouble DJoinCardinality
				(
				 CStatisticsConfig *pstatsconf,
				 CDouble dRowsLeft,
				 CDouble dRowsRight,
				 DrgPdouble *pdrgpd,
				 IStatistics::EStatsJoinType eStatsJoinType
				);


			// check if the join statistics object is empty output based on the input
			// histograms and the join histograms
			static
			BOOL FEmptyJoinStats
				(
				 BOOL fEmptyOuter,
				 BOOL fEmptyOutput,
				 const CHistogram *phistOuter,
				 const CHistogram *phistInner,
				 CHistogram *phistJoin,
				 IStatistics::EStatsJoinType eStatsJoinType
				 );

			// helper for joining histograms
			static
			void JoinHistograms
				(
				 IMemoryPool *pmp,
				 const CHistogram *phist1,
				 const CHistogram *phist2,
				 CStatsPredJoin *pstatsjoin,
				 CDouble dRows1,
				 CDouble dRows2,
				 CHistogram **pphist1, // output: histogram 1 after join
				 CHistogram **pphist2, // output: histogram 2 after join
				 CDouble *pdScaleFactor, // output: scale factor based on the join
				 BOOL fEmptyInput, // if true, one of the inputs is empty
				 IStatistics::EStatsJoinType eStatsJoinType,
				 BOOL fIgnoreLasjHistComputation
				 );

		public:

			// main driver to generate join stats
			static
			CStatistics *PstatsJoinDriver
				(
				 IMemoryPool *pmp,
				 CStatisticsConfig *pstatsconf,
				 const IStatistics *pistatsOuter,
				 const IStatistics *pistatsInner,
				 DrgPstatspredjoin *pdrgpstatspredjoin,
				 IStatistics::EStatsJoinType eStatsJoinType,
				 BOOL fIgnoreLasjHistComputation
				 );

			static
			IStatistics *PstatsJoinArray
				(
				 IMemoryPool *pmp,
				 DrgPstat *pdrgpstat,
				 CExpression *pexprScalar,
				 IStatistics::EStatsJoinType eStatsJoinType
				 );

			// derive statistics for join operation given array of statistics object
			static
			IStatistics *PstatsJoin
				(
				 IMemoryPool *pmp,
				 CExpressionHandle &exprhdl,
				 DrgPstat *pdrgpstatCtxt
				 );

			// derive statistics when scalar expression has outer references
			static
			IStatistics *PstatsDeriveWithOuterRefs
				(
				 IMemoryPool *pmp,
				 CExpressionHandle &exprhdl, // handle attached to the logical expression we want to derive stats for
				 CExpression *pexprScalar, // scalar condition used for stats derivation
				 IStatistics *pstats, // statistics object of attached expression
				 DrgPstat *pdrgpstatOuter, // array of stats objects where outer references are defined
				 IStatistics::EStatsJoinType eStatsJoinType
				 );
	};
}

#endif // !GPNAUCRATES_CJoinStatsProcessor_H

// EOF

