//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLeftAntiSemiJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Anti-Semi Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftAntiSemiJoinStatsProcessor_H
#define GPNAUCRATES_CLeftAntiSemiJoinStatsProcessor_H

#include "naucrates/statistics/CJoinStatsProcessor.h"



namespace gpnaucrates
{
	class CLeftAntiSemiJoinStatsProcessor : public CJoinStatsProcessor
	{
		public:
			// helper for LAS-joining histograms
			static
			void JoinHistogramsLASJ
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
			// left anti semi join with another stats structure
			static
			CStatistics *PstatsLASJoinStatic
					(
					IMemoryPool *pmp,
					const IStatistics *pistatsOuter,
					const IStatistics *pistatsInner,
					DrgPstatspredjoin *pdrgpstatspredjoin,
					BOOL fIgnoreLasjHistComputation // except for the case of LOJ cardinality estimation this flag is always
					// "true" since LASJ stats computation is very aggressive
					);
			// compute the null frequency for LASJ
			static
			CDouble DNullFreqLASJ(CStatsPred::EStatsCmpType escmpt, const CHistogram *phistOuter, const CHistogram *phistInner);
	};
}

#endif // !GPNAUCRATES_CLeftAntiSemiJoinStatsProcessor_H

// EOF

