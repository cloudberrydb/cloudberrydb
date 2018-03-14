//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLeftOuterJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Outer Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftOuterJoinStatsProcessor_H
#define GPNAUCRATES_CLeftOuterJoinStatsProcessor_H

#include "naucrates/statistics/CJoinStatsProcessor.h"

namespace gpnaucrates
{
	class CLeftOuterJoinStatsProcessor : public CJoinStatsProcessor
	{
	private:
		// create a new hash map of histograms from the results of the inner join and the histograms of the outer child
		static
		HMUlHist *PhmulhistLOJ
				(
				IMemoryPool *pmp,
				const CStatistics *pstatsOuter,
				const CStatistics *pstatsInner,
				CStatistics *pstatsInnerJoin,
				DrgPstatspredjoin *pdrgpstatspredjoin,
				CDouble dRowsInnerJoin,
				CDouble *pdRowsLASJ
				);
		// helper method to add histograms of the inner side of a LOJ
		static
		void AddHistogramsLOJInner
				(
				IMemoryPool *pmp,
				const CStatistics *pstatsInnerJoin,
				DrgPul *pdrgpulInnerColId,
				CDouble dRowsLASJ,
				CDouble dRowsInnerJoin,
				HMUlHist *phmulhistLOJ
				);

	public:
		static
		CStatistics *PstatsLOJStatic
				(
				IMemoryPool *pmp,
				const IStatistics *pstatsOuter,
				const IStatistics *pstatsInner,
				DrgPstatspredjoin *pdrgpstatspredjoin
				);
	};
}

#endif // !GPNAUCRATES_CLeftOuterJoinStatsProcessor_H

// EOF

