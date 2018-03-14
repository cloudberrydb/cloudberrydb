//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLeftSemiJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Semi Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftSemiJoinStatsProcessor_H
#define GPNAUCRATES_CLeftSemiJoinStatsProcessor_H

#include "naucrates/statistics/CJoinStatsProcessor.h"

namespace gpnaucrates
{
	class CLeftSemiJoinStatsProcessor : public CJoinStatsProcessor
	{
		public:
			static
			CStatistics *PstatsLSJoinStatic
					(
					IMemoryPool *pmp,
					const IStatistics *pstatsOuter,
					const IStatistics *pstatsInner,
					DrgPstatspredjoin *pdrgpstatspredjoin
					);
	};
}

#endif // !GPNAUCRATES_CLeftSemiJoinStatsProcessor_H

// EOF

