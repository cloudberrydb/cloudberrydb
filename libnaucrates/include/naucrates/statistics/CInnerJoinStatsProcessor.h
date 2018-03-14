//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CInnerJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Inner Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CInnerJoinStatsProcessor_H
#define GPNAUCRATES_CInnerJoinStatsProcessor_H

#include "naucrates/statistics/CJoinStatsProcessor.h"


namespace gpnaucrates
{
	class CInnerJoinStatsProcessor : public CJoinStatsProcessor
	{
		public:
			// inner join with another stats structure
			static
			CStatistics *PstatsInnerJoinStatic
					(
					IMemoryPool *pmp,
					const IStatistics *pistatsOuter,
					const IStatistics *pistatsInner,
					DrgPstatspredjoin *pdrgpstatspredjoin
					);
	};
}

#endif // !GPNAUCRATES_CInnerJoinStatsProcessor_H

// EOF

