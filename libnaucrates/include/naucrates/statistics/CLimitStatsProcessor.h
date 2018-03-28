//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLimitStatsProcessor.h
//
//	@doc:
//		Compute statistics for limit operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLimitStatsProcessor_H
#define GPNAUCRATES_CLimitStatsProcessor_H

namespace gpnaucrates
{

	class CLimitStatsProcessor
	{
		public:

		// limit
		static
		CStatistics *PstatsLimit(IMemoryPool *pmp, const CStatistics *pstatsInput, CDouble dLimitRows);
	};
}

#endif // !GPNAUCRATES_CLimitStatsProcessor_H

// EOF

