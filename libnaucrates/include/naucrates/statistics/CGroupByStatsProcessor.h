//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CGroupByStatsProcessor.h
//
//	@doc:
//		Compute statistics for group by operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CGroupByStatsProcessor_H
#define GPNAUCRATES_CGroupByStatsProcessor_H

namespace gpnaucrates
{

	class CGroupByStatsProcessor
	{
		public:

		// group by
		static
		CStatistics *PstatsGroupBy
						(
						IMemoryPool *pmp,
						const CStatistics *pstatsInput,
						DrgPul *pdrgpulGC,
						DrgPul *pdrgpulAgg,
						CBitSet *pbsKeys
						);
	};
}

#endif // !GPNAUCRATES_CGroupByStatsProcessor_H

// EOF

