//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CUnionAllStatsProcessor.h
//
//	@doc:
//		Compute statistics for union all operation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CUnionAllStatsProcessor_H
#define GPNAUCRATES_CUnionAllStatsProcessor_H

#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/statistics/CStatistics.h"

namespace gpnaucrates
{

	class CUnionAllStatsProcessor
	{
		public:

		static
		CStatistics *PstatsUnionAll
						(
						IMemoryPool *pmp,
						const CStatistics *pstatsFst,
						const CStatistics *pstatsSnd,
						DrgPul *pdrgpulOutput,
						DrgPul *pdrgpulInput1,
						DrgPul *pdrgpulInput2
						);
	};
}

#endif // !GPNAUCRATES_CUnionAllStatsProcessor_H

// EOF

