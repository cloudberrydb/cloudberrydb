//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 Pivotal, Inc.
//
//	@filename:
//		CInnerJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Inner Joins
//---------------------------------------------------------------------------

#include "gpopt/operators/ops.h"
#include "naucrates/statistics/CInnerJoinStatsProcessor.h"

using namespace gpmd;

// return statistics object after performing inner join
CStatistics *
CInnerJoinStatsProcessor::PstatsInnerJoinStatic
			(
			IMemoryPool *pmp,
			const IStatistics *pistatsOuter,
			const IStatistics *pistatsInner,
			DrgPstatspredjoin *pdrgpstatspredjoin
			)
{
	GPOS_ASSERT(NULL != pistatsOuter);
	GPOS_ASSERT(NULL != pistatsInner);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);
	const CStatistics *pstatsOuter = dynamic_cast<const CStatistics *> (pistatsOuter);

	return CJoinStatsProcessor::PstatsJoinDriver
			(
			pmp,
			pstatsOuter->PStatsConf(),
			pistatsOuter,
			pistatsInner,
			pdrgpstatspredjoin,
			IStatistics::EsjtInnerJoin,
			true /* fIgnoreLasjHistComputation */
			);
}

// EOF
