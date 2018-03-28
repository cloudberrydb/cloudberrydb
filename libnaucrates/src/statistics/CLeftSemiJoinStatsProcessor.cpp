//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 Pivotal, Inc.
//
//	@filename:
//		CLeftSemiJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Left Semi Joins
//---------------------------------------------------------------------------

#include "gpopt/operators/ops.h"
#include "naucrates/statistics/CLeftSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CGroupByStatsProcessor.h"

using namespace gpopt;

// return statistics object after performing LSJ operation
CStatistics *
CLeftSemiJoinStatsProcessor::PstatsLSJoinStatic
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

	const ULONG ulLen = pdrgpstatspredjoin->UlLength();

	// iterate over all inner columns and perform a group by to remove duplicates
	DrgPul *pdrgpulInnerColumnIds = GPOS_NEW(pmp) DrgPul(pmp);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULONG ulInnerColId = ((*pdrgpstatspredjoin)[ul])->UlColId2();
		pdrgpulInnerColumnIds->Append(GPOS_NEW(pmp) ULONG(ulInnerColId));
	}

	// dummy agg columns required for group by derivation
	DrgPul *pdrgpulAgg = GPOS_NEW(pmp) DrgPul(pmp);
	IStatistics *pstatsInnerNoDups = CGroupByStatsProcessor::PstatsGroupBy
			(
			pmp,
			dynamic_cast<const CStatistics *>(pistatsInner),
			pdrgpulInnerColumnIds,
			pdrgpulAgg,
			NULL // pbsKeys: no keys, use all grouping cols
			);

	const CStatistics *pstatsOuter = dynamic_cast<const CStatistics *> (pistatsOuter);
	CStatistics *pstatsSemiJoin = CJoinStatsProcessor::PstatsJoinDriver
			(
			pmp,
			pstatsOuter->PStatsConf(),
			pstatsOuter,
			pstatsInnerNoDups,
			pdrgpstatspredjoin,
			IStatistics::EsjtLeftSemiJoin /* esjt */,
			true /* fIgnoreLasjHistComputation */
			);

	// clean up
	pdrgpulInnerColumnIds->Release();
	pdrgpulAgg->Release();
	pstatsInnerNoDups->Release();

	return pstatsSemiJoin;

}

// EOF
