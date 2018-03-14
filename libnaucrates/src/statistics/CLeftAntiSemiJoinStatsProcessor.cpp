//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 Pivotal, Inc.
//
//	@filename:
//		CLeftAntiSemiJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Left Anti-Semi Join
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CScaleFactorUtils.h"

using namespace gpmd;

// helper for LAS-joining histograms
void
CLeftAntiSemiJoinStatsProcessor::JoinHistogramsLASJ
			(
			IMemoryPool *pmp,
			const CHistogram *phist1,
			const CHistogram *phist2,
			CStatsPredJoin *pstatsjoin,
			CDouble dRows1,
			CDouble ,//dRows2,
			CHistogram **pphist1, // output: histogram 1 after join
			CHistogram **pphist2, // output: histogram 2 after join
			CDouble *pdScaleFactor, // output: scale factor based on the join
			BOOL fEmptyInput,
			IStatistics::EStatsJoinType,
			BOOL fIgnoreLasjHistComputation
			)
{
	GPOS_ASSERT(NULL != phist1);
	GPOS_ASSERT(NULL != phist2);
	GPOS_ASSERT(NULL != pstatsjoin);
	GPOS_ASSERT(NULL != pphist1);
	GPOS_ASSERT(NULL != pphist2);
	GPOS_ASSERT(NULL != pdScaleFactor);

	// anti-semi join should give the full outer side.
	// use 1.0 as scale factor if anti semi join
	*pdScaleFactor = 1.0;

	CStatsPred::EStatsCmpType escmpt = pstatsjoin->Escmpt();

	if (fEmptyInput)
	{
		*pphist1 = phist1->PhistCopy(pmp);
		*pphist2 = NULL;

		return;
	}

	BOOL fEmptyHistograms = phist1->FEmpty() || phist2->FEmpty();
	if (!fEmptyHistograms && CHistogram::FSupportsJoinPred(escmpt))
	{
		*pphist1 = phist1->PhistLASJoinNormalized
				(
				pmp,
				escmpt,
				dRows1,
				phist2,
				pdScaleFactor,
				fIgnoreLasjHistComputation
				);
		*pphist2 = NULL;

		if ((*pphist1)->FEmpty())
		{
			// if the LASJ histograms is empty then all tuples of the outer join column
			// joined with those on the inner side. That is, LASJ will produce no rows
			*pdScaleFactor = dRows1;
		}

		return;
	}

	// for an unsupported join predicate operator or in the case of missing stats,
	// copy input histograms and use default scale factor
	*pdScaleFactor = CDouble(CScaleFactorUtils::DDefaultScaleFactorJoin);
	*pphist1 = phist1->PhistCopy(pmp);
	*pphist2 = NULL;
}

//	Return statistics object after performing LASJ
CStatistics *
CLeftAntiSemiJoinStatsProcessor::PstatsLASJoinStatic
		(
		IMemoryPool *pmp,
		const IStatistics *pistatsOuter,
		const IStatistics *pistatsInner,
		DrgPstatspredjoin *pdrgpstatspredjoin,
		BOOL fIgnoreLasjHistComputation
		)
{
	GPOS_ASSERT(NULL != pistatsInner);
	GPOS_ASSERT(NULL != pistatsOuter);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);
	const CStatistics *pstatsOuter = dynamic_cast<const CStatistics *> (pistatsOuter);

	return CJoinStatsProcessor::PstatsJoinDriver
			(
			pmp,
			pstatsOuter->PStatsConf(),
			pistatsOuter,
			pistatsInner,
			pdrgpstatspredjoin,
			IStatistics::EsjtLeftAntiSemiJoin /* esjt */,
			fIgnoreLasjHistComputation
			);
}

// Compute the null frequency for LASJ
CDouble
CLeftAntiSemiJoinStatsProcessor::DNullFreqLASJ
		(
		CStatsPred::EStatsCmpType escmpt,
		const CHistogram *phistOuter,
		const CHistogram *phistInner
		)
{
	GPOS_ASSERT(NULL != phistOuter);
	GPOS_ASSERT(NULL != phistInner);

	if (CStatsPred::EstatscmptINDF != escmpt)
	{
		// for equality predicate NULLs on the outer side of the join
		// will not join with those in the inner side
		return phistOuter->DNullFreq();
	}

	if (CStatistics::DEpsilon < phistInner->DNullFreq())
	{
		// for INDF predicate NULLs on the outer side of the join
		// will join with those in the inner side if they are present
		return CDouble(0.0);
	}

	return phistOuter->DNullFreq();
}


// EOF
