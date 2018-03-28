//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 Pivotal, Inc.
//
//	@filename:
//		CGroupByStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing group by operations
//---------------------------------------------------------------------------

#include "gpopt/operators/ops.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CGroupByStatsProcessor.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

// return statistics object after Group by computation
CStatistics *
CGroupByStatsProcessor::PstatsGroupBy
	(
	IMemoryPool *pmp,
	const CStatistics *pstatsInput,
	DrgPul *pdrgpulGC,
	DrgPul *pdrgpulAgg,
	CBitSet *pbsKeys
	)
{
	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	// hash map colid -> width
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	CStatistics *pstatsAgg = NULL;
	CDouble dRowsAgg = CStatistics::DMinRows;
	if (pstatsInput->FEmpty())
	{
		// add dummy histograms for the aggregates and grouping columns
		CHistogram::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhist, phmuldoubleWidth, pdrgpulAgg, true /* fEmpty */);
		CHistogram::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhist, phmuldoubleWidth, pdrgpulGC, true /* fEmpty */);

		pstatsAgg = GPOS_NEW(pmp) CStatistics(pmp, phmulhist, phmuldoubleWidth, dRowsAgg, true /* fEmpty */);
	}
	else
	{
		// for computed aggregates, we're not going to be very smart right now
		CHistogram::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhist, phmuldoubleWidth, pdrgpulAgg, false /* fEmpty */);

		CColRefSet *pcrsGrpColComputed = GPOS_NEW(pmp) CColRefSet(pmp);
		CColRefSet *pcrsGrpColsForStats = CStatisticsUtils::PcrsGrpColsForStats(pmp, pdrgpulGC, pcrsGrpColComputed);

		// add statistical information of columns (1) used to compute the cardinality of the aggregate
		// and (2) the grouping columns that are computed
		CStatisticsUtils::AddGrpColStats(pmp, pstatsInput, pcrsGrpColsForStats, phmulhist, phmuldoubleWidth);
		CStatisticsUtils::AddGrpColStats(pmp, pstatsInput, pcrsGrpColComputed, phmulhist, phmuldoubleWidth);

		const CStatisticsConfig *pstatsconf = pstatsInput->PStatsConf();

		DrgPdouble *pdrgpdNDV = CStatisticsUtils::PdrgPdoubleNDV(pmp, pstatsconf, pstatsInput, pcrsGrpColsForStats, pbsKeys);
		CDouble dGroups = CStatisticsUtils::DNumOfDistinctVal(pstatsconf, pdrgpdNDV);

		// clean up
		pcrsGrpColsForStats->Release();
		pcrsGrpColComputed->Release();
		pdrgpdNDV->Release();

		dRowsAgg = std::min(std::max(CStatistics::DMinRows.DVal(), dGroups.DVal()), pstatsInput->DRows().DVal());

		// create a new stats object for the output
		pstatsAgg = GPOS_NEW(pmp) CStatistics(pmp, phmulhist, phmuldoubleWidth, dRowsAgg, pstatsInput->FEmpty());
	}

	// In the output statistics object, the upper bound source cardinality of the grouping column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated group by cardinality.

	// modify source id to upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(pmp, pstatsInput, pstatsAgg, dRowsAgg, CStatistics::EcbmMin /* ecbm */);

	return pstatsAgg;
}

// EOF
