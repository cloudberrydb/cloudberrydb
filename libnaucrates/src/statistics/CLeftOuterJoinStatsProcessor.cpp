//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 Pivotal, Inc.
//
//	@filename:
//		CLeftOuterJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Left Outer Joins
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CLeftOuterJoinStatsProcessor.h"

using namespace gpmd;

// return statistics object after performing LOJ operation with another statistics structure
CStatistics *
CLeftOuterJoinStatsProcessor::PstatsLOJStatic
		(
		IMemoryPool *pmp,
		const IStatistics *pstatsOuter,
		const IStatistics *pstatsInner,
		DrgPstatspredjoin *pdrgpstatspredjoin
		)
{
	GPOS_ASSERT(NULL != pstatsOuter);
	GPOS_ASSERT(NULL != pstatsInner);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);

	const CStatistics *pstatsOuterSide = dynamic_cast<const CStatistics *> (pstatsOuter);
	const CStatistics *pstatsInnerSide = dynamic_cast<const CStatistics *> (pstatsInner);

	CStatistics *pstatsInnerJoin = pstatsOuterSide->PstatsInnerJoin(pmp, pstatsInner, pdrgpstatspredjoin);
	CDouble dRowsInnerJoin = pstatsInnerJoin->DRows();
	CDouble dRowsLASJ(1.0);

	// create a new hash map of histograms, for each column from the outer child
	// add the buckets that do not contribute to the inner join
	HMUlHist *phmulhistLOJ = CLeftOuterJoinStatsProcessor::PhmulhistLOJ
			(
			pmp,
			pstatsOuterSide,
			pstatsInnerSide,
			pstatsInnerJoin,
			pdrgpstatspredjoin,
			dRowsInnerJoin,
			&dRowsLASJ
			);

	// cardinality of LOJ is at least the cardinality of the outer child
	CDouble dRowsLOJ = std::max(pstatsOuter->DRows(), dRowsInnerJoin + dRowsLASJ);

	// create an output stats object
	CStatistics *pstatsLOJ = GPOS_NEW(pmp) CStatistics
			(
			pmp,
			phmulhistLOJ,
			pstatsInnerJoin->CopyWidths(pmp),
			dRowsLOJ,
			pstatsOuter->FEmpty(),
			pstatsOuter->UlNumberOfPredicates()
			);

	pstatsInnerJoin->Release();

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	// modify source id to upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(pmp, pstatsOuterSide, pstatsLOJ, dRowsLOJ, CStatistics::EcbmMin /* ecbm */);
	CStatisticsUtils::ComputeCardUpperBounds(pmp, pstatsInnerSide, pstatsLOJ, dRowsLOJ, CStatistics::EcbmMin /* ecbm */);

	return pstatsLOJ;
}

// create a new hash map of histograms for LOJ from the histograms
// of the outer child and the histograms of the inner join
HMUlHist *
CLeftOuterJoinStatsProcessor::PhmulhistLOJ
		(
		IMemoryPool *pmp,
		const CStatistics *pstatsOuter,
		const CStatistics *pstatsInner,
		CStatistics *pstatsInnerJoin,
		DrgPstatspredjoin *pdrgpstatspredjoin,
		CDouble dRowsInnerJoin,
		CDouble *pdRowsLASJ
		)
{
	GPOS_ASSERT(NULL != pstatsOuter);
	GPOS_ASSERT(NULL != pstatsInner);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);
	GPOS_ASSERT(NULL != pstatsInnerJoin);

	// build a bitset with all outer child columns contributing to the join
	CBitSet *pbsOuterJoinCol = GPOS_NEW(pmp) CBitSet(pmp);
	for (ULONG ul1 = 0; ul1 < pdrgpstatspredjoin->UlLength(); ul1++)
	{
		CStatsPredJoin *pstatsjoin = (*pdrgpstatspredjoin)[ul1];
		(void) pbsOuterJoinCol->FExchangeSet(pstatsjoin->UlColId1());
	}

	// for the columns in the outer child, compute the buckets that do not contribute to the inner join
	CStatistics *pstatsLASJ = pstatsOuter->PstatsLASJoin
			(
			pmp,
			pstatsInner,
			pdrgpstatspredjoin,
			false /* fIgnoreLasjHistComputation */
			);
	CDouble dRowsLASJ(0.0);
	if (!pstatsLASJ->FEmpty())
	{
		dRowsLASJ = pstatsLASJ->DRows();
	}

	HMUlHist *phmulhistLOJ = GPOS_NEW(pmp) HMUlHist(pmp);

	DrgPul *pdrgpulOuterColId = pstatsOuter->PdrgulColIds(pmp);
	const ULONG ulOuterCols = pdrgpulOuterColId->UlLength();

	for (ULONG ul2 = 0; ul2 < ulOuterCols; ul2++)
	{
		ULONG ulColId = *(*pdrgpulOuterColId)[ul2];
		const CHistogram *phistInnerJoin = pstatsInnerJoin->Phist(ulColId);
		GPOS_ASSERT(NULL != phistInnerJoin);

		if (pbsOuterJoinCol->FBit(ulColId))
		{
			// add buckets from the outer histogram that do not contribute to the inner join
			const CHistogram *phistLASJ = pstatsLASJ->Phist(ulColId);
			GPOS_ASSERT(NULL != phistLASJ);

			if (phistLASJ->FWellDefined() && !phistLASJ->FEmpty())
			{
				// union the buckets from the inner join and LASJ to get the LOJ buckets
				CHistogram *phistLOJ = phistLASJ->PhistUnionAllNormalized(pmp, dRowsLASJ, phistInnerJoin, dRowsInnerJoin);
				CStatisticsUtils::AddHistogram(pmp, ulColId, phistLOJ, phmulhistLOJ);
				GPOS_DELETE(phistLOJ);
			}
			else
			{
				CStatisticsUtils::AddHistogram(pmp, ulColId, phistInnerJoin, phmulhistLOJ);
			}
		}
		else
		{
			// if column from the outer side that is not a join then just add it
			CStatisticsUtils::AddHistogram(pmp, ulColId, phistInnerJoin, phmulhistLOJ);
		}
	}

	pstatsLASJ->Release();

	// extract all columns from the inner child of the join
	DrgPul *pdrgpulInnerColId = pstatsInner->PdrgulColIds(pmp);

	// add its corresponding statistics
	AddHistogramsLOJInner(pmp, pstatsInnerJoin, pdrgpulInnerColId, dRowsLASJ, dRowsInnerJoin, phmulhistLOJ);

	*pdRowsLASJ = dRowsLASJ;

	// clean up
	pdrgpulInnerColId->Release();
	pdrgpulOuterColId->Release();
	pbsOuterJoinCol->Release();

	return phmulhistLOJ;
}


// helper function to add histograms of the inner side of a LOJ
void
CLeftOuterJoinStatsProcessor::AddHistogramsLOJInner
		(
		IMemoryPool *pmp,
		const CStatistics *pstatsInnerJoin,
		DrgPul *pdrgpulInnerColId,
		CDouble dRowsLASJ,
		CDouble dRowsInnerJoin,
		HMUlHist *phmulhistLOJ
		)
{
	GPOS_ASSERT(NULL != pstatsInnerJoin);
	GPOS_ASSERT(NULL != pdrgpulInnerColId);
	GPOS_ASSERT(NULL != phmulhistLOJ);

	const ULONG ulInnerCols = pdrgpulInnerColId->UlLength();

	for (ULONG ul = 0; ul < ulInnerCols; ul++)
	{
		ULONG ulColId = *(*pdrgpulInnerColId)[ul];

		const CHistogram *phistInnerJoin = pstatsInnerJoin->Phist(ulColId);
		GPOS_ASSERT(NULL != phistInnerJoin);

		// the number of nulls added to the inner side should be the number of rows of the LASJ on the outer side.
		CHistogram *phistNull = GPOS_NEW(pmp) CHistogram
				(
				GPOS_NEW(pmp) DrgPbucket(pmp),
				true /*fWellDefined*/,
				1.0 /*dNullFreq*/,
				CHistogram::DDefaultNDVRemain,
				CHistogram::DDefaultNDVFreqRemain
				);
		CHistogram *phistLOJ = phistInnerJoin->PhistUnionAllNormalized(pmp, dRowsInnerJoin, phistNull, dRowsLASJ);
		CStatisticsUtils::AddHistogram(pmp, ulColId, phistLOJ, phmulhistLOJ);

		GPOS_DELETE(phistNull);
		GPOS_DELETE(phistLOJ);
	}
}

// EOF
