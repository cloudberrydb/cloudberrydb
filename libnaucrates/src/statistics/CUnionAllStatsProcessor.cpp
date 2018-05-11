//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 Pivotal, Inc.
//
//	@filename:
//		CUnionAllStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing union all operations
//---------------------------------------------------------------------------

#include "naucrates/statistics/CUnionAllStatsProcessor.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

// return statistics object after union all operation with input statistics object
CStatistics *
CUnionAllStatsProcessor::PstatsUnionAll
	(
	IMemoryPool *pmp,
	const CStatistics *pstatsFst,
	const CStatistics *pstatsSnd,
	DrgPul *pdrgpulOutput,
	DrgPul *pdrgpulInput1,
	DrgPul *pdrgpulInput2
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pstatsSnd);

	// lengths must match
	GPOS_ASSERT(pdrgpulOutput->UlLength() == pdrgpulInput1->UlLength());
	GPOS_ASSERT(pdrgpulOutput->UlLength() == pdrgpulInput2->UlLength());

	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhistNew = GPOS_NEW(pmp) HMUlHist(pmp);

	// column ids on which widths are to be computed
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	BOOL fEmptyUnionAll = pstatsFst->FEmpty() && pstatsSnd->FEmpty();
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CDouble dRowsUnionAll = CStatistics::DMinRows;
	if (fEmptyUnionAll)
	{
		CHistogram::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhistNew, phmuldoubleWidth, pdrgpulOutput, true /*fEmpty*/);
	}
	else
	{
		const ULONG ulLen = pdrgpulOutput->UlLength();
		for (ULONG ul = 0; ul < ulLen; ul++)
		{
			ULONG ulColIdOutput = *(*pdrgpulOutput)[ul];
			ULONG ulColIdInput1 = *(*pdrgpulInput1)[ul];
			ULONG ulColIdInput2 = *(*pdrgpulInput2)[ul];

			const CHistogram *phistInput1 = pstatsFst->Phist(ulColIdInput1);
			GPOS_ASSERT(NULL != phistInput1);
			const CHistogram *phistInput2 = pstatsSnd->Phist(ulColIdInput2);
			GPOS_ASSERT(NULL != phistInput2);

			if (phistInput1->FWellDefined() || phistInput2->FWellDefined())
			{
				CHistogram *phistOutput = phistInput1->PhistUnionAllNormalized(pmp, pstatsFst->DRows(), phistInput2, pstatsSnd->DRows());
				CStatisticsUtils::AddHistogram(pmp, ulColIdOutput, phistOutput, phmulhistNew);
				GPOS_DELETE(phistOutput);
			}
			else
			{
				CColRef *pcr = pcf->PcrLookup(ulColIdOutput);
				GPOS_ASSERT(NULL != pcr);

				CHistogram *phistDummy = CHistogram::PhistDefault(pmp, pcr, false /* fEmpty*/);
				phmulhistNew->FInsert(GPOS_NEW(pmp) ULONG(ulColIdOutput), phistDummy);
			}

			// look up width
			const CDouble *pdWidth = pstatsFst->PdWidth(ulColIdInput1);
			GPOS_ASSERT(NULL != pdWidth);
			phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColIdOutput), GPOS_NEW(pmp) CDouble(*pdWidth));
		}

		dRowsUnionAll = pstatsFst->DRows() + pstatsSnd->DRows();
	}

	// release inputs
	pdrgpulOutput->Release();
	pdrgpulInput1->Release();
	pdrgpulInput2->Release();

	// create an output stats object
	CStatistics *pstatsUnionAll = GPOS_NEW(pmp) CStatistics
											(
											pmp,
											phmulhistNew,
											phmuldoubleWidth,
											dRowsUnionAll,
											fEmptyUnionAll,
											0 /* m_ulNumPredicates */
											);

	// In the output statistics object, the upper bound source cardinality of the UNION ALL column
	// is the estimate union all cardinality.

	// modify upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(pmp, pstatsFst, pstatsUnionAll, dRowsUnionAll, CStatistics::EcbmOutputCard /* ecbm */);

	return pstatsUnionAll;
}

// EOF
