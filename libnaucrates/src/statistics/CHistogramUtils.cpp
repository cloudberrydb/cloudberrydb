//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CHistogramUtils.cpp
//
//	@doc:
//		Utility functions that operate on histograms
//---------------------------------------------------------------------------

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/statistics/CHistogramUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CScaleFactorUtils.h"

#include "gpos/common/CBitSet.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColumnFactory.h"

#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;

// append given histograms to current object
void
CHistogramUtils::AddHistograms
	(
	IMemoryPool *pmp,
	HMUlHist *phmulhistSrc,
	HMUlHist *phmulhistDest
	)
{
	HMIterUlHist hmiterulhist(phmulhistSrc);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());
		const CHistogram *phist = hmiterulhist.Pt();
		CStatisticsUtils::AddHistogram(pmp, ulColId, phist, phmulhistDest);
	}
}

// add dummy histogram buckets and column information for the array of columns
void
CHistogramUtils::AddDummyHistogramAndWidthInfo
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	HMUlHist *phmulhistOutput,
	HMUlDouble *phmuldoubleWidthOutput,
	const DrgPul *pdrgpul,
	BOOL fEmpty
	)
{
	GPOS_ASSERT(NULL != pcf);
	GPOS_ASSERT(NULL != phmulhistOutput);
	GPOS_ASSERT(NULL != phmuldoubleWidthOutput);
	GPOS_ASSERT(NULL != pdrgpul);

	const ULONG ulCount = pdrgpul->UlLength();
	// for computed aggregates, we're not going to be very smart right now
	for (ULONG ul = 0; ul < ulCount; ul++)
	{
		ULONG ulColId = *(*pdrgpul)[ul];

		CColRef *pcr = pcf->PcrLookup(ulColId);
		GPOS_ASSERT(NULL != pcr);

		CHistogram *phist = CHistogram::PhistDefault(pmp, pcr, fEmpty);
		phmulhistOutput->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phist);

		CDouble dWidth = CStatisticsUtils::DDefaultColumnWidth(pcr->Pmdtype());
		phmuldoubleWidthOutput->FInsert(GPOS_NEW(pmp) ULONG(ulColId), GPOS_NEW(pmp) CDouble(dWidth));
	}
}

//	add empty histogram for the columns in the input histogram
void
CHistogramUtils::AddEmptyHistogram
	(
	IMemoryPool *pmp,
	HMUlHist *phmulhistOutput,
	HMUlHist *phmulhistInput
	)
{
	GPOS_ASSERT(NULL != phmulhistOutput);
	GPOS_ASSERT(NULL != phmulhistInput);

	HMIterUlHist hmiterulhist(phmulhistInput);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());

		// empty histogram
		CHistogram *phist =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp), false /* fWellDefined */);
		phmulhistOutput->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phist);
	}
}

//	create a new histograms after applying the filter that is not
//	an AND/OR predicate
CHistogram *
CHistogramUtils::PhistSimpleFilter
	(
	IMemoryPool *pmp,
	CStatsPred *pstatspred,
	CBitSet *pbsFilterColIds,
	CHistogram *phistBefore,
	CDouble *pdScaleFactorLast,
	ULONG *pulColIdLast
	)
{
	if (CStatsPred::EsptPoint == pstatspred->Espt())
	{
		CStatsPredPoint *pstatspredPoint = CStatsPredPoint::PstatspredConvert(pstatspred);
		return PhistPointFilter(pmp, pstatspredPoint, pbsFilterColIds, phistBefore, pdScaleFactorLast, pulColIdLast);
	}

	if (CStatsPred::EsptLike == pstatspred->Espt())
	{
		CStatsPredLike *pstatspredLike = CStatsPredLike::PstatspredConvert(pstatspred);

		return PhistLikeFilter(pmp, pstatspredLike, pbsFilterColIds, phistBefore, pdScaleFactorLast, pulColIdLast);
	}

	CStatsPredUnsupported *pstatspredUnsupported = CStatsPredUnsupported::PstatspredConvert(pstatspred);

	return PhistUnsupportedPred(pmp, pstatspredUnsupported, pbsFilterColIds, phistBefore, pdScaleFactorLast, pulColIdLast);
}

// create a new histograms after applying the point filter
CHistogram *
CHistogramUtils::PhistPointFilter
	(
	IMemoryPool *pmp,
	CStatsPredPoint *pstatspred,
	CBitSet *pbsFilterColIds,
	CHistogram *phistBefore,
	CDouble *pdScaleFactorLast,
	ULONG *pulColIdLast
	)
{
	GPOS_ASSERT(NULL != pstatspred);
	GPOS_ASSERT(NULL != pbsFilterColIds);
	GPOS_ASSERT(NULL != phistBefore);

	const ULONG ulColId = pstatspred->UlColId();
	GPOS_ASSERT(CHistogram::FSupportsFilter(pstatspred->Escmpt()));

	CPoint *ppoint = pstatspred->Ppoint();

	// note column id
	(void) pbsFilterColIds->FExchangeSet(ulColId);

	CDouble dScaleFactorLocal(1.0);
	CHistogram *phistAfter = phistBefore->PhistFilterNormalized(pmp, pstatspred->Escmpt(), ppoint, &dScaleFactorLocal);

	GPOS_ASSERT(DOUBLE(1.0) <= dScaleFactorLocal.DVal());

	*pdScaleFactorLast = *pdScaleFactorLast * dScaleFactorLocal;
	*pulColIdLast = ulColId;

	return phistAfter;
}


//	create a new histograms for an unsupported predicate
CHistogram *
CHistogramUtils::PhistUnsupportedPred
	(
	IMemoryPool *pmp,
	CStatsPredUnsupported *pstatspred,
	CBitSet *pbsFilterColIds,
	CHistogram *phistBefore,
	CDouble *pdScaleFactorLast,
	ULONG *pulColIdLast
	)
{
	GPOS_ASSERT(NULL != pstatspred);
	GPOS_ASSERT(NULL != pbsFilterColIds);
	GPOS_ASSERT(NULL != phistBefore);

	const ULONG ulColId = pstatspred->UlColId();

	// note column id
	(void) pbsFilterColIds->FExchangeSet(ulColId);

	// generate after histogram
	CHistogram *phistAfter = phistBefore->PhistCopy(pmp);
	GPOS_ASSERT(NULL != phistAfter);

	*pdScaleFactorLast = *pdScaleFactorLast * pstatspred->DScaleFactor();
	*pulColIdLast = ulColId;

	return phistAfter;
}

//	create a new histograms after applying the LIKE filter
CHistogram *
CHistogramUtils::PhistLikeFilter
	(
	IMemoryPool *pmp,
	CStatsPredLike *pstatspred,
	CBitSet *pbsFilterColIds,
	CHistogram *phistBefore,
	CDouble *pdScaleFactorLast,
	ULONG *pulColIdLast
	)
{
	GPOS_ASSERT(NULL != pstatspred);
	GPOS_ASSERT(NULL != pbsFilterColIds);
	GPOS_ASSERT(NULL != phistBefore);

	const ULONG ulColId = pstatspred->UlColId();

	// note column id
	(void) pbsFilterColIds->FExchangeSet(ulColId);
	CHistogram *phistAfter = phistBefore->PhistCopy(pmp);

	*pdScaleFactorLast = *pdScaleFactorLast * pstatspred->DDefaultScaleFactor();
	*pulColIdLast = ulColId;

	return phistAfter;
}

// create a new hash map of histograms after applying a conjunctive
// or a disjunctive filter
HMUlHist *
CHistogramUtils::PhmulhistApplyConjOrDisjFilter
	(
	IMemoryPool *pmp,
	CStatisticsConfig *pstatsconf,
	HMUlHist *phmulhistInput,
	CDouble dRowsInput,
	CStatsPred *pstatspred,
	CDouble *pdScaleFactor
	)
{
	GPOS_ASSERT(NULL != pstatspred);
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != phmulhistInput);

	HMUlHist *phmulhistAfter = NULL;

	if (CStatsPred::EsptConj == pstatspred->Espt())
	{
		CStatsPredConj *pstatspredConj = CStatsPredConj::PstatspredConvert(pstatspred);
		return PhmulhistApplyConjFilter
				(
				pmp,
				pstatsconf,
				phmulhistInput,
				dRowsInput,
				pstatspredConj,
				pdScaleFactor
				);
	}

	CStatsPredDisj *pstatspredDisj = CStatsPredDisj::PstatspredConvert(pstatspred);
	phmulhistAfter  = PhmulhistApplyDisjFilter
						(
						pmp,
						pstatsconf,
						phmulhistInput,
						dRowsInput,
						pstatspredDisj,
						pdScaleFactor
						);

	GPOS_ASSERT(NULL != phmulhistAfter);

	return phmulhistAfter;
}

// create new hash map of histograms after applying conjunctive predicates
HMUlHist *
CHistogramUtils::PhmulhistApplyConjFilter
	(
	IMemoryPool *pmp,
	CStatisticsConfig *pstatsconf,
	HMUlHist *phmulhistInput,
	CDouble dRowsInput,
	CStatsPredConj *pstatspredConj,
	CDouble *pdScaleFactor
	)
{
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != phmulhistInput);
	GPOS_ASSERT(NULL != pstatspredConj);

	pstatspredConj->Sort();

	CBitSet *pbsFilterColIds = GPOS_NEW(pmp) CBitSet(pmp);
	DrgPdouble *pdrgpdScaleFactor = GPOS_NEW(pmp) DrgPdouble(pmp);

	// create copy of the original hash map of colid -> histogram
	HMUlHist *phmulhistResult = CStatisticsUtils::PhmulhistCopy(pmp, phmulhistInput);

	// properties of last seen column
	CDouble dScaleFactorLast(1.0);
	ULONG ulColIdLast = ULONG_MAX;

	// iterate over filters and update corresponding histograms
	const ULONG ulFilters = pstatspredConj->UlFilters();
	for (ULONG ul = 0; ul < ulFilters; ul++)
	{
		CStatsPred *pstatspredChild = pstatspredConj->Pstatspred(ul);

		GPOS_ASSERT(CStatsPred::EsptConj != pstatspredChild->Espt());

		// get the components of the statistics filter
		ULONG ulColId = pstatspredChild->UlColId();

		if (CStatsPredUtils::FUnsupportedPredOnDefinedCol(pstatspredChild))
		{
			// for example, (expression OP const) where expression is a defined column like (a+b)
			CStatsPredUnsupported *pstatspredUnsupported = CStatsPredUnsupported::PstatspredConvert(pstatspredChild);
			pdrgpdScaleFactor->Append(GPOS_NEW(pmp) CDouble(pstatspredUnsupported->DScaleFactor()));

			continue;
		}

		// the histogram to apply filter on
		CHistogram *phistBefore = NULL;
		if (FNewStatsColumn(ulColId, ulColIdLast))
		{
			pdrgpdScaleFactor->Append( GPOS_NEW(pmp) CDouble(dScaleFactorLast));
			dScaleFactorLast = CDouble(1.0);
		}

		if (CStatsPred::EsptDisj != pstatspredChild->Espt())
		{
			GPOS_ASSERT(ULONG_MAX != ulColId);
			phistBefore = phmulhistResult->PtLookup(&ulColId)->PhistCopy(pmp);
			GPOS_ASSERT(NULL != phistBefore);

			CHistogram *phistResult = NULL;
			phistResult = CHistogramUtils::PhistSimpleFilter(pmp, pstatspredChild, pbsFilterColIds, phistBefore, &dScaleFactorLast, &ulColIdLast);
			GPOS_DELETE(phistBefore);

			GPOS_ASSERT(NULL != phistResult);

			CHistogram *phistInput = phmulhistInput->PtLookup(&ulColId);
			GPOS_ASSERT(NULL != phistInput);
			if (phistInput->FEmpty())
			{
				// input histogram is empty so scaling factor does not make sense.
				// if the input itself is empty, then scaling factor is of no effect
				dScaleFactorLast = 1 / CHistogram::DDefaultSelectivity;
			}

			CStatisticsUtils::AddHistogram(pmp, ulColId, phistResult, phmulhistResult, true /* fReplaceOld */);
			GPOS_DELETE(phistResult);
		}
		else
		{
			CStatsPredDisj *pstatspredDisj = CStatsPredDisj::PstatspredConvert(pstatspredChild);

			phmulhistResult->AddRef();
			HMUlHist *phmulhistDisjInput = phmulhistResult;

			CDouble dScaleFactorDisj(1.0);
			CDouble dRowsDisjInput(CStatistics::DMinRows.DVal());

			if (ULONG_MAX != ulColId)
			{
				// The disjunction predicate uses a single column. The input rows to the disjunction
				// is obtained by scaling attained so far on that column
				dRowsDisjInput = std::max(CStatistics::DMinRows.DVal(), (dRowsInput / dScaleFactorLast).DVal());
			}
			else
			{
				// the disjunction uses multiple columns therefore cannot reason about the number of input rows
				// to the disjunction
				dRowsDisjInput = dRowsInput.DVal();
			}

			HMUlHist *phmulhistAfterDisj = PhmulhistApplyDisjFilter
											(
											pmp,
											pstatsconf,
											phmulhistResult,
											dRowsDisjInput,
											pstatspredDisj,
											&dScaleFactorDisj
											);

			// replace intermediate result with the newly generated result from the disjunction
			if (ULONG_MAX != ulColId)
			{
				CHistogram *phistResult = phmulhistAfterDisj->PtLookup(&ulColId);
				CStatisticsUtils::AddHistogram(pmp, ulColId, phistResult, phmulhistResult, true /* fReplaceOld */);
				phmulhistAfterDisj->Release();

				dScaleFactorLast = dScaleFactorLast * dScaleFactorDisj;
			}
			else
			{
				dScaleFactorLast = dScaleFactorDisj.DVal();
				phmulhistResult->Release();
				phmulhistResult = phmulhistAfterDisj;
			}

			ulColIdLast = ulColId;
			phmulhistDisjInput->Release();
		}
	}

	// scaling factor of the last predicate
	pdrgpdScaleFactor->Append(GPOS_NEW(pmp) CDouble(dScaleFactorLast));

	GPOS_ASSERT(NULL != pdrgpdScaleFactor);
	CScaleFactorUtils::SortScalingFactor(pdrgpdScaleFactor, true /* fDescending */);

	*pdScaleFactor = CScaleFactorUtils::DScaleFactorCumulativeConj(pstatsconf, pdrgpdScaleFactor);

	// clean up
	pdrgpdScaleFactor->Release();
	pbsFilterColIds->Release();

	return phmulhistResult;
}

// create new hash map of histograms after applying disjunctive predicates
HMUlHist *
CHistogramUtils::PhmulhistApplyDisjFilter
	(
	IMemoryPool *pmp,
	CStatisticsConfig *pstatsconf,
	HMUlHist *phmulhistInput,
	CDouble dRowsInput,
	CStatsPredDisj *pstatspredDisj,
	CDouble *pdScaleFactor
	)
{
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != phmulhistInput);
	GPOS_ASSERT(NULL != pstatspredDisj);

	CBitSet *pbsStatsNonUpdateableCols = CStatisticsUtils::PbsNonUpdatableHistForDisj(pmp, pstatspredDisj);

	pstatspredDisj->Sort();

	CBitSet *pbsFilterColIds = GPOS_NEW(pmp) CBitSet(pmp);
	DrgPdouble *pdrgpdScaleFactor = GPOS_NEW(pmp) DrgPdouble(pmp);

	HMUlHist *phmulhistResultDisj = GPOS_NEW(pmp) HMUlHist(pmp);

	CHistogram *phistPrev = NULL;
	ULONG ulColIdPrev = ULONG_MAX;
	CDouble dScaleFactorPrev(dRowsInput);

	CDouble dRowsCumulative(CStatistics::DMinRows.DVal());

	// iterate over filters and update corresponding histograms
	const ULONG ulFilters = pstatspredDisj->UlFilters();
	for (ULONG ul = 0; ul < ulFilters; ul++)
	{
		CStatsPred *pstatspredChild = pstatspredDisj->Pstatspred(ul);

		// get the components of the statistics filter
		ULONG ulColId = pstatspredChild->UlColId();

		if (CStatsPredUtils::FUnsupportedPredOnDefinedCol(pstatspredChild))
		{
			CStatsPredUnsupported *pstatspredUnsupported = CStatsPredUnsupported::PstatspredConvert(pstatspredChild);
			pdrgpdScaleFactor->Append(GPOS_NEW(pmp) CDouble(pstatspredUnsupported->DScaleFactor()));

			continue;
		}

		if (FNewStatsColumn(ulColId, ulColIdPrev))
		{
			pdrgpdScaleFactor->Append(GPOS_NEW(pmp) CDouble(dScaleFactorPrev.DVal()));
			CStatisticsUtils::UpdateDisjStatistics
								(
								pmp,
								pbsStatsNonUpdateableCols,
								dRowsInput,
								dRowsCumulative,
								phistPrev,
								phmulhistResultDisj,
								ulColIdPrev
								);
			phistPrev = NULL;
		}

		CHistogram *phist = phmulhistInput->PtLookup(&ulColId);
		CHistogram *phistDisjChildCol = NULL;

		BOOL fPredSimple = !CStatsPredUtils::FConjOrDisjPred(pstatspredChild);
		BOOL fColIdPresent = (ULONG_MAX != ulColId);
		HMUlHist *phmulhistChild = NULL;
		CDouble dScaleFactorChild(1.0);

		if (fPredSimple)
		{
			GPOS_ASSERT(NULL != phist);
			phistDisjChildCol = CHistogramUtils::PhistSimpleFilter(pmp, pstatspredChild, pbsFilterColIds, phist, &dScaleFactorChild, &ulColIdPrev);

			CHistogram *phistInput = phmulhistInput->PtLookup(&ulColId);
			GPOS_ASSERT(NULL != phistInput);
			if (phistInput->FEmpty())
			{
				// input histogram is empty so scaling factor does not make sense.
				// if the input itself is empty, then scaling factor is of no effect
				dScaleFactorChild = 1 / CHistogram::DDefaultSelectivity;
			}
		}
		else
		{
			phmulhistChild = CHistogramUtils::PhmulhistApplyConjOrDisjFilter
								  	  			(
								  	  			pmp,
												pstatsconf,
												phmulhistInput,
												dRowsInput,
												pstatspredChild,
												&dScaleFactorChild
								  	  			);

			GPOS_ASSERT_IMP(CStatsPred::EsptDisj == pstatspredChild->Espt(), ULONG_MAX != ulColId);

			if (fColIdPresent)
			{
				// conjunction or disjunction uses only a single column
				phistDisjChildCol = phmulhistChild->PtLookup(&ulColId)->PhistCopy(pmp);
			}
		}

		CDouble dRowsDisjChild = dRowsInput / dScaleFactorChild;
		if (fColIdPresent)
		{
			// 1. a simple predicate (a == 5), (b LIKE "%%GOOD%%")
			// 2. conjunctive / disjunctive predicate where each of its component are predicates on the same column
			// e.g. (a <= 5 AND a >= 1), a in (5, 1)
			GPOS_ASSERT(NULL != phistDisjChildCol);

			if (NULL == phistPrev)
			{
				phistPrev = phistDisjChildCol;
				dRowsCumulative = dRowsDisjChild;
			}
			else
			{
				// statistics operation already conducted on this column
				CDouble dRowOutput(0.0);
				CHistogram *phistNew = phistPrev->PhistUnionNormalized(pmp, dRowsCumulative, phistDisjChildCol, dRowsDisjChild, &dRowOutput);
				dRowsCumulative = dRowOutput;

				GPOS_DELETE(phistPrev);
				GPOS_DELETE(phistDisjChildCol);
				phistPrev = phistNew;
			}

			dScaleFactorPrev = dRowsInput / std::max(CStatistics::DMinRows.DVal(), dRowsCumulative.DVal());
			ulColIdPrev = ulColId;
		}
		else
		{
			// conjunctive predicate where each of it component are predicates on different columns
			// e.g. ((a <= 5) AND (b LIKE "%%GOOD%%"))
			GPOS_ASSERT(NULL != phmulhistChild);
			GPOS_ASSERT(NULL == phistDisjChildCol);

			CDouble dRowsCurrentEst = dRowsInput / CScaleFactorUtils::DScaleFactorCumulativeDisj(pstatsconf, pdrgpdScaleFactor, dRowsInput);
			HMUlHist *phmulhistMerge = CStatisticsUtils::PhmulhistMergeAfterDisjChild
													  	  (
													  	  pmp,
													  	  pbsStatsNonUpdateableCols,
													  	  phmulhistResultDisj,
													  	  phmulhistChild,
													  	  dRowsCurrentEst,
													  	  dRowsDisjChild
													  	  );
			phmulhistResultDisj->Release();
			phmulhistResultDisj = phmulhistMerge;

			phistPrev = NULL;
			dScaleFactorPrev = dScaleFactorChild;
			ulColIdPrev = ulColId;
		}

		CRefCount::SafeRelease(phmulhistChild);
	}

	// process the result and scaling factor of the last predicate
	CStatisticsUtils::UpdateDisjStatistics
						(
						pmp,
						pbsStatsNonUpdateableCols,
						dRowsInput,
						dRowsCumulative,
						phistPrev,
						phmulhistResultDisj,
						ulColIdPrev
						);
	phistPrev = NULL;
	pdrgpdScaleFactor->Append(GPOS_NEW(pmp) CDouble(std::max(CStatistics::DMinRows.DVal(), dScaleFactorPrev.DVal())));

	*pdScaleFactor = CScaleFactorUtils::DScaleFactorCumulativeDisj(pstatsconf, pdrgpdScaleFactor, dRowsInput);

	AddHistograms(pmp, phmulhistInput, phmulhistResultDisj);

	pbsStatsNonUpdateableCols->Release();

	// clean up
	pdrgpdScaleFactor->Release();
	pbsFilterColIds->Release();

	return phmulhistResultDisj;
}

// check if the join cardinality estimation can be done based on NDV alone
BOOL
CHistogramUtils::FNDVBasedCardEstimation
	(
	const CHistogram *phist
	)
{
	GPOS_ASSERT(NULL != phist);

	if (0 == phist->UlBuckets())
	{
		// no buckets, so join cardinality estimation is based solely on NDV remain
		return true;
	}

	const IBucket *pbucket = (*phist->Pdrgpbucket())[0];

	IDatum *pdatum = pbucket->PpLower()->Pdatum();

	IMDType::ETypeInfo eti = pdatum->Eti();
	if (IMDType::EtiInt2 == eti ||
		IMDType::EtiInt4 == eti ||
		IMDType::EtiInt8 == eti ||
		IMDType::EtiBool == eti ||
		IMDType::EtiOid == eti )
	{
		return false;
	}

	BOOL fRes = true;
	if (pdatum->FStatsMappable())
	{
		IDatumStatisticsMappable *pdatumMappable = (IDatumStatisticsMappable *) pdatum;

		if (pdatumMappable->FHasStatsDoubleMapping())
		{
			fRes = false;
		}
	}

	return fRes;
}

// check if the column is a new column for statistic calculation
BOOL
CHistogramUtils::FNewStatsColumn
	(
	ULONG ulColId,
	ULONG ulColIdLast
	)
{
	return (ULONG_MAX == ulColId || ulColId != ulColIdLast);
}

// EOF

