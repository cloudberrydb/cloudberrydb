//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CStatistics.cpp
//
//	@doc:
//		Histograms based statistics
//---------------------------------------------------------------------------

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CHistogramUtils.h"

#include "naucrates/statistics/CScaleFactorUtils.h"

#include "gpos/common/CBitSet.h"
#include "gpos/sync/CAutoMutex.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/mdcache/CMDAccessor.h"


#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;

// default number of rows in relation
const CDouble CStatistics::DDefaultRelationRows(1000.0);

// epsilon to be used for various computations
const CDouble CStatistics::DEpsilon(0.001);

// default column width
const CDouble CStatistics::DDefaultColumnWidth(8.0);

// minimum number of rows in relation
const CDouble CStatistics::DMinRows(1.0);

// default number of distinct values
const CDouble CStatistics::DDefaultDistinctValues(1000.0);

// the default value for operators that have no cardinality estimation risk
const ULONG CStatistics::ulStatsEstimationNoRisk = 1;

// ctor
CStatistics::CStatistics
	(
	IMemoryPool *pmp,
	HMUlHist *phmulhist,
	HMUlDouble *phmuldoubleWidth,
	CDouble dRows,
	BOOL fEmpty,
	ULONG ulNumPredicates
	)
	:
	m_phmulhist(phmulhist),
	m_phmuldoubleWidth(phmuldoubleWidth),
	m_dRows(dRows),
	m_ulStatsEstimationRisk(ulStatsEstimationNoRisk),
	m_fEmpty(fEmpty),
	m_dRebinds(1.0), // by default, a stats object is rebound to parameters only once
	m_ulNumPredicates(ulNumPredicates),
	m_pdrgpubndvs(NULL)
{
	GPOS_ASSERT(NULL != m_phmulhist);
	GPOS_ASSERT(NULL != m_phmuldoubleWidth);
	GPOS_ASSERT(CDouble(0.0) <= m_dRows);

	// hash map for source id -> max source cardinality mapping
	m_pdrgpubndvs = GPOS_NEW(pmp) DrgPubndvs(pmp);

	m_pstatsconf = COptCtxt::PoctxtFromTLS()->Poconf()->Pstatsconf();
}

// Dtor
CStatistics::~CStatistics()
{
	m_phmulhist->Release();
	m_phmuldoubleWidth->Release();
	m_pdrgpubndvs->Release();
}

// look up the width of a particular column
const CDouble *
CStatistics::PdWidth
	(
	ULONG ulColId
	)
	const
{
	return m_phmuldoubleWidth->PtLookup(&ulColId);
}


//	cap the total number of distinct values (NDVs) in buckets to the number of rows
void
CStatistics::CapNDVs
	(
	CDouble dRows,
	HMUlHist *phmulhist
	)
{
	GPOS_ASSERT(NULL != phmulhist);
	HMIterUlHist hmiterulhist(phmulhist);
	while (hmiterulhist.FAdvance())
	{
		CHistogram *phist = const_cast<CHistogram *>(hmiterulhist.Pt());
		phist->CapNDVs(dRows);
	}
}

// create new structure from a list of statistics filters
CStatistics *
CStatistics::PstatsFilter
	(
	IMemoryPool *pmp,
	CStatsPred *pstatspredBase,
	BOOL fCapNdvs
	)
	const
{
	GPOS_ASSERT(NULL != pstatspredBase);

	CDouble dRowsInput = std::max(DMinRows.DVal(), m_dRows.DVal());
	CDouble dScaleFactor(1.0);
	ULONG ulNumPredicates = 1;
	CDouble dRowsFilter = DMinRows;
	HMUlHist *phmulhistNew = NULL;
	if (FEmpty())
	{
		phmulhistNew = GPOS_NEW(pmp) HMUlHist(pmp);
		CHistogramUtils::AddEmptyHistogram(pmp, phmulhistNew, m_phmulhist);
	}
	else
	{
		if (CStatsPred::EsptDisj == pstatspredBase->Espt())
		{
			CStatsPredDisj *pstatspred = CStatsPredDisj::PstatspredConvert(pstatspredBase);

			phmulhistNew  = CHistogramUtils::PhmulhistApplyDisjFilter
												(
												pmp,
												m_pstatsconf,
												m_phmulhist,
												dRowsInput,
												pstatspred,
												&dScaleFactor
												);
		}
		else
		{
			GPOS_ASSERT(CStatsPred::EsptConj == pstatspredBase->Espt());
			CStatsPredConj *pstatspred = CStatsPredConj::PstatspredConvert(pstatspredBase);
			ulNumPredicates = pstatspred->UlFilters();
			phmulhistNew = CHistogramUtils::PhmulhistApplyConjFilter
											(
											pmp,
											m_pstatsconf,
											m_phmulhist,
											dRowsInput,
											pstatspred,
											&dScaleFactor
											);
		}

		GPOS_ASSERT(DMinRows.DVal() <= dScaleFactor.DVal());
		dRowsFilter = dRowsInput / dScaleFactor;
		dRowsFilter = std::max(DMinRows.DVal(), dRowsFilter.DVal());
	}

	GPOS_ASSERT(dRowsFilter.DVal() <= dRowsInput.DVal());
	m_phmuldoubleWidth->AddRef();

	if (fCapNdvs)
	{
		CapNDVs(dRowsFilter, phmulhistNew);
	}

	CStatistics *pstatsFilter = GPOS_NEW(pmp) CStatistics
												(
												pmp,
												phmulhistNew,
												m_phmuldoubleWidth,
												dRowsFilter,
												FEmpty(),
												m_ulNumPredicates + ulNumPredicates
												);

	// since the filter operation is reductive, we choose the bounding method that takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated output cardinality
	ComputeCardUpperBounds(pmp, pstatsFilter, dRowsFilter, CStatistics::EcbmMin /* ecbm */);

	return pstatsFilter;
}

// helper print function
IOstream &
CStatistics::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "{" << std::endl;
	os << "Rows = " << DRows() << std::endl;
	os << "Rebinds = " << DRebinds() << std::endl;

	HMIterUlHist hmiterulhist(m_phmulhist);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());
		os << "Col" << ulColId << ":" << std::endl;
		const CHistogram *phist = hmiterulhist.Pt();
		phist->OsPrint(os);
		os << std::endl;
	}

	HMIterUlDouble hmiteruldouble(m_phmuldoubleWidth);
	while (hmiteruldouble.FAdvance())
	{
		ULONG ulColId = *(hmiteruldouble.Pk());
		os << "Col" << ulColId << ":" << std::endl;
		const CDouble *pdWidth = hmiteruldouble.Pt();
		os << " width " << (*pdWidth) << std::endl;
	}

	const ULONG ulLen = m_pdrgpubndvs->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const CUpperBoundNDVs *pubndv = (*m_pdrgpubndvs)[ul];
		pubndv->OsPrint(os);
	}
	os << "StatsEstimationRisk = " << UlStatsEstimationRisk() << std::endl;
	os << "}" << std::endl;

	return os;
}

//	return the total number of rows for this statistics object
CDouble
CStatistics::DRows() const
{
	return m_dRows;
}

// return the estimated skew of the given column
CDouble
CStatistics::DSkew
	(
	ULONG ulColId
	)
	const
{
	CHistogram *phist = m_phmulhist->PtLookup(&ulColId);
	if (NULL == phist)
	{
		return CDouble(1.0);
	}

	return phist->DSkew();
}

// return total width in bytes
CDouble
CStatistics::DWidth() const
{
	CDouble dWidth(0.0);
	HMIterUlDouble hmiteruldouble(m_phmuldoubleWidth);
	while (hmiteruldouble.FAdvance())
	{
		const CDouble *pdWidth = hmiteruldouble.Pt();
		dWidth = dWidth + (*pdWidth);
	}
	return dWidth.FpCeil();
}

// return the width in bytes of a set of columns
CDouble
CStatistics::DWidth
	(
	DrgPul *pdrgpulColIds
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpulColIds);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CDouble dWidth(0.0);
	const ULONG ulSize = pdrgpulColIds->UlLength();
	for (ULONG ulIdx = 0; ulIdx < ulSize; ulIdx++)
	{
		ULONG ulColId = *((*pdrgpulColIds)[ulIdx]);
		CDouble *pdWidth = m_phmuldoubleWidth->PtLookup(&ulColId);
		if (NULL != pdWidth)
		{
			dWidth = dWidth + (*pdWidth);
		}
		else
		{
			CColRef *pcr = pcf->PcrLookup(ulColId);
			GPOS_ASSERT(NULL != pcr);

			dWidth = dWidth + CStatisticsUtils::DDefaultColumnWidth(pcr->Pmdtype());
		}
	}
	return dWidth.FpCeil();
}

// return width in bytes of a set of columns
CDouble
CStatistics::DWidth
	(
	IMemoryPool *pmp,
	CColRefSet *pcrs
	)
	const
{
	GPOS_ASSERT(NULL != pcrs);

	DrgPul *pdrgpulColIds = GPOS_NEW(pmp) DrgPul(pmp);
	pcrs->ExtractColIds(pmp, pdrgpulColIds);

	CDouble dWidth = DWidth(pdrgpulColIds);
	pdrgpulColIds->Release();

	return dWidth;
}

// return dummy statistics object
CStatistics *
CStatistics::PstatsDummy
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulColIds,
	CDouble dRows
	)
{
	GPOS_ASSERT(NULL != pdrgpulColIds);

	// hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	// hashmap from colid -> width (double)
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	BOOL fEmpty = (CStatistics::DEpsilon >= dRows);
	CHistogramUtils::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhist, phmuldoubleWidth, pdrgpulColIds, fEmpty);

	CStatistics *pstats = GPOS_NEW(pmp) CStatistics(pmp, phmulhist, phmuldoubleWidth, dRows, fEmpty);
	CreateAndInsertUpperBoundNDVs(pmp, pstats, pdrgpulColIds, dRows);

	return pstats;
}

// add upper bound ndvs information for a given set of columns
void
CStatistics::CreateAndInsertUpperBoundNDVs
	(
	IMemoryPool *pmp,
	CStatistics *pstats,
	DrgPul *pdrgpulColIds,
	CDouble dRows
)
{
	GPOS_ASSERT(NULL != pstats);
	GPOS_ASSERT(NULL != pdrgpulColIds);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	const ULONG ulCols = pdrgpulColIds->UlLength();
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		ULONG ulColId = *(*pdrgpulColIds)[ul];
		const CColRef *pcr = pcf->PcrLookup(ulColId);
		if (NULL != pcr)
		{
			pcrs->Include(pcr);
		}
	}

	if (0 < pcrs->CElements())
	{
		pstats->AddCardUpperBound(GPOS_NEW(pmp) CUpperBoundNDVs(pcrs, dRows));
	}
	else
	{
		pcrs->Release();
	}
}

//	return dummy statistics object
CStatistics *
CStatistics::PstatsDummy
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulHistColIds,
	DrgPul *pdrgpulWidthColIds,
	CDouble dRows
	)
{
	GPOS_ASSERT(NULL != pdrgpulHistColIds);
	GPOS_ASSERT(NULL != pdrgpulWidthColIds);

	BOOL fEmpty = (CStatistics::DEpsilon >= dRows);
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	// hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	const ULONG ulHistCol = pdrgpulHistColIds->UlLength();
	for (ULONG ul = 0; ul < ulHistCol; ul++)
	{
		ULONG ulColId = *(*pdrgpulHistColIds)[ul];

		CColRef *pcr = pcf->PcrLookup(ulColId);
		GPOS_ASSERT(NULL != pcr);

		// empty histogram
		CHistogram *phist = CHistogram::PhistDefault(pmp, pcr, fEmpty);
		phmulhist->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phist);
	}

	// hashmap from colid -> width (double)
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	const ULONG ulWidthCol = pdrgpulWidthColIds->UlLength();
	for (ULONG ul = 0; ul < ulWidthCol; ul++)
	{
		ULONG ulColId = *(*pdrgpulWidthColIds)[ul];

		CColRef *pcr = pcf->PcrLookup(ulColId);
		GPOS_ASSERT(NULL != pcr);

		CDouble dWidth = CStatisticsUtils::DDefaultColumnWidth(pcr->Pmdtype());
		phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColId), GPOS_NEW(pmp) CDouble(dWidth));
	}

	CStatistics *pstats = GPOS_NEW(pmp) CStatistics(pmp, phmulhist, phmuldoubleWidth, dRows, false /* fEmpty */);
	CreateAndInsertUpperBoundNDVs(pmp, pstats, pdrgpulHistColIds, dRows);

	return pstats;
}

// main driver to generate join stats
CStatistics *
CStatistics::PstatsJoinDriver
	(
	IMemoryPool *pmp,
	const IStatistics *pistatsOther,
	DrgPstatspredjoin *pdrgpstatspredjoin,
	IStatistics::EStatsJoinType esjt,
	BOOL fIgnoreLasjHistComputation
	)
	const
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pistatsOther);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);

	BOOL fLASJ = (IStatistics::EsjtLeftAntiSemiJoin == esjt);
	BOOL fSemiJoin = IStatistics::FSemiJoin(esjt);

	const CStatistics *pstatsOther = dynamic_cast<const CStatistics *> (pistatsOther);

	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhistJoin = GPOS_NEW(pmp) HMUlHist(pmp);

	// build a bitset with all join columns
	CBitSet *pbsJoinColIds = GPOS_NEW(pmp) CBitSet(pmp);
	for (ULONG ul = 0; ul < pdrgpstatspredjoin->UlLength(); ul++)
	{
		CStatsPredJoin *pstatsjoin = (*pdrgpstatspredjoin)[ul];
		(void) pbsJoinColIds->FExchangeSet(pstatsjoin->UlColId1());
		if (!fSemiJoin)
		{
			(void) pbsJoinColIds->FExchangeSet(pstatsjoin->UlColId2());
		}
	}

	// histograms on columns that do not appear in join condition will
	// be copied over to the result structure
	AddNotExcludedHistograms(pmp, pbsJoinColIds, phmulhistJoin);
	if (!fSemiJoin)
	{
		pstatsOther->AddNotExcludedHistograms(pmp, pbsJoinColIds, phmulhistJoin);
	}

	DrgPdouble *pdrgpd = GPOS_NEW(pmp) DrgPdouble(pmp);
	const ULONG ulJoinConds = pdrgpstatspredjoin->UlLength();

	BOOL fEmptyOutput = false;
	// iterate over joins
	for (ULONG ul = 0; ul < ulJoinConds; ul++)
	{
		CStatsPredJoin *pstatsjoin = (*pdrgpstatspredjoin)[ul];
		ULONG ulColId1 = pstatsjoin->UlColId1();
		ULONG ulColId2 = pstatsjoin->UlColId2();

		GPOS_ASSERT(ulColId1 != ulColId2);
		// find the histograms corresponding to the two columns
		CHistogram *phist1 = m_phmulhist->PtLookup(&ulColId1);
		CHistogram *phist2 = pstatsOther->m_phmulhist->PtLookup(&ulColId2);
		GPOS_ASSERT(NULL != phist1);
		GPOS_ASSERT(NULL != phist2);

		BOOL fEmptyInput = FEmptyJoinInput(this, pstatsOther, fLASJ);

		CDouble dScaleFactorLocal(1.0);
		CHistogram *phist1After = NULL;
		CHistogram *phist2After = NULL;
		JoinHistograms
			(
			pmp,
			phist1,
			phist2,
			pstatsjoin,
			DRows(),
			pstatsOther->DRows(),
			fLASJ,
			&phist1After,
			&phist2After,
			&dScaleFactorLocal,
			fEmptyInput,
			fIgnoreLasjHistComputation
			);

		fEmptyOutput = FEmptyJoinStats(FEmpty(), fEmptyOutput, fLASJ, phist1, phist2, phist1After);

		CStatisticsUtils::AddHistogram(pmp, ulColId1, phist1After, phmulhistJoin);
		if (!fSemiJoin)
		{
			CStatisticsUtils::AddHistogram(pmp, ulColId2, phist2After, phmulhistJoin);
		}
		GPOS_DELETE(phist1After);
		GPOS_DELETE(phist2After);

		pdrgpd->Append(GPOS_NEW(pmp) CDouble(dScaleFactorLocal));
	}

	CDouble dRowsJoin = DJoinCardinality(m_pstatsconf, m_dRows, pstatsOther->m_dRows, pdrgpd, esjt);
	if (fEmptyOutput)
	{
		dRowsJoin = DMinRows;
	}

	// clean up
	pdrgpd->Release();
	pbsJoinColIds->Release();

	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);
	CStatistics::AddWidthInfo(pmp, m_phmuldoubleWidth, phmuldoubleWidth);
	if (!fSemiJoin)
	{
		CStatistics::AddWidthInfo(pmp, pstatsOther->m_phmuldoubleWidth, phmuldoubleWidth);
	}

	// create an output stats object
	CStatistics *pstatsJoin = GPOS_NEW(pmp) CStatistics
											(
											pmp,
											phmulhistJoin,
											phmuldoubleWidth,
											dRowsJoin,
											fEmptyOutput,
											m_ulNumPredicates
											);

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	ComputeCardUpperBounds(pmp, pstatsJoin, dRowsJoin, CStatistics::EcbmMin /* ecbm */);
	if (!fSemiJoin)
	{
		pstatsOther->ComputeCardUpperBounds(pmp, pstatsJoin, dRowsJoin, CStatistics::EcbmMin /* ecbm */);
	}

	return pstatsJoin;
}

//	check if the input statistics from join statistics computation empty
BOOL
CStatistics::FEmptyJoinInput
	(
	const CStatistics *pstatsOuter,
	const CStatistics *pstatsInner,
	BOOL fLASJ
	)
{
	GPOS_ASSERT(NULL != pstatsOuter);
	GPOS_ASSERT(NULL != pstatsInner);

	if (fLASJ)
	{
		return pstatsOuter->FEmpty();
	}

	return pstatsOuter->FEmpty() || pstatsInner->FEmpty();
}


// check if the join statistics object is empty output based on the input
// histograms and the join histograms
BOOL
CStatistics::FEmptyJoinStats
	(
	BOOL fEmptyOuter,
	BOOL fEmptyOutput,
	BOOL fLASJ,
	CHistogram *phistOuter,
	CHistogram *phistInner,
	CHistogram *phistJoin
	)
{
	GPOS_ASSERT(NULL != phistOuter);
	GPOS_ASSERT(NULL != phistInner);
	GPOS_ASSERT(NULL != phistJoin);

	return fEmptyOutput ||
		   (!fLASJ && fEmptyOuter) ||
		   (!phistOuter->FEmpty() && !phistInner->FEmpty() && phistJoin->FEmpty());
	}

// return join cardinality based on scaling factor and join type
CDouble
CStatistics::DJoinCardinality
	(
	CStatisticsConfig *pstatsconf,
	CDouble dRowsLeft,
	CDouble dRowsRight,
	DrgPdouble *pdrgpd,
	IStatistics::EStatsJoinType esjt
	)
{
	GPOS_ASSERT(NULL != pstatsconf);
	GPOS_ASSERT(NULL != pdrgpd);

	CDouble dScaleFactor = CScaleFactorUtils::DCumulativeJoinScaleFactor(pstatsconf, pdrgpd);
	CDouble dCartesianProduct = dRowsLeft * dRowsRight;

	BOOL fLASJ = IStatistics::EsjtLeftAntiSemiJoin == esjt;
	BOOL fLeftSemiJoin = IStatistics::EsjtLeftSemiJoin == esjt;
	if (fLASJ || fLeftSemiJoin)
	{
		CDouble dRows = dRowsLeft;

		if (fLASJ)
		{
			dRows = dRowsLeft / dScaleFactor;
		}
		else
		{
			// semi join results cannot exceed size of outer side
			dRows = std::min(dRowsLeft.DVal(), (dCartesianProduct / dScaleFactor).DVal());
		}

		return std::max(DOUBLE(1.0), dRows.DVal());
	}

	GPOS_ASSERT(DMinRows <= dScaleFactor);

	return std::max(DMinRows.DVal(), (dCartesianProduct / dScaleFactor).DVal());
}

//	return statistics object after performing LOJ operation with another statistics structure
CStatistics *
CStatistics::PstatsLOJ
	(
	IMemoryPool *pmp,
	const IStatistics *pstatsOther,
	DrgPstatspredjoin *pdrgpstatspredjoin
	)
	const
{
	GPOS_ASSERT(NULL != pstatsOther);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);

	CStatistics *pstatsInnerJoin = PstatsInnerJoin(pmp, pstatsOther, pdrgpstatspredjoin);
	CDouble dRowsInnerJoin = pstatsInnerJoin->DRows();
	CDouble dRowsLASJ(1.0);

	const CStatistics *pstatsInnerSide = dynamic_cast<const CStatistics *> (pstatsOther);

	// create a new hash map of histograms, for each column from the outer child
	// add the buckets that do not contribute to the inner join
	HMUlHist *phmulhistLOJ = PhmulhistLOJ
								(
								pmp,
								this,
								pstatsInnerSide,
								pstatsInnerJoin,
								pdrgpstatspredjoin,
								dRowsInnerJoin,
								&dRowsLASJ
								);

	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);
	CStatistics::AddWidthInfo(pmp, pstatsInnerJoin->m_phmuldoubleWidth, phmuldoubleWidth);

	pstatsInnerJoin->Release();

	// cardinality of LOJ is at least the cardinality of the outer child
	CDouble dRowsLOJ = std::max(DRows(), dRowsInnerJoin + dRowsLASJ);

	// create an output stats object
	CStatistics *pstatsLOJ = GPOS_NEW(pmp) CStatistics
										(
										pmp,
										phmulhistLOJ,
										phmuldoubleWidth,
										dRowsLOJ,
										FEmpty(),
										m_ulNumPredicates
										);

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	// modify source id to upper bound card information
	ComputeCardUpperBounds(pmp, pstatsLOJ, dRowsLOJ, CStatistics::EcbmMin /* ecbm */);
	pstatsInnerSide->ComputeCardUpperBounds(pmp, pstatsLOJ, dRowsLOJ, CStatistics::EcbmMin /* ecbm */);

	return pstatsLOJ;
}

//	create a new hash map of histograms for LOJ from the histograms
//	of the outer child and the histograms of the inner join
HMUlHist *
CStatistics::PhmulhistLOJ
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
CStatistics::AddHistogramsLOJInner
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

//	return statistics object after performing semi-join with another statistics structure
CStatistics *
CStatistics::PstatsLSJoin
	(
	IMemoryPool *pmp,
	const IStatistics *pstatsInner,
	DrgPstatspredjoin *pdrgpstatspredjoin
	)
	const
{
	GPOS_ASSERT(NULL != pstatsInner);
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
	IStatistics *pstatsInnerNoDups = pstatsInner->PstatsGroupBy
													(
													pmp,
													pdrgpulInnerColumnIds,
													pdrgpulAgg,
													NULL // pbsKeys: no keys, use all grouping cols
													);

	CStatistics *pstatsSemiJoin = PstatsJoinDriver
									(
									pmp,
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

// helper for LAS-joining histograms
void
CStatistics::LASJoinHistograms
	(
	IMemoryPool *pmp,
	CHistogram *phist1,
	CHistogram *phist2,
	CStatsPredJoin *pstatsjoin,
	CDouble dRows1,
	CDouble ,//dRows2,
	CHistogram **pphist1, // output: histogram 1 after join
	CHistogram **pphist2, // output: histogram 2 after join
	CDouble *pdScaleFactor, // output: scale factor based on the join
	BOOL fEmptyInput,
	BOOL fIgnoreLasjHistComputation
	)
{
	GPOS_ASSERT(NULL != phist1);
	GPOS_ASSERT(NULL != phist2);
	GPOS_ASSERT(NULL != pstatsjoin);
	GPOS_ASSERT(NULL != pphist1);
	GPOS_ASSERT(NULL != pphist2);
	GPOS_ASSERT(NULL != pdScaleFactor);

	*pdScaleFactor = 1.0;

	CStatsPred::EStatsCmpType escmpt = pstatsjoin->Escmpt();

	if (fEmptyInput)
	{
		// anti-semi join should give the full outer side.
		// use 1.0 as scale factor if anti semi join
		*pdScaleFactor = 1.0;
		*pphist1 = phist1->PhistCopy(pmp);
		*pphist2 = NULL;

		return;
	}

	BOOL fEmptyHistograms = phist1->FEmpty() || phist2->FEmpty();
	if (!fEmptyHistograms && CHistogram::FSupportsJoin(escmpt))
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

	// not supported join operator or missing stats,
	// copy input histograms and use default scale factor
	*pdScaleFactor = CDouble(CScaleFactorUtils::DDefaultScaleFactorJoin);
	*pphist1 = phist1->PhistCopy(pmp);
	*pphist2 = NULL;
}

// helper for inner-joining histograms
void
CStatistics::InnerJoinHistograms
	(
	IMemoryPool *pmp,
	CHistogram *phist1,
	CHistogram *phist2,
	CStatsPredJoin *pstatsjoin,
	CDouble dRows1,
	CDouble dRows2,
	CHistogram **pphist1, // output: histogram 1 after join
	CHistogram **pphist2, // output: histogram 2 after join
	CDouble *pdScaleFactor, // output: scale factor based on the join
	BOOL fEmptyInput
	)
{
	GPOS_ASSERT(NULL != phist1);
	GPOS_ASSERT(NULL != phist2);
	GPOS_ASSERT(NULL != pstatsjoin);
	GPOS_ASSERT(NULL != pphist1);
	GPOS_ASSERT(NULL != pphist2);
	GPOS_ASSERT(NULL != pdScaleFactor);

	*pdScaleFactor = 1.0;
	CStatsPred::EStatsCmpType escmpt = pstatsjoin->Escmpt();

	if (fEmptyInput)
	{
		// use Cartesian product as scale factor
		*pdScaleFactor = dRows1 * dRows2;
		*pphist1 =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp));
		*pphist2 =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp));

		return;
	}

	*pdScaleFactor = CScaleFactorUtils::DDefaultScaleFactorJoin;
	
	BOOL fEmptyHistograms = phist1->FEmpty() || phist2->FEmpty();

	if (fEmptyHistograms)
	{
		// if one more input has no histograms (due to lack of statistics
		// for table columns or computed columns), we estimate
		// the join cardinality to be the max of the two rows.
		// In other words, the scale factor is equivalent to the
		// min of the two rows.
		*pdScaleFactor = std::min(dRows1, dRows2);
	}
	else if (CHistogram::FSupportsJoin(escmpt))
	{
		CHistogram *phistJoin = phist1->PhistJoinNormalized
											(
											pmp,
											escmpt,
											dRows1,
											phist2,
											dRows2,
											pdScaleFactor
											);

		if (CStatsPred::EstatscmptEq == escmpt || CStatsPred::EstatscmptINDF == escmpt)
		{
			if (phist1->FScaledNDV())
			{
				phistJoin->SetNDVScaled();
			}
			*pphist1 = phistJoin;
			*pphist2 = (*pphist1)->PhistCopy(pmp);
			if (phist2->FScaledNDV())
			{
				(*pphist2)->SetNDVScaled();
			}
			return;
		}

		// note that IDF and Not Equality predicate we do not generate histograms but
		// just the scale factors.

		GPOS_ASSERT(phistJoin->FEmpty());
		GPOS_DELETE(phistJoin);
			
		// TODO:  Feb 21 2014, for all join condition except for "=" join predicate 
		// we currently do not compute new histograms for the join columns
	}

	// not supported join operator or missing histograms,
	// copy input histograms and use default scale factor
	*pphist1 = phist1->PhistCopy(pmp);
	*pphist2 = phist2->PhistCopy(pmp);
}

// helper for joining histograms
void
CStatistics::JoinHistograms
	(
	IMemoryPool *pmp,
	CHistogram *phist1,
	CHistogram *phist2,
	CStatsPredJoin *pstatsjoin,
	CDouble dRows1,
	CDouble dRows2,
	BOOL fLASJ, // if true, use anti-semi join semantics, otherwise use inner join semantics
	CHistogram **pphist1, // output: histogram 1 after join
	CHistogram **pphist2, // output: histogram 2 after join
	CDouble *pdScaleFactor, // output: scale factor based on the join
	BOOL fEmptyInput,
	BOOL fIgnoreLasjHistComputation
	)
{
	GPOS_ASSERT(NULL != phist1);
	GPOS_ASSERT(NULL != phist2);
	GPOS_ASSERT(NULL != pstatsjoin);
	GPOS_ASSERT(NULL != pphist1);
	GPOS_ASSERT(NULL != pphist2);
	GPOS_ASSERT(NULL != pdScaleFactor);

	if (fLASJ)
	{
		LASJoinHistograms
			(
			pmp,
			phist1,
			phist2,
			pstatsjoin,
			dRows1,
			dRows2,
			pphist1,
			pphist2,
			pdScaleFactor,
			fEmptyInput,
			fIgnoreLasjHistComputation
			);

		return;
	}

	InnerJoinHistograms(pmp, phist1, phist2, pstatsjoin, dRows1, dRows2, pphist1, pphist2, pdScaleFactor, fEmptyInput);
}


// return statistics object after performing inner join
CStatistics *
CStatistics::PstatsInnerJoin
	(
	IMemoryPool *pmp,
	const IStatistics *pistatsOther,
	DrgPstatspredjoin *pdrgpstatspredjoin
	)
	const
{
	GPOS_ASSERT(NULL != pistatsOther);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);

	return PstatsJoinDriver
			(
			pmp,
			pistatsOther,
			pdrgpstatspredjoin,
			IStatistics::EsjtInnerJoin /* esjt */,
			true /* fIgnoreLasjHistComputation */
			);
}

//		return statistics object after performing LASJ
CStatistics *
CStatistics::PstatsLASJoin
	(
	IMemoryPool *pmp,
	const IStatistics *pistatsOther,
	DrgPstatspredjoin *pdrgpstatspredjoin,
	BOOL fIgnoreLasjHistComputation
	)
	const
{
	GPOS_ASSERT(NULL != pistatsOther);
	GPOS_ASSERT(NULL != pdrgpstatspredjoin);

	return PstatsJoinDriver
			(
			pmp,
			pistatsOther,
			pdrgpstatspredjoin,
			IStatistics::EsjtLeftAntiSemiJoin /* esjt */,
			fIgnoreLasjHistComputation
			);
}

// return statistics object after Group by computation
CStatistics *
CStatistics::PstatsGroupBy
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulGC,
	DrgPul *pdrgpulAgg,
	CBitSet *pbsKeys
	)
	const
{
	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhist = GPOS_NEW(pmp) HMUlHist(pmp);

	// hash map colid -> width
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	CStatistics *pstatsAgg = NULL;
	CDouble dRowsAgg = DMinRows;
	if (FEmpty())
	{
		// add dummy histograms for the aggregates and grouping columns
		CHistogramUtils::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhist, phmuldoubleWidth, pdrgpulAgg, true /* fEmpty */);
		CHistogramUtils::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhist, phmuldoubleWidth, pdrgpulGC, true /* fEmpty */);

		pstatsAgg = GPOS_NEW(pmp) CStatistics(pmp, phmulhist, phmuldoubleWidth, dRowsAgg, true /* fEmpty */);
	}
	else
	{
		// for computed aggregates, we're not going to be very smart right now
		CHistogramUtils::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhist, phmuldoubleWidth, pdrgpulAgg, false /* fEmpty */);

		CColRefSet *pcrsGrpColComputed = GPOS_NEW(pmp) CColRefSet(pmp);
		CColRefSet *pcrsGrpColsForStats = CStatisticsUtils::PcrsGrpColsForStats(pmp, pdrgpulGC, pcrsGrpColComputed);

		// add statistical information of columns (1) used to compute the cardinality of the aggregate
		// and (2) the grouping columns that are computed
		CStatisticsUtils::AddGrpColStats(pmp, this, pcrsGrpColsForStats, phmulhist, phmuldoubleWidth);
		CStatisticsUtils::AddGrpColStats(pmp, this, pcrsGrpColComputed, phmulhist, phmuldoubleWidth);

		DrgPdouble *pdrgpdNDV = CStatisticsUtils::PdrgPdoubleNDV(pmp, m_pstatsconf, this, pcrsGrpColsForStats, pbsKeys);
		CDouble dGroups = CStatisticsUtils::DNumOfDistinctVal(m_pstatsconf, pdrgpdNDV);

		// clean up
		pcrsGrpColsForStats->Release();
		pcrsGrpColComputed->Release();
		pdrgpdNDV->Release();

		dRowsAgg = std::min(std::max(DMinRows.DVal(), dGroups.DVal()), m_dRows.DVal());

		// create a new stats object for the output
		pstatsAgg = GPOS_NEW(pmp) CStatistics(pmp, phmulhist, phmuldoubleWidth, dRowsAgg, FEmpty());
	}

	// In the output statistics object, the upper bound source cardinality of the grouping column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated group by cardinality.

	// modify source id to upper bound card information
	ComputeCardUpperBounds(pmp, pstatsAgg, dRowsAgg, CStatistics::EcbmMin /* ecbm */);

	return pstatsAgg;
}


//  return a statistics object for a project operation
CStatistics *
CStatistics::PstatsProject
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulProjColIds,
	HMUlDatum *phmuldatum
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpulProjColIds);

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhistNew = GPOS_NEW(pmp) HMUlHist(pmp);

	// column ids on which widths are to be computed
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	const ULONG ulLen = pdrgpulProjColIds->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		ULONG ulColId = *(*pdrgpulProjColIds)[ul];
		CHistogram *phist = m_phmulhist->PtLookup(&ulColId);

		if (NULL == phist)
		{

			// create histogram for the new project column
			DrgPbucket *pdrgbucket = GPOS_NEW(pmp) DrgPbucket(pmp);
			CDouble dNullFreq = 0.0;

			BOOL fWellDefined = false;
			if (NULL != phmuldatum)
			{
				IDatum *pdatum = phmuldatum->PtLookup(&ulColId);
				if (NULL != pdatum)
				{
					fWellDefined = true;
					if (!pdatum->FNull())
					{
						pdrgbucket->Append(CBucket::PbucketSingleton(pmp, pdatum));
					}
					else
					{
						dNullFreq = 1.0;
					}
				}
			}

			CHistogram *phistPrCol = NULL;
			CColRef *pcr = pcf->PcrLookup(ulColId);
			GPOS_ASSERT(NULL != pcr);

			if (0 == pdrgbucket->UlLength() && IMDType::EtiBool == pcr->Pmdtype()->Eti())
			{
				pdrgbucket->Release();
			 	phistPrCol = CHistogram::PhistDefaultBoolColStats(pmp);
			}
			else
			{
				phistPrCol = GPOS_NEW(pmp) CHistogram
										(
										pdrgbucket,
										fWellDefined,
										dNullFreq,
										CHistogram::DDefaultNDVRemain,
										CHistogram::DDefaultNDVFreqRemain
										);
			}

			phmulhistNew->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phistPrCol);
		}
		else
		{
			phmulhistNew->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phist->PhistCopy(pmp));
		}

		// look up width
		CDouble *pdWidth = m_phmuldoubleWidth->PtLookup(&ulColId);
		if (NULL == pdWidth)
		{
			CColRef *pcr = pcf->PcrLookup(ulColId);
			GPOS_ASSERT(NULL != pcr);

			CDouble dWidth = CStatisticsUtils::DDefaultColumnWidth(pcr->Pmdtype());
			phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColId), GPOS_NEW(pmp) CDouble(dWidth));
		}
		else
		{
			phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColId), GPOS_NEW(pmp) CDouble(*pdWidth));
		}
	}

	// create an output stats object
	CStatistics *pstatsProject = GPOS_NEW(pmp) CStatistics
											(
											pmp,
											phmulhistNew,
											phmuldoubleWidth,
											m_dRows,
											FEmpty(),
											m_ulNumPredicates
											);

	// In the output statistics object, the upper bound source cardinality of the project column
	// is equivalent the estimate project cardinality.

	ComputeCardUpperBounds(pmp, pstatsProject, m_dRows, CStatistics::EcbmInputSourceMaxCard /* ecbm */);

	// add upper bound card information for the project columns
	CreateAndInsertUpperBoundNDVs(pmp, pstatsProject, pdrgpulProjColIds, m_dRows);

	return pstatsProject;
}

//	helper method to copy statistics on columns that are not excluded by bitset
void
CStatistics::AddNotExcludedHistograms
	(
	IMemoryPool *pmp,
	CBitSet *pbsExcludedColIds,
	HMUlHist *phmulhist
	)
	const
{
	GPOS_ASSERT(NULL != pbsExcludedColIds);
	GPOS_ASSERT(NULL != phmulhist);

	HMIterUlHist hmiterulhist(m_phmulhist);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());
		if (!pbsExcludedColIds->FBit(ulColId))
		{
			const CHistogram *phist = hmiterulhist.Pt();
			CStatisticsUtils::AddHistogram(pmp, ulColId, phist, phmulhist);
		}

		GPOS_CHECK_ABORT;
	}
}

//	add width information
void
CStatistics::AddWidthInfo
	(
	IMemoryPool *pmp,
	HMUlDouble *phmuldoubleSrc,
	HMUlDouble *phmuldoubleDest
	)
{
	HMIterUlDouble hmiteruldouble(phmuldoubleSrc);
	while (hmiteruldouble.FAdvance())
	{
		ULONG ulColId = *(hmiteruldouble.Pk());
		BOOL fPresent = (NULL != phmuldoubleDest->PtLookup(&ulColId));
		if (!fPresent)
		{
			const CDouble *pdWidth = hmiteruldouble.Pt();
			CDouble *pdWidthCopy = GPOS_NEW(pmp) CDouble(*pdWidth);
			phmuldoubleDest->FInsert(GPOS_NEW(pmp) ULONG(ulColId), pdWidthCopy);
		}

		GPOS_CHECK_ABORT;
	}
}

// return statistics object after union all operation with input statistics object
CStatistics *
CStatistics::PstatsUnionAll
	(
	IMemoryPool *pmp,
	const IStatistics *pistatsOther,
	DrgPul *pdrgpulOutput,
	DrgPul *pdrgpulInput1,
	DrgPul *pdrgpulInput2
	)
	const
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pistatsOther);

	// lengths must match
	GPOS_ASSERT(pdrgpulOutput->UlLength() == pdrgpulInput1->UlLength());
	GPOS_ASSERT(pdrgpulOutput->UlLength() == pdrgpulInput2->UlLength());

	const CStatistics *pstatsOther = dynamic_cast<const CStatistics *> (pistatsOther);

	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhistNew = GPOS_NEW(pmp) HMUlHist(pmp);

	// column ids on which widths are to be computed
	HMUlDouble *phmuldoubleWidth = GPOS_NEW(pmp) HMUlDouble(pmp);

	BOOL fEmptyUnionAll = FEmpty() && pistatsOther->FEmpty();
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CDouble dRowsUnionAll = DMinRows;
	if (fEmptyUnionAll)
	{
		CHistogramUtils::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhistNew, phmuldoubleWidth, pdrgpulOutput, true /*fEmpty*/);
	}
	else
	{
		const ULONG ulLen = pdrgpulOutput->UlLength();
		for (ULONG ul = 0; ul < ulLen; ul++)
		{
			ULONG ulColIdOutput = *(*pdrgpulOutput)[ul];
			ULONG ulColIdInput1 = *(*pdrgpulInput1)[ul];
			ULONG ulColIdInput2 = *(*pdrgpulInput2)[ul];

			CHistogram *phistInput1 = m_phmulhist->PtLookup(&ulColIdInput1);
			GPOS_ASSERT(NULL != phistInput1);
			CHistogram *phistInput2 = pstatsOther->m_phmulhist->PtLookup(&ulColIdInput2);
			GPOS_ASSERT(NULL != phistInput2);

			if (phistInput1->FWellDefined() || phistInput2->FWellDefined())
			{
				CHistogram *phistOutput = phistInput1->PhistUnionAllNormalized(pmp, DRows(), phistInput2, pstatsOther->DRows());
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
			CDouble *pdWidth = m_phmuldoubleWidth->PtLookup(&ulColIdInput1);
			GPOS_ASSERT(NULL != pdWidth);
			phmuldoubleWidth->FInsert(GPOS_NEW(pmp) ULONG(ulColIdOutput), GPOS_NEW(pmp) CDouble(*pdWidth));
		}

		dRowsUnionAll = m_dRows + pstatsOther->m_dRows;
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
	ComputeCardUpperBounds(pmp, pstatsUnionAll, dRowsUnionAll, CStatistics::EcbmOutputCard /* ecbm */);

	return pstatsUnionAll;
}

//	compute the statistics of a limit operation
CStatistics *
CStatistics::PstatsLimit
	(
	IMemoryPool *pmp,
	CDouble dLimitCount
	)
	const
{
	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhistNew = GPOS_NEW(pmp) HMUlHist(pmp);

	CDouble dRowsLimit = DMinRows;
	if (FEmpty())
	{
		CHistogramUtils::AddEmptyHistogram(pmp, phmulhistNew, m_phmulhist);
	}
	else
	{
		HMIterUlHist hmiterulhist(m_phmulhist);
		while (hmiterulhist.FAdvance())
		{
			ULONG ulColId = *(hmiterulhist.Pk());
			const CHistogram *phist = hmiterulhist.Pt();
			CStatisticsUtils::AddHistogram(pmp, ulColId, phist, phmulhistNew);
		}

		// TODO:  Sept 23rd 2014, fix this to the minimum of input card and limit count
		dRowsLimit = dLimitCount;
	}

	m_phmuldoubleWidth->AddRef();

	// create an output stats object
	CStatistics *pstatsLimit = GPOS_NEW(pmp) CStatistics
											(
											pmp,
											phmulhistNew,
											m_phmuldoubleWidth,
											dRowsLimit,
											FEmpty(),
											m_ulNumPredicates
											);

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated limit cardinality.

	// modify source id to upper bound card information
	ComputeCardUpperBounds(pmp, pstatsLimit, dRowsLimit, CStatistics::EcbmMin /* ecbm */);

	return pstatsLimit;
}

//	return required props associated with statistics object
CReqdPropRelational *
CStatistics::Prprel
	(
	IMemoryPool *pmp
	)
	const
{
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	GPOS_ASSERT(NULL != pcf);

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	// add columns from histogram map
	HMIterUlHist hmiterulhist(m_phmulhist);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());
		CColRef *pcr = pcf->PcrLookup(ulColId);
		GPOS_ASSERT(NULL != pcr);

		pcrs->Include(pcr);
	}

	return GPOS_NEW(pmp) CReqdPropRelational(pcrs);
}

// append given statistics to current object
void
CStatistics::AppendStats
	(
	IMemoryPool *pmp,
	IStatistics *pstatsInput
	)
{
	CStatistics *pstats = CStatistics::PstatsConvert(pstatsInput);

	CHistogramUtils::AddHistograms(pmp, pstats->m_phmulhist, m_phmulhist);
	GPOS_CHECK_ABORT;

	AddWidthInfo(pmp, pstats->m_phmuldoubleWidth, m_phmuldoubleWidth);
	GPOS_CHECK_ABORT;
}

// copy statistics object
IStatistics *
CStatistics::PstatsCopy
	(
	IMemoryPool *pmp
	)
	const
{
	return PstatsScale(pmp, CDouble(1.0) /*dFactor*/);
}

// return a copy of this statistics object scaled by a given factor
IStatistics *
CStatistics::PstatsScale
	(
	IMemoryPool *pmp,
	CDouble dFactor
	)
	const
{
	HMUlHist *phmulhistNew = GPOS_NEW(pmp) HMUlHist(pmp);
	HMUlDouble *phmuldoubleNew = GPOS_NEW(pmp) HMUlDouble(pmp);

	CHistogramUtils::AddHistograms(pmp, m_phmulhist, phmulhistNew);
	GPOS_CHECK_ABORT;

	AddWidthInfo(pmp, m_phmuldoubleWidth, phmuldoubleNew);
	GPOS_CHECK_ABORT;

	CDouble dRowsScaled = m_dRows * dFactor;

	// create a scaled stats object
	CStatistics *pstatsScaled = GPOS_NEW(pmp) CStatistics
												(
												pmp,
												phmulhistNew,
												phmuldoubleNew,
												dRowsScaled,
												FEmpty(),
												m_ulNumPredicates
												);

	// In the output statistics object, the upper bound source cardinality of the scaled column
	// cannot be greater than the the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated output cardinality.

	// modify source id to upper bound card information
	ComputeCardUpperBounds(pmp, pstatsScaled, dRowsScaled, CStatistics::EcbmMin /* ecbm */);

	return pstatsScaled;
}

//	copy statistics object with re-mapped column ids
IStatistics *
CStatistics::PstatsCopyWithRemap
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
	const
{
	GPOS_ASSERT(NULL != phmulcr);
	HMUlHist *phmulhistNew = GPOS_NEW(pmp) HMUlHist(pmp);
	HMUlDouble *phmuldoubleNew = GPOS_NEW(pmp) HMUlDouble(pmp);

	AddHistogramsWithRemap(pmp, m_phmulhist, phmulhistNew, phmulcr, fMustExist);
	AddWidthInfoWithRemap(pmp, m_phmuldoubleWidth, phmuldoubleNew, phmulcr, fMustExist);

	// create a copy of the stats object
	CStatistics *pstatsCopy = GPOS_NEW(pmp) CStatistics
											(
											pmp,
											phmulhistNew,
											phmuldoubleNew,
											m_dRows,
											FEmpty(),
											m_ulNumPredicates
											);

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the the upper bound source cardinality information maintained in the input
	// statistics object.

	// copy the upper bound ndv information
	const ULONG ulLen = m_pdrgpubndvs->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const CUpperBoundNDVs *pubndv = (*m_pdrgpubndvs)[ul];
 	 	CUpperBoundNDVs *pubndvCopy = pubndv->PubndvCopyWithRemap(pmp, phmulcr);

		if (NULL != pubndvCopy)
	 	{
			pstatsCopy->AddCardUpperBound(pubndvCopy);
	 	}
	}

	return pstatsCopy;
}

//	return the column identifiers of all columns whose statistics are
//	maintained by the statistics object
DrgPul *
CStatistics::PdrgulColIds
	(
	IMemoryPool *pmp
	)
	const
{
	DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);

	HMIterUlHist hmiterulhist(m_phmulhist);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());
		pdrgpul->Append(GPOS_NEW(pmp) ULONG(ulColId));
	}

	return pdrgpul;
}

// return the set of column references we have statistics for
CColRefSet *
CStatistics::Pcrs
	(
	IMemoryPool *pmp
	)
	const
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	HMIterUlHist hmiterulhist(m_phmulhist);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());
		CColRef *pcr = pcf->PcrLookup(ulColId);
		GPOS_ASSERT(NULL != pcr);

		pcrs->Include(pcr);
	}

	return pcrs;
}

//	append given histograms to current object where the column ids have been re-mapped
void
CStatistics::AddHistogramsWithRemap
	(
	IMemoryPool *pmp,
	HMUlHist *phmulhistSrc,
	HMUlHist *phmulhistDest,
	HMUlCr *phmulcr,
	BOOL
#ifdef GPOS_DEBUG
	fMustExist
#endif //GPOS_DEBUG
	)
{
	HMIterUlCr hmiterulcr(phmulcr);
	while (hmiterulcr.FAdvance())
	{
		ULONG ulColIdSrc = *(hmiterulcr.Pk());
		const CColRef *pcrDest = hmiterulcr.Pt();
		GPOS_ASSERT_IMP(fMustExist, NULL != pcrDest);

		ULONG ulColIdDest = pcrDest->UlId();

		const CHistogram *phistSrc = phmulhistSrc->PtLookup(&ulColIdSrc);
		if (NULL != phistSrc)
		{
			CStatisticsUtils::AddHistogram(pmp, ulColIdDest, phistSrc, phmulhistDest);
		}
	}
}

// add width information where the column ids have been re-mapped
void
CStatistics::AddWidthInfoWithRemap
	(
	IMemoryPool *pmp,
	HMUlDouble *phmuldoubleSrc,
	HMUlDouble *phmuldoubleDest,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	HMIterUlDouble hmiteruldouble(phmuldoubleSrc);
	while (hmiteruldouble.FAdvance())
	{
		ULONG ulColId = *(hmiteruldouble.Pk());
		CColRef *pcrNew = phmulcr->PtLookup(&ulColId);
		if (fMustExist && NULL == pcrNew)
		{
			continue;
		}

		if (NULL != pcrNew)
		{
			ulColId = pcrNew->UlId();
		}

		if (NULL == phmuldoubleDest->PtLookup(&ulColId))
		{
			const CDouble *pdWidth = hmiteruldouble.Pt();
			CDouble *pdWidthCopy = GPOS_NEW(pmp) CDouble(*pdWidth);
#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmuldoubleDest->FInsert(GPOS_NEW(pmp) ULONG(ulColId), pdWidthCopy);
			GPOS_ASSERT(fResult);
		}
	}
}

//	for the output statistics object, compute its upper bound cardinality
// 	mapping based on the bounding method estimated output cardinality
//  and information maintained in the current statistics object
void
CStatistics::ComputeCardUpperBounds
	(
	IMemoryPool *pmp,
	CStatistics *pstatsOutput, // output statistics object that is to be updated
	CDouble dRowsOutput, // estimated output cardinality of the operator
	CStatistics::ECardBoundingMethod ecbm // technique used to estimate max source cardinality in the output stats object
	)
	const
{
	GPOS_ASSERT(NULL != pstatsOutput);
	GPOS_ASSERT(CStatistics::EcbmSentinel != ecbm);

	ULONG ulLen = m_pdrgpubndvs->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const CUpperBoundNDVs *pubndv = (*m_pdrgpubndvs)[ul];
		CDouble dUpperBoundNDVInput = pubndv->DUpperBoundNDVs();
		CDouble dUpperBoundNDVOutput = dRowsOutput;
		if (CStatistics::EcbmInputSourceMaxCard == ecbm)
		{
			dUpperBoundNDVOutput = dUpperBoundNDVInput;
		}
		else if (CStatistics::EcbmMin == ecbm)
		{
			dUpperBoundNDVOutput = std::min(dUpperBoundNDVInput.DVal(), dRowsOutput.DVal());
		}

		CUpperBoundNDVs *pubndvCopy = pubndv->PubndvCopy(pmp, dUpperBoundNDVOutput);
		pstatsOutput->AddCardUpperBound(pubndvCopy);
	}
}

// return the index of the array of upper bound ndvs to which column reference belongs
ULONG
CStatistics::UlIndexUpperBoundNDVs
(
	const CColRef *pcr
)
{
	GPOS_ASSERT(NULL != pcr);
 	CAutoMutex am(m_mutexCardUpperBoundAccess);
 	am.Lock();

 	const ULONG ulLen = m_pdrgpubndvs->UlLength();
 	for (ULONG ul = 0; ul < ulLen; ul++)
 	{
 		const CUpperBoundNDVs *pubndv = (*m_pdrgpubndvs)[ul];
 	 	if (pubndv->FPresent(pcr))
 	 	{
 	 		return ul;
 	 	}
	}

	return ULONG_MAX;
}

// add upper bound of source cardinality
void
CStatistics::AddCardUpperBound
	(
	CUpperBoundNDVs *pubndv
	)
{
	GPOS_ASSERT(NULL != pubndv);

	CAutoMutex am(m_mutexCardUpperBoundAccess);
	am.Lock();

	m_pdrgpubndvs->Append(pubndv);
}

// return the dxl representation of the statistics object
CDXLStatsDerivedRelation *
CStatistics::Pdxlstatsderrel
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda 
	)
	const
{
	DrgPdxlstatsdercol *pdrgpdxlstatsdercol = GPOS_NEW(pmp) DrgPdxlstatsdercol(pmp);
	
	HMIterUlHist hmiterulhist(m_phmulhist);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());
		const CHistogram *phist = hmiterulhist.Pt();

		CDouble *pdWidth = m_phmuldoubleWidth->PtLookup(&ulColId);
		GPOS_ASSERT(pdWidth);

		CDXLStatsDerivedColumn *pdxlstatsdercol = phist->Pdxlstatsdercol(pmp, pmda, ulColId, *pdWidth);
		pdrgpdxlstatsdercol->Append(pdxlstatsdercol);
	}
	
	return GPOS_NEW(pmp) CDXLStatsDerivedRelation(m_dRows, FEmpty(), pdrgpdxlstatsdercol);
}

// return the upper bound of ndvs for a column reference
CDouble
CStatistics::DUpperBoundNDVs
	(
	const CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pcr);

	CAutoMutex am(m_mutexCardUpperBoundAccess);
	am.Lock();

	const ULONG ulLen = m_pdrgpubndvs->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		const CUpperBoundNDVs *pubndv = (*m_pdrgpubndvs)[ul];
		if (pubndv->FPresent(pcr))
		{
			return pubndv->DUpperBoundNDVs();
		}
	}

	return DDefaultDistinctValues;
}


// look up the number of distinct values of a particular column
CDouble
CStatistics::DNDV
	(
	const CColRef *pcr
	)
{
	ULONG ulColId = pcr->UlId();
	CHistogram *phistCol = m_phmulhist->PtLookup(&ulColId);
	if (NULL != phistCol)
	{
		return std::min(phistCol->DDistinct(), DUpperBoundNDVs(pcr));
	}

#ifdef GPOS_DEBUG
	{
		// the case of no histogram available for requested column signals
		// something wrong with computation of required statistics columns,
		// we print a debug message to log this case

		CAutoMemoryPool amp;
		CAutoTrace at(amp.Pmp());

		at.Os() << "\nREQUESTED NDVs FOR COL (" << pcr->UlId()  << ") WITH A MISSING HISTOGRAM";
	}
#endif //GPOS_DEBUG

	// if no histogram is available for required column, we use
	// the number of rows as NDVs estimate
	return std::min(m_dRows, DUpperBoundNDVs(pcr));
}


// EOF
