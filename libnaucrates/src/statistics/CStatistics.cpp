//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
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
#include "naucrates/statistics/CScaleFactorUtils.h"

#include "naucrates/statistics/CJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftOuterJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CInnerJoinStatsProcessor.h"
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

// minimum number of rows in relation
const CDouble CStatistics::DMinRows(1.0);

// default column width
const CDouble CStatistics::DDefaultColumnWidth(8.0);

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
	CHistogram::AddDummyHistogramAndWidthInfo(pmp, pcf, phmulhist, phmuldoubleWidth, pdrgpulColIds, fEmpty);

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

// Currently, Pstats[Join type] are thin wrappers the C[Join type]StatsProcessor class's method
// for deriving the stat objects for the corresponding join operator

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
	return CLeftOuterJoinStatsProcessor::PstatsLOJStatic(pmp, this, pstatsOther, pdrgpstatspredjoin);
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
	return CLeftSemiJoinStatsProcessor::PstatsLSJoinStatic(pmp, this, pstatsInner, pdrgpstatspredjoin);
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
	return CInnerJoinStatsProcessor::PstatsInnerJoinStatic(pmp, this, pistatsOther, pdrgpstatspredjoin);
}

// return statistics object after performing LASJ
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
	return CLeftAntiSemiJoinStatsProcessor::PstatsLASJoinStatic(pmp, this, pistatsOther, pdrgpstatspredjoin, fIgnoreLasjHistComputation);
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

HMUlDouble *
CStatistics::CopyWidths
	(
	IMemoryPool *pmp
	)
	const
{
	HMUlDouble *phmuldoubleCoopy = GPOS_NEW(pmp) HMUlDouble(pmp);
	CStatisticsUtils::AddWidthInfo(pmp, m_phmuldoubleWidth, phmuldoubleCoopy);

	return phmuldoubleCoopy;
}

void
CStatistics::CopyWidthsInto
	(
	IMemoryPool *pmp,
	HMUlDouble *phmuldouble
	)
	const
{
	CStatisticsUtils::AddWidthInfo(pmp, m_phmuldoubleWidth, phmuldouble);
}

HMUlHist *
CStatistics::CopyHistograms
	(
	IMemoryPool *pmp
	)
	const
{
	// create hash map from colid -> histogram for resultant structure
	HMUlHist *phmulhistCopy = GPOS_NEW(pmp) HMUlHist(pmp);

	BOOL fEmpty = FEmpty();

	HMIterUlHist hmiterulhist(m_phmulhist);
	while (hmiterulhist.FAdvance())
	{
		ULONG ulColId = *(hmiterulhist.Pk());
		const CHistogram *phist = hmiterulhist.Pt();
		CHistogram *phistCopy = NULL;
		if (fEmpty)
		{
			phistCopy =  GPOS_NEW(pmp) CHistogram(GPOS_NEW(pmp) DrgPbucket(pmp), false /* fWellDefined */);
		}
		else
		{
			phistCopy = phist->PhistCopy(pmp);
		}

		phmulhistCopy->FInsert(GPOS_NEW(pmp) ULONG(ulColId), phistCopy);
	}

	return phmulhistCopy;
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

	CHistogram::AddHistograms(pmp, pstats->m_phmulhist, m_phmulhist);
	GPOS_CHECK_ABORT;

	CStatisticsUtils::AddWidthInfo(pmp, pstats->m_phmuldoubleWidth, m_phmuldoubleWidth);
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

	CHistogram::AddHistograms(pmp, m_phmulhist, phmulhistNew);
	GPOS_CHECK_ABORT;

	CStatisticsUtils::AddWidthInfo(pmp, m_phmuldoubleWidth, phmuldoubleNew);
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
	CStatisticsUtils::ComputeCardUpperBounds(pmp, this, pstatsScaled, dRowsScaled, CStatistics::EcbmMin /* ecbm */);

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

	return gpos::ulong_max;
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
