//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CStatistics.h
//
//	@doc:
//		Statistics implementation over 1D histograms
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatistics_H
#define GPNAUCRATES_CStatistics_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/sync/CMutex.h"

#include "naucrates/statistics/IStatistics.h"
#include "naucrates/statistics/CStatsPredDisj.h"
#include "naucrates/statistics/CStatsPredConj.h"
#include "naucrates/statistics/CStatsPredLike.h"
#include "naucrates/statistics/CStatsPredUnsupported.h"
#include "naucrates/statistics/CUpperBoundNDVs.h"

#include "naucrates/statistics/CHistogram.h"
#include "gpos/common/CBitSet.h"

namespace gpopt
{
	class CStatisticsConfig;
	class CColumnFactory;
}

namespace gpnaucrates
{
	using namespace gpos;
	using namespace gpdxl;
	using namespace gpmd;
	using namespace gpopt;

	// hash maps ULONG -> array of ULONGs
	typedef CHashMap<ULONG, DrgPul, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<DrgPul> > HMUlPdrgpul;

	// iterator
	typedef CHashMapIter<ULONG, DrgPul, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
		CleanupDelete<ULONG>, CleanupRelease<DrgPul> > HMIterUlPdrgpul;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatistics
	//
	//	@doc:
	//		Abstract statistics API
	//---------------------------------------------------------------------------
	class CStatistics: public IStatistics
	{
		public:
				// method used to compute for columns of each source it corresponding
				// the cardinality upper bound
				enum ECardBoundingMethod
				{
					EcbmOutputCard = 0, // use the estimated output cardinality as the upper bound cardinality of the source
					EcbmInputSourceMaxCard, // use the upper bound cardinality of the source in the input statistics object
					EcbmMin, // use the minimum of the above two cardinality estimates

					EcbmSentinel
				};

		// helper method to copy stats on columns that are not excluded by bitset
		void AddNotExcludedHistograms(IMemoryPool *pmp, CBitSet *pbsExcludedColIds, HMUlHist *phmulhist) const;

		private:

			// private copy ctor
			CStatistics(const CStatistics &);

			// private assignment operator
			CStatistics& operator=(CStatistics &);

			// hashmap from column ids to histograms
			HMUlHist *m_phmulhist;

			// hashmap from column id to width
			HMUlDouble *m_phmuldoubleWidth;

			// number of rows
			CDouble m_dRows;

			// the risk to have errors in cardinality estimation; it goes from 1 to infinity,
			// where 1 is no risk
			// when going from the leaves to the root of the plan, operators that generate joins,
			// selections and groups increment the risk
			ULONG m_ulStatsEstimationRisk;

			// flag to indicate if input relation is empty
			BOOL m_fEmpty;

			// statistics could be computed using predicates with external parameters (outer references),
			// this is the total number of external parameters' values
			CDouble m_dRebinds;

			// number of predicates applied
			ULONG m_ulNumPredicates;

			// statistics configuration
			CStatisticsConfig *m_pstatsconf;

			// array of upper bound of ndv per source;
			// source can be one of the following operators: like Get, Group By, and Project
			DrgPubndvs *m_pdrgpubndvs;

			// mutex for locking entry when accessing hashmap from source id -> upper bound of source cardinality
			CMutex m_mutexCardUpperBoundAccess;

			// the default value for operators that have no cardinality estimation risk
			static
			const ULONG ulStatsEstimationNoRisk;

			// helper method to add histograms where the column ids have been remapped
			static
			void AddHistogramsWithRemap(IMemoryPool *pmp, HMUlHist *phmulhistSrc, HMUlHist *phmulhistDest, HMUlCr *phmulcr, BOOL fMustExist);

			// helper method to add width information where the column ids have been remapped
			static
			void AddWidthInfoWithRemap(IMemoryPool *pmp, HMUlDouble *phmuldoubleSrc, HMUlDouble *phmuldoubleDest, HMUlCr *phmulcr, BOOL fMustExist);

		public:

			// ctor
			CStatistics
				(
				IMemoryPool *pmp,
				HMUlHist *phmulhist,
				HMUlDouble *phmuldoubleWidth,
				CDouble dRows,
				BOOL fEmpty,
				ULONG ulNumPredicates = 0
				);

			// dtor
			virtual
			~CStatistics();

			virtual
			HMUlDouble *CopyWidths(IMemoryPool *pmp) const;

			virtual
			void CopyWidthsInto(IMemoryPool *pmp, HMUlDouble *phmuldouble) const;

			virtual
			HMUlHist *CopyHistograms(IMemoryPool *pmp) const;

			// actual number of rows
			virtual
			CDouble DRows() const;

			// number of rebinds
			virtual
			CDouble DRebinds() const
			{
				return m_dRebinds;
			}

			// skew estimate for given column
			virtual
			CDouble DSkew(ULONG ulColId) const;

			// what is the width in bytes of set of column id's
			virtual
			CDouble DWidth(DrgPul *pdrgpulColIds) const;

			// what is the width in bytes of set of column references
			virtual
			CDouble DWidth(IMemoryPool *pmp, CColRefSet *pcrs) const;

			// what is the width in bytes
			virtual
			CDouble DWidth() const;

			// is statistics on an empty input
			virtual
			BOOL FEmpty() const
			{
				return m_fEmpty;
			}

			// look up the histogram of a particular column
			virtual
			const CHistogram *Phist
								(
								ULONG ulColId
								)
								const
			{
				return m_phmulhist->PtLookup(&ulColId);
			}

			// look up the number of distinct values of a particular column
			virtual
			CDouble DNDV(const CColRef *pcr);

			// look up the width of a particular column
			virtual
			const CDouble *PdWidth(ULONG ulColId) const;

			// the risk of errors in cardinality estimation
			virtual
			ULONG UlStatsEstimationRisk() const
			{
				return m_ulStatsEstimationRisk;
			}

			// update the risk of errors in cardinality estimation
			virtual
			void SetStatsEstimationRisk
				(
				ULONG ulRisk
				)
			{
				m_ulStatsEstimationRisk = ulRisk;
			}

			// inner join with another stats structure
			virtual
			CStatistics *PstatsInnerJoin(IMemoryPool *pmp, const IStatistics *pistatsOther, DrgPstatspredjoin *pdrgpstatspredjoin) const;

			// LOJ with another stats structure
			virtual
			CStatistics *PstatsLOJ(IMemoryPool *pmp, const IStatistics *pistatsOther, DrgPstatspredjoin *pdrgpstatspredjoin) const;

			// left anti semi join with another stats structure
			virtual
			CStatistics *PstatsLASJoin
							(
							IMemoryPool *pmp,
							const IStatistics *pstatsOther,
							DrgPstatspredjoin *pdrgpstatspredjoin,
							BOOL fIgnoreLasjHistComputation // except for the case of LOJ cardinality estimation this flag is always
                                                            // "true" since LASJ stats computation is very aggressive
							) const;

			// semi join stats computation
			virtual
			CStatistics *PstatsLSJoin(IMemoryPool *pmp, const IStatistics *pstatsInner, DrgPstatspredjoin *pdrgpstatspredjoin) const;

			// return required props associated with stats object
			virtual
			CReqdPropRelational *Prprel(IMemoryPool *pmp) const;

			// append given stats to current object
			virtual
			void AppendStats(IMemoryPool *pmp, IStatistics *pstats);

			// set number of rebinds
			virtual
			void SetRebinds
				(
				CDouble dRebinds
				)
			{
				GPOS_ASSERT(0.0 < dRebinds);

				m_dRebinds = dRebinds;
			}

			// copy stats
			virtual
			IStatistics *PstatsCopy(IMemoryPool *pmp) const;

			// return a copy of this stats object scaled by a given factor
			virtual
			IStatistics *PstatsScale(IMemoryPool *pmp, CDouble dFactor) const;

			// copy stats with remapped column id
			virtual
			IStatistics *PstatsCopyWithRemap(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist) const;

			// return the set of column references we have stats for
			virtual
			CColRefSet *Pcrs(IMemoryPool *pmp) const;

			// generate the DXL representation of the statistics object
			virtual
			CDXLStatsDerivedRelation *Pdxlstatsderrel(IMemoryPool *pmp, CMDAccessor *pmda) const;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const;

			// add upper bound of source cardinality
			virtual
			void AddCardUpperBound(CUpperBoundNDVs *pubndv);

			// return the upper bound of the number of distinct values for a given column
			virtual
			CDouble DUpperBoundNDVs(const CColRef *pcr);

			// return the index of the array of upper bound ndvs to which column reference belongs
			virtual
			ULONG UlIndexUpperBoundNDVs(const CColRef *pcr);

			// return the column identifiers of all columns statistics maintained
			virtual
			DrgPul *PdrgulColIds(IMemoryPool *pmp) const;

			virtual
			ULONG
			UlNumberOfPredicates() const
			{
				return m_ulNumPredicates;
			}

			CStatisticsConfig *PStatsConf() const
			{
				return	m_pstatsconf;
			}

			DrgPubndvs *Pdrgundv() const
			{
				return m_pdrgpubndvs;
			}
			// create an empty statistics object
			static
			CStatistics *PstatsEmpty
				(
				IMemoryPool *pmp
				)
			{
				DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);
				CStatistics *pstats = PstatsDummy(pmp, pdrgpul, DDefaultRelationRows);

				// clean up
				pdrgpul->Release();

				return pstats;
			}

			// conversion function
			static
			CStatistics *PstatsConvert
				(
				IStatistics *pistats
				)
			{
				GPOS_ASSERT(NULL != pistats);
				return dynamic_cast<CStatistics *> (pistats);
			}

			// create a dummy statistics object
			static
			CStatistics *PstatsDummy(IMemoryPool *pmp, DrgPul *pdrgpulColIds, CDouble dRows);

			// create a dummy statistics object
			static
			CStatistics *PstatsDummy
				(
				IMemoryPool *pmp,
				DrgPul *pdrgpulHistColIds,
				DrgPul *pdrgpulWidthColIds,
				CDouble dRows
				);

			// default column width
			static
			const CDouble DDefaultColumnWidth;

			// default number of rows in relation
			static
			const CDouble DDefaultRelationRows;

			// minimum number of rows in relation
			static
			const CDouble DMinRows;

			// epsilon
			static
			const CDouble DEpsilon;

			// default number of distinct values
			static
			const CDouble DDefaultDistinctValues;

			// check if the input statistics from join statistics computation empty
			static
			BOOL FEmptyJoinInput(const CStatistics *pstatsOuter, const CStatistics *pstatsInner, BOOL fLASJ);

			// add upper bound ndvs information for a given set of columns
			static
			void CreateAndInsertUpperBoundNDVs(IMemoryPool *pmp, CStatistics *pstats, DrgPul *pdrgpulColIds, CDouble dRows);

			// cap the total number of distinct values (NDV) in buckets to the number of rows
			static
			void CapNDVs(CDouble dRows, HMUlHist *phmulhist);
	}; // class CStatistics

}

#endif // !GPNAUCRATES_CStatistics_H

// EOF
