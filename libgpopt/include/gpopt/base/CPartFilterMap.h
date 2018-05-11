//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartFilter
//
//	@doc:
//		Partitioned table filter map used in required and derived properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CPartFilterMap_H
#define GPOPT_CPartFilterMap_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CHashMap.h"
#include "gpopt/base/CDrvdProp.h"


namespace gpnaucrates
{
	// forward declarations
	class IStatistics;
}

using gpnaucrates::IStatistics;

namespace gpopt
{

	// forward declarations
	class CExpression;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPartFilterMap
	//
	//	@doc:
	//		Partitioned table filter map used in required and derived properties
	//
	//---------------------------------------------------------------------------
	class CPartFilterMap : public CRefCount
	{
		private:

			//-------------------------------------------------------------------
			//	@class:
			//		CPartFilter
			//
			//	@doc:
			//		Single entry of CPartFilterMap
			//
			//-------------------------------------------------------------------
			class CPartFilter : public CRefCount
			{
				private:

					// scan id
					ULONG m_ulScanId;

					// scalar expression
					CExpression *m_pexpr;

					// statistics of the plan below partition selector -- used only during plan property derivation
					IStatistics *m_pstats;

				public:

					// ctor
					CPartFilter
						(
						ULONG ulScanId,
						CExpression *pexpr,
						IStatistics *pstats = NULL
						);

					// dtor
					virtual
					~CPartFilter();

					// match function
					BOOL FMatch(const CPartFilter *ppf) const;

					// return scan id
					ULONG UlScanId() const
					{
						return m_ulScanId;
					}

					// return scalar expression
					CExpression *Pexpr() const
					{
						return m_pexpr;
					}

					// return statistics of the plan below partition selector
					IStatistics *Pstats() const
					{
						return m_pstats;
					}

					// print function
					IOstream &OsPrint(IOstream &os) const;

			}; // class CPartFilter

			// map of partition index ids to filter expressions
			typedef CHashMap<ULONG, CPartFilter, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
				CleanupDelete<ULONG>, CleanupRelease<CPartFilter> > HMULPartFilter;

			// map iterator
			typedef CHashMapIter<ULONG, CPartFilter, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
				CleanupDelete<ULONG>, CleanupRelease<CPartFilter> > HMULPartFilterIter;

			// hash map from ScanId to CPartFilter
			HMULPartFilter *m_phmulpf;

		public:

			// ctors
			explicit
			CPartFilterMap(IMemoryPool *pmp);

			CPartFilterMap(IMemoryPool *pmp, CPartFilterMap *ppfm);

			// dtor
			virtual
			~CPartFilterMap();

			// check whether map contains the given scan id
			BOOL FContainsScanId
				(
				ULONG ulScanId
				)
				const
			{
				return (NULL != m_phmulpf->PtLookup(&ulScanId));
			}

			// the expression associated with the given scan id
			CExpression *Pexpr(ULONG ulScanId) const;

			// stats associated with the given scan id
			IStatistics *Pstats(ULONG ulScanId) const;

			// check whether the map is empty
			BOOL FEmpty() const
			{
				return 0 == m_phmulpf->UlEntries();
			}

			// check whether current part filter map is a subset of the given one
			BOOL FSubset(CPartFilterMap *ppfm);

			// check equality of part filter maps
			BOOL FEqual
				(
				CPartFilterMap *ppfm
				)
			{
				GPOS_ASSERT(NULL != ppfm);

				return
					(m_phmulpf->UlEntries() == ppfm->m_phmulpf->UlEntries()) &&
					this->FSubset(ppfm);
			}

			// extract part Scan id's in the given memory pool
			DrgPul *PdrgpulScanIds(IMemoryPool *pmp) const;

			// add part filter to map
			void AddPartFilter
				(
				IMemoryPool *pmp,
				ULONG ulScanId,
				CExpression *pexpr,
				IStatistics *pstats
				);

			// look for given scan id in given map and, if found, copy the corresponding entry to current map
			BOOL FCopyPartFilter(IMemoryPool *pmp, ULONG ulScanId, CPartFilterMap *ppfmSource);

			// copy all part filters from source map to current map
			void CopyPartFilterMap(IMemoryPool *pmp, CPartFilterMap *ppfmSource);

			// print function
			IOstream &OsPrint(IOstream &os) const;

#ifdef GPOS_DEBUG
			// debug print for interactive debugging sessions only
			void DbgPrint() const;
#endif // GPOS_DEBUG

	}; // class CPartFilterMap

}

#endif // !GPOPT_CPartFilterMap_H

// EOF
