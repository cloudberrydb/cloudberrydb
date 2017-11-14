//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPartFilterMap.cpp
//
//	@doc:
//		Implementation of partitioned table filter map
//---------------------------------------------------------------------------

#include "naucrates/statistics/IStatistics.h"
#include "gpopt/base/CPartFilterMap.h"
#include "gpopt/operators/CExpression.h"

#ifdef GPOS_DEBUG
#include "gpopt/base/COptCtxt.h"
#include "gpos/error/CAutoTrace.h"
#endif // GPOS_DEBUG

using namespace gpos;
using namespace gpopt;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilter::CPartFilter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartFilterMap::CPartFilter::CPartFilter
	(
	ULONG ulScanId,
	CExpression *pexpr,
	IStatistics *pstats
	)
	:
	m_ulScanId(ulScanId),
	m_pexpr(pexpr),
	m_pstats(pstats)
{
	GPOS_ASSERT(NULL != pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilter::~CPartFilter
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartFilterMap::CPartFilter::~CPartFilter()
{
	m_pexpr->Release();
	CRefCount::SafeRelease(m_pstats);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilter::FMatch
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
BOOL
CPartFilterMap::CPartFilter::FMatch
	(
	const CPartFilter *ppf
	)
	const
{
	return NULL != ppf &&
			m_ulScanId == ppf->m_ulScanId &&
			m_pexpr->FMatch(ppf->m_pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilter::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CPartFilterMap::CPartFilter::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "[ " << m_ulScanId << " : "
		<< *m_pexpr << std::endl;
	if (NULL != m_pstats)
	{
		os << *m_pstats;
	}
	else
	{
		os << " (Empty Stats) ";
	}
	os << "]" << std::endl;

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilterMap
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartFilterMap::CPartFilterMap
	(
	IMemoryPool *pmp
	)
{
	m_phmulpf = GPOS_NEW(pmp) HMULPartFilter(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CPartFilterMap
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartFilterMap::CPartFilterMap
	(
	IMemoryPool *pmp,
	CPartFilterMap *ppfm
	)
{
	GPOS_ASSERT(NULL != ppfm);
	m_phmulpf = GPOS_NEW(pmp) HMULPartFilter(pmp);
	CopyPartFilterMap(pmp, ppfm);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::~CPartFilterMap
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartFilterMap::~CPartFilterMap()
{
	CRefCount::SafeRelease(m_phmulpf);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::FSubset
//
//	@doc:
//		Check if current part filter map is a subset of the given one
//
//---------------------------------------------------------------------------
BOOL
CPartFilterMap::FSubset
	(
	CPartFilterMap *ppfm
	)
{
	GPOS_ASSERT(NULL != ppfm);

	HMULPartFilterIter hmulpfi(m_phmulpf);
	while (hmulpfi.FAdvance())
	{
		const CPartFilter *ppfCurrent = hmulpfi.Pt();
		ULONG ulScanId = ppfCurrent->UlScanId();
		CPartFilter *ppfOther = ppfm->m_phmulpf->PtLookup(&ulScanId);
		if (!ppfCurrent->FMatch(ppfOther))
		{
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::Pexpr
//
//	@doc:
//		Return the expression associated with the given scan id
//
//---------------------------------------------------------------------------
CExpression *
CPartFilterMap::Pexpr
	(
	ULONG ulScanId
	)
	const
{
	CPartFilter *ppf = m_phmulpf->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppf);

	return ppf->Pexpr();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::Pstats
//
//	@doc:
//		Return the stats associated with the given scan id
//
//---------------------------------------------------------------------------
IStatistics *
CPartFilterMap::Pstats
	(
	ULONG ulScanId
	)
	const
{
	CPartFilter *ppf = m_phmulpf->PtLookup(&ulScanId);
	GPOS_ASSERT(NULL != ppf);

	return ppf->Pstats();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::PdrgpulScanIds
//
//	@doc:
//		 Extract Scan ids
//
//---------------------------------------------------------------------------
DrgPul *
CPartFilterMap::PdrgpulScanIds
	(
	IMemoryPool *pmp
	)
	const
{
	DrgPul *pdrgpul = GPOS_NEW(pmp) DrgPul(pmp);
	HMULPartFilterIter hmulpfi(m_phmulpf);
	while (hmulpfi.FAdvance())
	{
		CPartFilter *ppf = const_cast<CPartFilter *>(hmulpfi.Pt());

		pdrgpul->Append(GPOS_NEW(pmp) ULONG(ppf->UlScanId()));
	}

	return pdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::AddPartFilter
//
//	@doc:
//		Add part filter to map
//
//---------------------------------------------------------------------------
void
CPartFilterMap::AddPartFilter
	(
	IMemoryPool *pmp,
	ULONG ulScanId,
	CExpression *pexpr,
	IStatistics *pstats
	)
{
	GPOS_ASSERT(NULL != pexpr);
	CPartFilter *ppf = m_phmulpf->PtLookup(&ulScanId);
	if (NULL != ppf)
	{
		return;
	}

	ppf = GPOS_NEW(pmp) CPartFilter(ulScanId, pexpr, pstats);

#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif // GPOS_DEBUG
	m_phmulpf->FInsert(GPOS_NEW(pmp) ULONG(ulScanId), ppf);

	GPOS_ASSERT(fSuccess);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::FCopyPartFilter
//
//	@doc:
//		Look for given scan id in given map and, if found, copy the
//		corresponding entry to current map,
//		return true if copying done successfully
//
//---------------------------------------------------------------------------
BOOL
CPartFilterMap::FCopyPartFilter
	(
	IMemoryPool *pmp,
	ULONG ulScanId,
	CPartFilterMap *ppfmSource
	)
{
	GPOS_ASSERT(NULL != ppfmSource);
	GPOS_ASSERT(this != ppfmSource);

	CPartFilter *ppf = ppfmSource->m_phmulpf->PtLookup(&ulScanId);
	if (NULL != ppf)
	{
		ppf->AddRef();
		ULONG *pulScanId = GPOS_NEW(pmp) ULONG(ulScanId);
		BOOL fSuccess = m_phmulpf->FInsert(pulScanId, ppf);
		GPOS_ASSERT(fSuccess);

		if (!fSuccess)
		{
			ppf->Release();
			GPOS_DELETE(pulScanId);
		}

		return fSuccess;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::CopyPartFilterMap
//
//	@doc:
//		Copy all part filters from source map to current map
//
//---------------------------------------------------------------------------
void
CPartFilterMap::CopyPartFilterMap
	(
	IMemoryPool *pmp,
	CPartFilterMap *ppfmSource
	)
{
	GPOS_ASSERT(NULL != ppfmSource);
	GPOS_ASSERT(this != ppfmSource);

	HMULPartFilterIter hmulpfi(ppfmSource->m_phmulpf);
	while (hmulpfi.FAdvance())
	{
		CPartFilter *ppf = const_cast<CPartFilter *>(hmulpfi.Pt());
		ULONG ulScanId = ppf->UlScanId();

		// check if part filter with same scan id already exists
		if (NULL != m_phmulpf->PtLookup(&ulScanId))
		{
			continue;
		}

		ppf->AddRef();

#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif // GPOS_DEBUG
		m_phmulpf->FInsert(GPOS_NEW(pmp) ULONG(ulScanId), ppf);

		GPOS_ASSERT(fSuccess);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPartFilterMap::OsPrint
//
//	@doc:
//		Print filter map
//
//---------------------------------------------------------------------------
IOstream &
CPartFilterMap::OsPrint
	(
	IOstream &os
	)
	const
{
	HMULPartFilterIter hmulpfi(m_phmulpf);
	while (hmulpfi.FAdvance())
	{
		const CPartFilter *ppf = hmulpfi.Pt();
		ppf->OsPrint(os);
	}

	return os;
}


#ifdef GPOS_DEBUG
void
CPartFilterMap::DbgPrint() const
{
	IMemoryPool *pmp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(pmp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG

// EOF

