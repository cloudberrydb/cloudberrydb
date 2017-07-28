//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CKeyCollection.cpp
//
//	@doc:
//		Implementation of key collections
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_pdrgpcrs(NULL)
{
	GPOS_ASSERT(NULL != pmp);
	
	m_pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection
	(
	IMemoryPool *pmp,
	CColRefSet *pcrs
	)
	:
	m_pmp(pmp),
	m_pdrgpcrs(NULL)
{
	GPOS_ASSERT(NULL != pcrs && 0 < pcrs->CElements());
	
	m_pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);

	// we own the set
	Add(pcrs);
}
	

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::CKeyCollection
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CKeyCollection::CKeyCollection
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr
	)
	:
	m_pmp(pmp),
	m_pdrgpcrs(NULL)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpcr && 0 < pdrgpcr->UlLength());
	
	m_pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(pdrgpcr);
	Add(pcrs);

	// we own the array
	pdrgpcr->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::~CKeyCollection
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CKeyCollection::~CKeyCollection()
{
	m_pdrgpcrs->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::Add
//
//	@doc:
//		Add key set to collection; takes ownership
//
//---------------------------------------------------------------------------
void
CKeyCollection::Add
	(
	CColRefSet *pcrs
	)
{
	GPOS_ASSERT(!FKey(pcrs) && "no duplicates allowed");
	
	m_pdrgpcrs->Append(pcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::FKey
//
//	@doc:
//		Check if set constitutes key
//
//---------------------------------------------------------------------------
BOOL
CKeyCollection::FKey
	(
	const CColRefSet *pcrs,
	BOOL fExactMatch // true: match keys exactly,
					//  false: match keys by inclusion
	)
	const
{
	const ULONG ulSets = m_pdrgpcrs->UlLength();
	for (ULONG ul = 0; ul < ulSets; ul++)
	{
		if (fExactMatch)
		{
			// accept only exact matches
			if (pcrs->FEqual((*m_pdrgpcrs)[ul]))
			{
				return true;
			}
		}
		else
		{
			// if given column set includes a key, then it is also a key
			if (pcrs->FSubset((*m_pdrgpcrs)[ul]))
			{
				return true;
			}
		}
	}
	
	return false;
}



//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::FKey
//
//	@doc:
//		Check if array constitutes key
//
//---------------------------------------------------------------------------
BOOL
CKeyCollection::FKey
	(
	IMemoryPool *pmp,
	const DrgPcr *pdrgpcr
	)
	const
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(pdrgpcr);
	
	BOOL fKey = FKey(pcrs);
	pcrs->Release();
	
	return fKey;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrTrim
//
//	@doc:
//		Return first subsumed key as column array
//
//---------------------------------------------------------------------------
DrgPcr *
CKeyCollection::PdrgpcrTrim
	(
	IMemoryPool *pmp,
	const DrgPcr *pdrgpcr
	)
	const
{
	DrgPcr *pdrgpcrTrim = NULL;
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(pdrgpcr);

	const ULONG ulSets = m_pdrgpcrs->UlLength();
	for(ULONG ul = 0; ul < ulSets; ul++)
	{
		CColRefSet *pcrsKey = (*m_pdrgpcrs)[ul];
		if (pcrs->FSubset(pcrsKey))
		{
			pdrgpcrTrim = pcrsKey->Pdrgpcr(pmp);
			break;
		}
	}
	pcrs->Release();

	return pdrgpcrTrim;
}	

//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrKey
//
//	@doc:
//		Extract a key
//
//---------------------------------------------------------------------------
DrgPcr *
CKeyCollection::PdrgpcrKey
	(
	IMemoryPool *pmp
	)
	const
{
	if (0 == m_pdrgpcrs->UlLength())
	{
		return NULL;
	}

	GPOS_ASSERT(NULL != (*m_pdrgpcrs)[0]);

	DrgPcr *pdrgpcr = (*m_pdrgpcrs)[0]->Pdrgpcr(pmp);
	return pdrgpcr;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrHashableKey
//
//	@doc:
//		Extract a hashable key
//
//---------------------------------------------------------------------------
DrgPcr *
CKeyCollection::PdrgpcrHashableKey
	(
	IMemoryPool *pmp
	)
	const
{
	const ULONG ulSets = m_pdrgpcrs->UlLength();
	for(ULONG ul = 0; ul < ulSets; ul++)
	{
		DrgPcr *pdrgpcrKey = (*m_pdrgpcrs)[ul]->Pdrgpcr(pmp);
		if (CUtils::FHashable(pdrgpcrKey))
		{
			return pdrgpcrKey;
		}
		pdrgpcrKey->Release();
	}

	// no hashable key is found
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PdrgpcrKey
//
//	@doc:
//		Extract the key at a position
//
//---------------------------------------------------------------------------
DrgPcr *
CKeyCollection::PdrgpcrKey
	(
	IMemoryPool *pmp,
	ULONG ulIndex
	)
	const
{
	if (0 == m_pdrgpcrs->UlLength())
	{
		return NULL;
	}
	
	GPOS_ASSERT(NULL != (*m_pdrgpcrs)[ulIndex]);
	
	DrgPcr *pdrgpcr = (*m_pdrgpcrs)[ulIndex]->Pdrgpcr(pmp);
	return pdrgpcr;
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::PcrsKey
//
//	@doc:
//		Extract key at given position
//
//---------------------------------------------------------------------------
CColRefSet *
CKeyCollection::PcrsKey
	(
	IMemoryPool *pmp,
	ULONG ulIndex
	)
	const
{
	if (0 == m_pdrgpcrs->UlLength())
	{
		return NULL;
	}

	GPOS_ASSERT(NULL != (*m_pdrgpcrs)[ulIndex]);

	CColRefSet *pcrsKey = (*m_pdrgpcrs)[ulIndex];
	return GPOS_NEW(pmp) CColRefSet(pmp, *pcrsKey);
}


//---------------------------------------------------------------------------
//	@function:
//		CKeyCollection::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CKeyCollection::OsPrint
	(
	IOstream &os
	)
	const
{
	os << " Keys: (";

	const ULONG ulSets = m_pdrgpcrs->UlLength();
	for(ULONG ul = 0; ul < ulSets; ul++)
	{
		if (0 < ul)
		{
			os << ", ";
		}

		GPOS_ASSERT(NULL != (*m_pdrgpcrs)[ul]);
		os << "[" << (*(*m_pdrgpcrs)[ul]) << "]";
	}
	
	return os << ")";
}


// EOF

