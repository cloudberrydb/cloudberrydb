//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSlab.cpp
//
//	@doc:
//		Implementation of memory slab
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CSlab.h"
#include "gpos/memory/CSlabCache.h"
#include "gpos/sync/atomic.h"

#define GPOS_MEM_SLAB_REACTIVATE_PERCENT	(50)

using namespace gpos;

// initialization of static variables
ULONG_PTR CSlab::m_ulpIdInvalid(0);


//---------------------------------------------------------------------------
//	@function:
//		CSlab::CSlab
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSlab::CSlab
	(
	CSlabCache *pscOwner,
	ULONG ulChunks,
	ULONG ulChunkSize,
	CSpinlockBase &slock
	)
	:
	m_ulpId((ULONG_PTR) this),
	m_ulSlots(ulChunks),
	m_ulSlotSize(ulChunkSize),
	m_ulSlotsFree(ulChunks),
	m_fActive(true),
	m_pshstackFree(NULL),
	m_pscOwner(pscOwner),
	m_slock(slock)
{
	GPOS_ASSERT(NULL != pscOwner);
	GPOS_ASSERT(MAX_ALIGNED(this));
	GPOS_ASSERT(MAX_ALIGNED(m_ulSlotSize));
	GPOS_ASSERT(0 < ulChunks);
	GPOS_ASSERT(0 < ulChunkSize);

	// initialize slots
	for (ULONG ul = 0; ul < m_ulSlots; ul++)
	{
		SSlotHeader &slot(GetSlot(ul));
		slot.m_pslabOwner = this;
		slot.ulpAllocType = EatSlabAlloc;
		slot.m_pshNext = NULL;
		Push(&slot);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSlab::PvAllocate
//
//	@doc:
//		Reserve slot
//
//---------------------------------------------------------------------------
void *
CSlab::PvAllocate
	(
	BOOL fNewSlab
	)
{
	SSlotHeader *psh = PshPop();
	GPOS_ASSERT(MAX_ALIGNED(psh));

	// check if all slots in slab are used
	if (NULL == psh)
	{
		GPOS_ASSERT(0 == m_ulSlotsFree);
		return NULL;
	}

	GPOS_ASSERT(0 < m_ulSlotsFree);
	if (!fNewSlab && m_ulSlotsFree == m_ulSlots)
	{
		// slab was not used before;
		// decrement unused slab counter in cache;
		m_pscOwner->DecrUnused();
	}
	m_ulSlotsFree--;

	// return position after slot header
	return psh + 1;
}


//---------------------------------------------------------------------------
//	@function:
//		CSlab::Free
//
//	@doc:
//		Recycle slot
//
//---------------------------------------------------------------------------
void
CSlab::Free
	(
	void *pvObj
	)
{
	GPOS_ASSERT(!m_slock.FOwned());

	// slot header precedes user data
	SSlotHeader *psh = static_cast<SSlotHeader*>(pvObj) - 1;
	GPOS_ASSERT(EatSlabAlloc == psh->ulpAllocType);

	CAutoSpinlock as(m_slock);
	m_pscOwner->SLock(as);

	// recycle slot
	Push(psh);
	m_ulSlotsFree++;

	// check if the slab was inactive and can now be re-activated
	if (!m_fActive &&
	    m_ulSlotsFree > (m_ulSlots * GPOS_MEM_SLAB_REACTIVATE_PERCENT) / 100)
	{
		// set this to active
		m_fActive = true;
		m_pscOwner->Add(psh->m_pslabOwner);
	}

	// check if this was the last used slot in the slab
	if (m_ulSlotsFree == m_ulSlots)
	{
		// increment unused slab counter in cache
		m_pscOwner->IncUnused(as);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSlab::GetSlot
//
//	@doc:
//		Get slot by offset
//
//---------------------------------------------------------------------------
CSlab::SSlotHeader &
CSlab::GetSlot
	(
	ULONG ulOffset
	)
{
	BYTE *pbSlotPosition =
			reinterpret_cast<BYTE*>(this + 1) +
			ulOffset * (GPOS_MEM_ALIGNED_SIZE(GPOS_SIZEOF(SSlotHeader)) + m_ulSlotSize);

	SSlotHeader &slot = *(reinterpret_cast<SSlotHeader*>(pbSlotPosition));

	GPOS_ASSERT(MAX_ALIGNED(&slot));

	return slot;
}


//---------------------------------------------------------------------------
//	@function:
//		CSlab::UlSize
//
//	@doc:
//		Calculate slab size
//
//---------------------------------------------------------------------------
ULONG
CSlab::UlSize
	(
	ULONG ulSlots,
	ULONG ulSlotSize
	)
{
	GPOS_ASSERT(MAX_ALIGNED(ulSlotSize));

	const ULONG ulHeader = GPOS_MEM_ALIGNED_SIZE(GPOS_SIZEOF(CSlab));
	const ULONG ulTotalSlotSize =
			ulSlots * (GPOS_MEM_ALIGNED_SIZE(GPOS_SIZEOF(SSlotHeader)) + ulSlotSize);

	const ULONG ulTotalSize = ulHeader + ulTotalSlotSize;

	GPOS_ASSERT(MAX_ALIGNED(ulTotalSize));

	return ulTotalSize;
}


//---------------------------------------------------------------------------
//	@function:
//		CSlab::PslabConvert
//
//	@doc:
//		Get slab cache from slot
//
//---------------------------------------------------------------------------
CSlab *
CSlab::PslabConvert(void *pv)
{
	// slot header precedes user data
	SSlotHeader *psh = static_cast<SSlotHeader*>(pv) - 1;
	CSlab *pslab = psh->m_pslabOwner;
	GPOS_ASSERT
		(
		pv >= (pslab + 1) &&
		pv < pslab + UlSize(pslab->m_ulSlots, pslab->m_ulSlotSize)
		);

	return pslab;
}


// EOF
