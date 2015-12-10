//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSlabCache.cpp
//
//	@doc:
//		Implementation of memory slab cache
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CSlabCache.h"
#include "gpos/sync/atomic.h"
#include "gpos/task/CAutoSuspendAbort.h"

#define GPOS_MEM_SLAB_CACHE_HT_SIZE   (128)

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::CSlabCache
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSlabCache::CSlabCache
	(
	IMemoryPool *pmp,
	ULONG ulChunkSize,
	ULONG ulChunks,
	ULONG ulThresholdHigh,
	ULONG ulThresholdLow,
	volatile ULONG_PTR *pulpReserved,
	ULLONG ullCapacity,
	BOOL fThreadSafe
	)
	:
	m_pmp(pmp),
	m_pslablistActive(NULL),
	m_pslablistAllocated(NULL),
	m_ulSlotSize(ulChunkSize),
	m_ulSlots(ulChunks),
	m_ulThresholdHigh(ulThresholdHigh),
	m_ulThresholdLow(ulThresholdLow),
	m_ulUnused(0),
	m_pulpReserved(pulpReserved),
	m_ullCapacity(ullCapacity),
	m_fThreadSafe(fThreadSafe),
	m_fTornDown(false)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pulpReserved);
	GPOS_ASSERT(0 < ulChunkSize);
	GPOS_ASSERT(0 < ulChunks);
	GPOS_ASSERT(0 < ulThresholdLow);
	GPOS_ASSERT(ulThresholdLow < ulThresholdHigh);

	// guard against OOM during initialization
	CAutoP<SlabList> a_pslablistActive;
	CAutoP<SlabList> a_pslablistAllocated;

	a_pslablistActive = GPOS_NEW(m_pmp) SlabList();
	a_pslablistAllocated = GPOS_NEW(m_pmp) SlabList();

	a_pslablistActive.Pt()->Init(GPOS_OFFSET(CSlab, m_linkActive));
	a_pslablistAllocated.Pt()->Init(GPOS_OFFSET(CSlab, m_linkAllocated));

	m_pslablistActive = a_pslablistActive.PtReset();
	m_pslablistAllocated = a_pslablistAllocated.PtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::~CSlabCache
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSlabCache::~CSlabCache()
{
	Teardown();
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::PvAllocate
//
//	@doc:
//		Allocate object
//
//---------------------------------------------------------------------------
void *
CSlabCache::PvAllocate()
{
	GPOS_ASSERT(NULL != m_pmp && "cache has not been initialized");
	GPOS_ASSERT(!m_slock.FOwned());

	CAutoSpinlock as(m_slock);
	SLock(as);

	void *pv = PvAllocateSlot();
	if (NULL != pv)
	{
		return pv;
	}

	GPOS_ASSERT(m_pslablistActive->FEmpty());

	// release spinlock to allocate memory for new slab
	SUnlock(as);

	// reserve memory for new slab
	if (!FReserveMem())
	{
		return NULL;
	}

	CSlab *pslab = PslabCreate();
	if (NULL == pslab)
	{
		// return reserved memory
		UnreserveMem();

		return NULL;
	}

	SLock(as);
	m_pslablistAllocated->Append(pslab);
	m_pslablistActive->Prepend(pslab);

	void *pvAlloc = pslab->PvAllocate(true /*fNewSlab*/);
	GPOS_ASSERT(NULL != pvAlloc);

	return pvAlloc;
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::PvAllocateSlot
//
//	@doc:
//		Attempt to allocate slot in active slabs
//
//---------------------------------------------------------------------------
void *
CSlabCache::PvAllocateSlot()
{
	GPOS_ASSERT_IMP(m_fThreadSafe, m_slock.FOwned());

	CSlab *pslab = m_pslablistActive->PtFirst();
	while (NULL != pslab)
	{
		// check if the slab has an available slot
		void *pvAlloc = pslab->PvAllocate(false /*fNewSlab*/);
		if (NULL != pvAlloc)
		{
			return pvAlloc;
		}

		// pop slab from cache's list
		CSlab *pslabPopped = m_pslablistActive->RemoveHead();

		GPOS_ASSERT(pslabPopped == pslab);
		GPOS_ASSERT(pslabPopped->FFull());
		pslabPopped->SetInactive();

		pslab = m_pslablistActive->PtFirst();
	}

	GPOS_ASSERT(m_pslablistActive->FEmpty());

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::Teardown
//
//	@doc:
//		Release all slabs
//
//---------------------------------------------------------------------------
void
CSlabCache::Teardown()
{
	GPOS_ASSERT(!m_slock.FOwned());

	if (!m_fTornDown)
	{
		// release all slabs
		while (!m_pslablistAllocated->FEmpty())
		{
			CSlab *pslab = m_pslablistAllocated->RemoveHead();
			Release(pslab);
		}

		// release slab lists
		GPOS_DELETE(m_pslablistAllocated);
		GPOS_DELETE(m_pslablistActive);

		m_fTornDown = true;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::PslabCreate
//
//	@doc:
//		Initialize slab
//
//---------------------------------------------------------------------------
CSlab *
CSlabCache::PslabCreate()
{
	GPOS_ASSERT(NULL != m_pmp && "cache has not been initialized");
	GPOS_ASSERT(!m_slock.FOwned());

	// allocate memory for slab
	void *pvSlabAlloc = PvAllocateSlab();
	if (NULL == pvSlabAlloc)
	{
		return NULL;
	}

	// create new slab using placement new
	return new(pvSlabAlloc) CSlab(this, m_ulSlots, m_ulSlotSize, m_slock);
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::Release
//
//	@doc:
//		Release slab
//
//---------------------------------------------------------------------------
void
CSlabCache::Release
	(
	CSlab *pslab
	)
{
	GPOS_ASSERT(NULL != m_pmp && "cache has not been initialized");
	GPOS_ASSERT(!m_slock.FOwned());

	m_pmp->Free(pslab);

	UnreserveMem();
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::Shrink
//
//	@doc:
//		Release unused slabs
//
//---------------------------------------------------------------------------
void
CSlabCache::Shrink
	(
	CAutoSpinlock &as
	)
{
	GPOS_ASSERT(NULL != m_pmp && "cache has not been initialized");
	GPOS_ASSERT(m_ulThresholdHigh < m_pslablistActive->UlSize());
	GPOS_ASSERT_IMP(m_fThreadSafe, m_slock.FOwned());

	// store empty slabs to separate list;
	// we need to release the spinlock before returning slabs
	// to underlying memory pool;
	SlabList slablistEmpty;
	slablistEmpty.Init(GPOS_OFFSET(CSlab, m_linkActive));

	CSlab *pslab = m_pslablistActive->PtFirst();
	while (NULL != pslab)
	{
		// release unused slabs until their number drops below lower threshold
		if (pslab->FUnused() && m_ulThresholdLow <= m_ulUnused)
		{
			DecrUnused();
			CSlab *pslabRemove = pslab;
			pslab = m_pslablistActive->PtNext(pslab);

			m_pslablistActive->Remove(pslabRemove);
			m_pslablistAllocated->Remove(pslabRemove);
			slablistEmpty.Append(pslabRemove);
		}
		else
		{
			pslab = m_pslablistActive->PtNext(pslab);
		}
	}

	// release spinlock to free slabs
	SUnlock(as);

	while (slablistEmpty.FEmpty())
	{
		Release(slablistEmpty.RemoveHead());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::FReserveMem
//
//	@doc:
//		Reserve memory for new slab
//
//---------------------------------------------------------------------------
BOOL
CSlabCache::FReserveMem()
{
	// check if capacity has been set
	if (ULLONG_MAX == m_ullCapacity)
	{
		return true;
	}

	ULONG ulAlloc = UlSlabSize();
	if (*m_pulpReserved + ulAlloc <= m_ullCapacity)
	{
		ULONG_PTR ulpOld = UlpExchangeAdd(m_pulpReserved, ulAlloc);
		if (ulpOld + ulAlloc <= m_ullCapacity)
		{
			return true;
		}

		UnreserveMem();
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::UnreserveMem
//
//	@doc:
//		Update reserved memory with release request
//
//---------------------------------------------------------------------------
void
CSlabCache::UnreserveMem()
{
	if (ULLONG_MAX > m_ullCapacity)
	{
		GPOS_ASSERT(ULLONG_MAX > m_ullCapacity);

		ULONG ulAlloc = UlSlabSize();
		(void) UlpExchangeAdd(m_pulpReserved, -ulAlloc);
	}
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CSlabCache::FOwns
//
//	@doc:
//		Check if slab belongs to cache
//
//---------------------------------------------------------------------------
BOOL
CSlabCache::FOwns
	(
	CSlab *pslab
	)
{
	CAutoSpinlock as(m_slock);
	SLock(as);

	return (GPOS_OK == m_pslablistAllocated->EresFind(pslab));
}

#endif // GPOS_DEBUG


// EOF
