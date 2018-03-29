//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolTracker.cpp
//
//	@doc:
//		Implementation for memory pool that allocates from an underlying
//		allocator and adds synchronization, statistics, debugging information
//		and memory tracing.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/CMemoryPoolTracker.h"
#include "gpos/memory/IMemoryVisitor.h"
#include "gpos/sync/CAutoMutex.h"

using namespace gpos;


#define GPOS_MEM_ALLOC_HEADER_SIZE \
	GPOS_MEM_ALIGNED_STRUCT_SIZE(SAllocHeader)

#define GPOS_MEM_BYTES_TOTAL(ulNumBytes) \
	(GPOS_MEM_ALLOC_HEADER_SIZE + GPOS_MEM_ALIGNED_SIZE(ulNumBytes))


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::CMemoryPoolTracker
//
//	@doc:
//		Ctor.
//
//---------------------------------------------------------------------------
CMemoryPoolTracker::CMemoryPoolTracker
	(
	IMemoryPool *pmpUnderlying,
	ULLONG ullSizeMax,
	BOOL fThreadSafe,
	BOOL fOwnsUnderlying
	)
	:
	CMemoryPool(pmpUnderlying, fOwnsUnderlying, fThreadSafe),
	m_ulAllocSequence(0),
	m_ullCapacity(ullSizeMax),
	m_ullReserved(0)
{
	GPOS_ASSERT(NULL != pmpUnderlying);

	m_listAllocations.Init(GPOS_OFFSET(SAllocHeader, m_link));
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::~CMemoryPoolTracker
//
//	@doc:
//		Dtor.
//
//---------------------------------------------------------------------------
CMemoryPoolTracker::~CMemoryPoolTracker()
{
	GPOS_ASSERT(m_listAllocations.FEmpty());
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::PvAllocate
//
//	@doc:
//		Allocate memory.
//
//---------------------------------------------------------------------------
void *
CMemoryPoolTracker::PvAllocate
	(
	const ULONG ulBytes,
	const CHAR *szFile,
	const ULONG ulLine
	)
{
	GPOS_ASSERT(GPOS_MEM_ALLOC_MAX >= ulBytes);

	CAutoSpinlock as(m_slock);

	ULONG ulAlloc = GPOS_MEM_BYTES_TOTAL(ulBytes);
	const BOOL fAvailableMem = FReserve(as, ulAlloc);

	// allocate from underlying
	void *pvAlloc;
	if (fAvailableMem)
	{
		pvAlloc = PmpUnderlying()->PvAllocate(ulAlloc, szFile, ulLine);
	}
	else
	{
		pvAlloc = NULL;
	}

	// check if allocation failed
	if (NULL == pvAlloc)
	{
		Unreserve(as, ulAlloc, fAvailableMem);

		return NULL;
	}

	// successful allocation: update header information and any memory pool data
	SAllocHeader *pahHeader = static_cast<SAllocHeader*>(pvAlloc);

	// scope indicating locking
	{
		SLock(as);

		m_mps.RecordAllocation(ulBytes, ulAlloc);
		m_listAllocations.Prepend(pahHeader);
		pahHeader->m_ullSerial = m_ulAllocSequence;
		++m_ulAllocSequence;

		SUnlock(as);
	}

	pahHeader->m_szFilename = szFile;
	pahHeader->m_ulLine = ulLine;
	pahHeader->m_ulSize = ulBytes;

	void *pvResult = pahHeader + 1;

#ifdef GPOS_DEBUG
	pahHeader->m_sd.BackTrace();

	clib::PvMemSet(pvResult, GPOS_MEM_INIT_PATTERN_CHAR, ulBytes);
#endif // GPOS_DEBUG

	return pvResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::FReserve
//
//	@doc:
//		Attempt to reserve memory for allocation
//
//---------------------------------------------------------------------------
BOOL
CMemoryPoolTracker::FReserve
	(
	CAutoSpinlock &as,
	ULONG ulAlloc
	)
{
	BOOL fAvailableMem = false;

	if (gpos::ullong_max == m_ullCapacity)
	{
		fAvailableMem = true;
	}
	else
	{
		SLock(as);

		if (ulAlloc + m_ullReserved <= m_ullCapacity)
		{
			m_ullReserved += ulAlloc;
			fAvailableMem = true;
		}

		SUnlock(as);
	}

	return fAvailableMem;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::Unreserve
//
//	@doc:
//		Revert memory reservation
//
//---------------------------------------------------------------------------
void
CMemoryPoolTracker::Unreserve
	(
	CAutoSpinlock &as,
	ULONG ulAlloc,
	BOOL fAvailableMem
	)
{
	SLock(as);

	// return reserved memory
	if (fAvailableMem)
	{
		m_ullReserved -= ulAlloc;
	}

	m_mps.RecordFailedAllocation();

	SUnlock(as);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::Free
//
//	@doc:
//		Free memory.
//
//---------------------------------------------------------------------------
void
CMemoryPoolTracker::Free
	(
	void *pv
	)
{
	CAutoSpinlock as(m_slock);

	SAllocHeader *pah = static_cast<SAllocHeader*>(pv) - 1;
	ULONG ulUserSize = pah->m_ulSize;

#ifdef GPOS_DEBUG
	// mark user memory as unused in debug mode
	clib::PvMemSet(pv, GPOS_MEM_INIT_PATTERN_CHAR, ulUserSize);
#endif // GPOS_DEBUG

	ULONG ulTotalSize = GPOS_MEM_BYTES_TOTAL(ulUserSize);

	// scope indicating locking
	{
		SLock(as);

		// update stats and allocation list
		m_mps.RecordFree(ulUserSize, ulTotalSize);
		m_listAllocations.Remove(pah);

		SUnlock(as);
	}

	// pass request to underlying memory pool;
	PmpUnderlying()->Free(pah);

	// update committed memory value
	if (m_ullCapacity != gpos::ullong_max)
	{
		SLock(as);

		m_ullReserved -= ulTotalSize;

		SUnlock(as);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::TearDown
//
//	@doc:
//		Prepare the memory pool to be deleted;
// 		this function is called only once so locking is not required;
//
//---------------------------------------------------------------------------
void
CMemoryPoolTracker::TearDown()
{
	while (!m_listAllocations.FEmpty())
	{
		SAllocHeader *pah = m_listAllocations.PtFirst();
		void *pvUserData = pah + 1;
		Free(pvUserData);
	}

	CMemoryPool::TearDown();
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::WalkLiveObjects
//
//	@doc:
//		Walk live objects.
//
//---------------------------------------------------------------------------
void
CMemoryPoolTracker::WalkLiveObjects
	(
	gpos::IMemoryVisitor *pmov
	)
{
	GPOS_ASSERT(NULL != pmov);

	SAllocHeader *pahHeader = m_listAllocations.PtFirst();
	while (NULL != pahHeader)
	{
		SIZE_T ulTotalSize = GPOS_MEM_BYTES_TOTAL(pahHeader->m_ulSize);
		void *pvUser = pahHeader + 1;

		pmov->Visit
			(
			pvUser,
			pahHeader->m_ulSize,
			pahHeader,
			ulTotalSize,
			pahHeader->m_szFilename,
			pahHeader->m_ulLine,
			pahHeader->m_ullSerial,
#ifdef GPOS_DEBUG
			&pahHeader->m_sd
#else
			NULL
#endif // GPOS_DEBUG
			);

		pahHeader = m_listAllocations.PtNext(pahHeader);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::UpdateStatistics
//
//	@doc:
//		Update statistics.
//
//---------------------------------------------------------------------------
void
CMemoryPoolTracker::UpdateStatistics
	(
	CMemoryPoolStatistics &mps
	)
{
	CAutoSpinlock as(m_slock);
	SLock(as);

	mps = m_mps;
}


#endif // GPOS_DEBUG

// EOF

