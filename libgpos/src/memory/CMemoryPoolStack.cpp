//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolStack.cpp
//
//	@doc:
//		Implementation of memory pool that allocates large blocks from the
//		underlying pool and incrementally reserves space in them.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/memory/CMemoryPoolStack.h"
#include "gpos/memory/CMemoryPoolManager.h"


#define GPOS_MEM_BLOCK_SIZE (1024 * 1024)

#define GPOS_MEM_BLOCK_HEADER_SIZE \
	(GPOS_MEM_ALIGNED_STRUCT_SIZE(SBlockDescriptor))


using namespace gpos;

GPOS_CPL_ASSERT(MAX_ALIGNED(GPOS_MEM_BLOCK_SIZE));


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::CMemoryPoolStack
//
//	@doc:
//	  ctor
//
//---------------------------------------------------------------------------
CMemoryPoolStack::CMemoryPoolStack
	(
	IMemoryPool *pmp,
	ULLONG ullCapacity,
	BOOL fThreadSafe,
	BOOL fOwnsUnderlying
	)
	:
	CMemoryPool(pmp, fOwnsUnderlying, fThreadSafe),
	m_pbd(NULL),
	m_ullReserved(0),
	m_ullCapacity(ullCapacity),
	m_ulBlockSize(GPOS_MEM_ALIGNED_SIZE(GPOS_MEM_BLOCK_SIZE))
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(GPOS_MEM_BLOCK_SIZE < m_ullCapacity);

	m_listBlocks.Init(GPOS_OFFSET(SBlockDescriptor, m_link));
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::~CMemoryPoolStack
//
//	@doc:
//		Dtor.
//
//---------------------------------------------------------------------------
CMemoryPoolStack::~CMemoryPoolStack()
{
	GPOS_ASSERT(m_listBlocks.FEmpty());
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::PvAllocate
//
//	@doc:
//		Allocate memory, either from the underlying pool directly (for large
//		requests) or by advancing the index in the current block.
//
//---------------------------------------------------------------------------
void *
CMemoryPoolStack::PvAllocate
	(
	ULONG ulBytes,
	const CHAR *,  // szFile
	const ULONG    // ulLine
	)
{
	GPOS_ASSERT(GPOS_MEM_ALLOC_MAX >= ulBytes);

	ULONG ulAlloc = GPOS_MEM_ALIGNED_SIZE(ulBytes);
	GPOS_ASSERT(MAX_ALIGNED(ulAlloc));

	CAutoSpinlock as(m_slock);

	// check if memory pool has enough capacity
	if (ulAlloc + m_ullReserved > m_ullCapacity)
	{
		return NULL;
	}

	// find block to allocate memory in it
	SBlockDescriptor *pbd = PbdProvider(as, ulAlloc);

	GPOS_ASSERT_IMP(FThreadSafe(), m_slock.FOwned());

	if (NULL != pbd)
	{
		// reserve memory
		m_ullReserved += ulAlloc;

		void *pvAlloc = GPOS_MEM_OFFSET_POS(pbd, pbd->m_ulUsed);
		pbd->m_ulUsed += ulAlloc;

		return pvAlloc;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::PbdProvider
//
//	@doc:
//		Find block to provide memory for allocation request.
//
//---------------------------------------------------------------------------
CMemoryPoolStack::SBlockDescriptor *
CMemoryPoolStack::PbdProvider
	(
	CAutoSpinlock &as,
	ULONG ulAlloc
	)
{
	SLock(as);

	SBlockDescriptor *pbd = m_pbd;

	if (NULL == pbd || !pbd->FFit(ulAlloc))
	{
		// release spinlock to allocate memory from underlying pool
		SUnlock(as);

		// allocate a new block
		pbd = PbdNew(ulAlloc);
		SLock(as);

		if (NULL == pbd)
		{
			return NULL;
		}

		// keep track of new block
		m_listBlocks.Append(pbd);

		if (pbd != NULL && m_ulBlockSize == pbd->m_ulTotal)
		{
			m_pbd = pbd;
		}
	}

	return pbd;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::PbdNew
//
//	@doc:
//		Allocate block from underlying pool.
//
//---------------------------------------------------------------------------
CMemoryPoolStack::SBlockDescriptor *
CMemoryPoolStack::PbdNew
	(
	ULONG ulSize
	)
{
	ULONG ulBlockSize = std::max(m_ulBlockSize, ulSize + (ULONG) GPOS_MEM_BLOCK_HEADER_SIZE);
	GPOS_ASSERT(MAX_ALIGNED(ulBlockSize));

	// allocate memory and put block descriptor to the beginning of it
	SBlockDescriptor *pbd = static_cast<SBlockDescriptor*>
			(
			PmpUnderlying()->PvAllocate(ulBlockSize, __FILE__, __LINE__)
			);

	if (NULL != pbd)
	{
		pbd->Init(ulBlockSize);
	}

	return pbd;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::TearDown
//
//	@doc:
//		Return all used memory to the underlying pool and tear it down.
//
//---------------------------------------------------------------------------
void
CMemoryPoolStack::TearDown()
{
	GPOS_ASSERT(!m_slock.FOwned());

	while (!m_listBlocks.FEmpty())
	{
		PmpUnderlying()->Free(m_listBlocks.RemoveHead());
	}

	CMemoryPool::TearDown();

	m_ullReserved = 0;
	m_pbd = NULL;
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::CheckAllocation
//
//	@doc:
//		Verifies that an allocation is correct and came from this pool.
//
//---------------------------------------------------------------------------
void
CMemoryPoolStack::CheckAllocation
	(
	void *pv
	)
	const
{
	SBlockDescriptor *pbd = m_listBlocks.PtFirst();
	while (NULL != pbd)
	{
		if (pv >= pbd->m_pvUser)
		{
			if (pv < GPOS_MEM_OFFSET_POS(pbd, pbd->m_ulUsed))
			{
				return;
			}
		}

		pbd = m_listBlocks.PtNext(pbd);
	}

	GPOS_ASSERT(!"object is allocated in one of the blocks");
}

#endif // GPOS_DEBUG

// EOF

