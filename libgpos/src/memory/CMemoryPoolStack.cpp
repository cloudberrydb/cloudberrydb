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
	CMemoryPool *mp,
	ULLONG capacity,
	BOOL thread_safe,
	BOOL owns_underlying_memory_pool
	)
	:
	CMemoryPool(mp, owns_underlying_memory_pool, thread_safe),
	m_block_descriptor(NULL),
	m_reserved(0),
	m_capacity(capacity),
	m_blocksize(GPOS_MEM_ALIGNED_SIZE(GPOS_MEM_BLOCK_SIZE))
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(GPOS_MEM_BLOCK_SIZE < m_capacity);

	m_block_list.Init(GPOS_OFFSET(SBlockDescriptor, m_link));
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
	GPOS_ASSERT(m_block_list.IsEmpty());
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::Allocate
//
//	@doc:
//		Allocate memory, either from the underlying pool directly (for large
//		requests) or by advancing the index in the current block.
//
//---------------------------------------------------------------------------
void *
CMemoryPoolStack::Allocate
	(
	ULONG bytes,
	const CHAR *,  // szFile
	const ULONG    // line
	)
{
	GPOS_ASSERT(GPOS_MEM_ALLOC_MAX >= bytes);

	ULONG alloc = GPOS_MEM_ALIGNED_SIZE(bytes);
	GPOS_ASSERT(MAX_ALIGNED(alloc));

	// check if memory pool has enough capacity
	if (alloc + m_reserved > m_capacity)
	{
		return NULL;
	}

	// find block to allocate memory in it
	SBlockDescriptor *desc = FindMemoryBlock(alloc);

	if (NULL != desc)
	{
		// reserve memory
		m_reserved += alloc;

		void *ptr = GPOS_MEM_OFFSET_POS(desc, desc->m_used_size);
		desc->m_used_size += alloc;

		return ptr;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::FindMemoryBlock
//
//	@doc:
//		Find block to provide memory for allocation request.
//
//---------------------------------------------------------------------------
CMemoryPoolStack::SBlockDescriptor *
CMemoryPoolStack::FindMemoryBlock
	(
	ULONG alloc
	)
{
	SBlockDescriptor *desc = m_block_descriptor;

	if (NULL == desc || !desc->CanFit(alloc))
	{
		// allocate a new block
		desc = New(alloc);

		if (NULL == desc)
		{
			return NULL;
		}

		// keep track of new block
		m_block_list.Append(desc);

		if (desc != NULL && m_blocksize == desc->m_total_size)
		{
			m_block_descriptor = desc;
		}
	}

	return desc;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolStack::New
//
//	@doc:
//		Allocate block from underlying pool.
//
//---------------------------------------------------------------------------
CMemoryPoolStack::SBlockDescriptor *
CMemoryPoolStack::New
	(
	ULONG size
	)
{
	ULONG block_size = std::max(m_blocksize, size + (ULONG) GPOS_MEM_BLOCK_HEADER_SIZE);
	GPOS_ASSERT(MAX_ALIGNED(block_size));

	// allocate memory and put block descriptor to the beginning of it
	SBlockDescriptor *desc = static_cast<SBlockDescriptor*>
			(
			GetUnderlyingMemoryPool()->Allocate(block_size, __FILE__, __LINE__)
			);

	if (NULL != desc)
	{
		desc->Init(block_size);
	}

	return desc;
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
	while (!m_block_list.IsEmpty())
	{
		GetUnderlyingMemoryPool()->Free(m_block_list.RemoveHead());
	}

	CMemoryPool::TearDown();

	m_reserved = 0;
	m_block_descriptor = NULL;
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
	void *ptr
	)
	const
{
	SBlockDescriptor *desc = m_block_list.First();
	while (NULL != desc)
	{
		if (ptr >= desc->m_user)
		{
			if (ptr < GPOS_MEM_OFFSET_POS(desc, desc->m_used_size))
			{
				return;
			}
		}

		desc = m_block_list.Next(desc);
	}

	GPOS_ASSERT(!"object is allocated in one of the blocks");
}

#endif // GPOS_DEBUG

// EOF

