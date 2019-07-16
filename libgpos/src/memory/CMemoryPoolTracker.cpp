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
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/CMemoryPoolTracker.h"
#include "gpos/memory/IMemoryVisitor.h"

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
	CMemoryPool *underlying_memory_pool,
	ULLONG max_size,
	BOOL thread_safe,
	BOOL owns_underlying_memory_pool
	)
	:
	CMemoryPool(underlying_memory_pool, owns_underlying_memory_pool, thread_safe),
	m_alloc_sequence(0),
	m_capacity(max_size),
	m_reserved(0)
{
	GPOS_ASSERT(NULL != underlying_memory_pool);

	m_allocations_list.Init(GPOS_OFFSET(SAllocHeader, m_link));
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
	GPOS_ASSERT(m_allocations_list.IsEmpty());
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::Allocate
//
//	@doc:
//		Allocate memory.
//
//---------------------------------------------------------------------------
void *
CMemoryPoolTracker::Allocate
	(
	const ULONG bytes,
	const CHAR *file,
	const ULONG line
	)
{
	GPOS_ASSERT(GPOS_MEM_ALLOC_MAX >= bytes);

	ULONG alloc = GPOS_MEM_BYTES_TOTAL(bytes);
	const BOOL mem_available = Reserve(alloc);

	// allocate from underlying
	void *ptr;
	if (mem_available)
	{
		ptr = GetUnderlyingMemoryPool()->Allocate(alloc, file, line);
	}
	else
	{
		ptr = NULL;
	}

	// check if allocation failed
	if (NULL == ptr)
	{
		Unreserve(alloc, mem_available);

		return NULL;
	}

	// successful allocation: update header information and any memory pool data
	SAllocHeader *header = static_cast<SAllocHeader*>(ptr);

	m_memory_pool_statistics.RecordAllocation(bytes, alloc);
	m_allocations_list.Prepend(header);
	header->m_serial = m_alloc_sequence;
	++m_alloc_sequence;

	header->m_filename = file;
	header->m_line = line;
	header->m_size = bytes;

	void *ptr_result = header + 1;

#ifdef GPOS_DEBUG
	header->m_stack_desc.BackTrace();

	clib::Memset(ptr_result, GPOS_MEM_INIT_PATTERN_CHAR, bytes);
#endif // GPOS_DEBUG

	return ptr_result;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTracker::Reserve
//
//	@doc:
//		Attempt to reserve memory for allocation
//
//---------------------------------------------------------------------------
BOOL
CMemoryPoolTracker::Reserve
	(
	ULONG alloc
	)
{
	BOOL mem_available = false;

	if (gpos::ullong_max == m_capacity)
	{
		mem_available = true;
	}
	else
	{
		if (alloc + m_reserved <= m_capacity)
		{
			m_reserved += alloc;
			mem_available = true;
		}
	}

	return mem_available;
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
	ULONG alloc,
	BOOL mem_available
	)
{
	// return reserved memory
	if (mem_available)
	{
		m_reserved -= alloc;
	}

	m_memory_pool_statistics.RecordFailedAllocation();
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
	void *ptr
	)
{
	SAllocHeader *header = static_cast<SAllocHeader*>(ptr) - 1;
	ULONG user_size = header->m_size;

#ifdef GPOS_DEBUG
	// mark user memory as unused in debug mode
	clib::Memset(ptr, GPOS_MEM_INIT_PATTERN_CHAR, user_size);
#endif // GPOS_DEBUG

	ULONG total_size = GPOS_MEM_BYTES_TOTAL(user_size);

	// update stats and allocation list
	m_memory_pool_statistics.RecordFree(user_size, total_size);
	m_allocations_list.Remove(header);

	// pass request to underlying memory pool;
	GetUnderlyingMemoryPool()->Free(header);

	// update committed memory value
	if (m_capacity != gpos::ullong_max)
	{
		m_reserved -= total_size;
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
	while (!m_allocations_list.IsEmpty())
	{
		SAllocHeader *header = m_allocations_list.First();
		void *user_data = header + 1;
		Free(user_data);
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
	gpos::IMemoryVisitor *visitor
	)
{
	GPOS_ASSERT(NULL != visitor);

	SAllocHeader *header = m_allocations_list.First();
	while (NULL != header)
	{
		SIZE_T total_size = GPOS_MEM_BYTES_TOTAL(header->m_size);
		void *user = header + 1;

		visitor->Visit
			(
			user,
			header->m_size,
			header,
			total_size,
			header->m_filename,
			header->m_line,
			header->m_serial,
#ifdef GPOS_DEBUG
			&header->m_stack_desc
#else
			NULL
#endif // GPOS_DEBUG
			);

		header = m_allocations_list.Next(header);
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
	CMemoryPoolStatistics &memory_pool_statistics
	)
{
	memory_pool_statistics = m_memory_pool_statistics;
}


#endif // GPOS_DEBUG

// EOF

