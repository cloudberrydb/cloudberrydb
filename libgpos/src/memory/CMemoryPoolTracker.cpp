//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolTracker.cpp
//
//	@doc:
//		Implementation for memory pool that allocates from Malloc
//		and adds synchronization, statistics, debugging information
//		and memory tracing.
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
	)
	:
	CMemoryPool(),
	m_alloc_sequence(0)
{
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

	void *ptr;
	ptr = clib::Malloc(alloc);

	// check if allocation failed
	if (NULL == ptr)
	{
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
	clib::Memset(ptr, GPOS_MEM_FREED_PATTERN_CHAR, user_size);
#endif // GPOS_DEBUG

	ULONG total_size = GPOS_MEM_BYTES_TOTAL(user_size);

	// update stats and allocation list
	m_memory_pool_statistics.RecordFree(user_size, total_size);
	m_allocations_list.Remove(header);

	clib::Free(header);
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


#endif // GPOS_DEBUG

// EOF

