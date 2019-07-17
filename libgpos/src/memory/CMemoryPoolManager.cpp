//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//	@filename:
//		CWorkerPoolManager.cpp
//
//	@doc:
//		Central scheduler; 
//		* maintains global worker-local-storage
//		* keeps track of all worker pools
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/common/CAutoP.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/error/CFSimulator.h" // for GPOS_FPSIMULATOR
#include "gpos/error/CAutoTrace.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolAlloc.h"
#include "gpos/memory/CMemoryPoolInjectFault.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/CMemoryPoolStack.h"
#include "gpos/memory/CMemoryPoolTracker.h"
#include "gpos/memory/CMemoryVisitorPrint.h"
#include "gpos/task/CAutoSuspendAbort.h"


using namespace gpos;
using namespace gpos::clib;


// global instance of memory pool manager
CMemoryPoolManager *CMemoryPoolManager::m_memory_pool_mgr = NULL;

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::CMemoryPoolManager
//
//	@doc:
//		Ctor.
//
//---------------------------------------------------------------------------
CMemoryPoolManager::CMemoryPoolManager
	(
	CMemoryPool *internal,
	CMemoryPool *base
	)
	:
	m_base_memory_pool(base),
	m_internal_memory_pool(internal),
	m_global_memory_pool(NULL),
	m_allow_global_new(true)
{
	GPOS_ASSERT(NULL != internal);
	GPOS_ASSERT(NULL != base);
	GPOS_ASSERT(GPOS_OFFSET(CMemoryPool, m_link) == GPOS_OFFSET(CMemoryPoolAlloc, m_link));
	GPOS_ASSERT(GPOS_OFFSET(CMemoryPool, m_link) == GPOS_OFFSET(CMemoryPoolTracker, m_link));

	m_hash_table.Init
		(
		m_internal_memory_pool,
		GPOS_MEMORY_POOL_HT_SIZE,
		GPOS_OFFSET(CMemoryPool, m_link),
		GPOS_OFFSET(CMemoryPool, m_hash_key),
		&(CMemoryPool::m_invalid),
		HashULongPtr,
		EqualULongPtr
		);

	// create pool used in allocations made using global new operator
	m_global_memory_pool = Create(EatTracker, true, gpos::ullong_max);
}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Init
//
//	@doc:
//		Initializer for global memory pool manager
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolManager::Init
	(
		void* (*alloc) (SIZE_T),
		void (*free_func) (void*)
	)
{
	GPOS_ASSERT(NULL == CMemoryPoolManager::m_memory_pool_mgr);

	// raw allocation of memory for internal memory pools
	void *alloc_base = Malloc(sizeof(CMemoryPoolAlloc));
	void *alloc_internal = Malloc(sizeof(CMemoryPoolTracker));

	// check if any allocation failed
	if (NULL == alloc_internal ||
		NULL == alloc_base)
	{
		Free(alloc_base);
		Free(alloc_internal);

		return GPOS_OOM;
	}

	// create base memory pool
	CMemoryPool *base = new(alloc_base) CMemoryPoolAlloc(alloc, free_func);

	// create internal memory pool
	CMemoryPool *internal = new(alloc_internal) CMemoryPoolTracker
			(
			base,
			gpos::ullong_max, // ullMaxMemory
			true, // IsThreadSafe
			false //fOwnsUnderlyingPmp
			);

	// instantiate manager
	GPOS_TRY
	{
		CMemoryPoolManager::m_memory_pool_mgr = GPOS_NEW(internal) CMemoryPoolManager
				(
				internal,
				base
				);
	}
	GPOS_CATCH_EX(ex)
	{
		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			Free(alloc_base);
			Free(alloc_internal);

			return GPOS_OOM;
		}

		return GPOS_FAILED;
	}
	GPOS_CATCH_END;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Create
//
//	@doc:
//		Create new memory pool
//
//---------------------------------------------------------------------------
CMemoryPool *
CMemoryPoolManager::Create
	(
	AllocType alloc_type,
	BOOL thread_safe,
	ULLONG capacity
	)
{
	CMemoryPool *mp =
#ifdef GPOS_DEBUG
			CreatePoolStack(alloc_type, capacity, thread_safe);
#else
			New(alloc_type, m_base_memory_pool, capacity, thread_safe, false /*owns_underlying_memory_pool*/);
#endif // GPOS_DEBUG

	// accessor scope
	{
		// HERE BE DRAGONS
		// See comment in CCache::InsertEntry
		const ULONG_PTR hashKey = mp->GetHashKey();
		MemoryPoolKeyAccessor acc(m_hash_table, hashKey);
		acc.Insert(mp);
	}

	return mp;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::New
//
//	@doc:
//		Create new pool of given type
//
//---------------------------------------------------------------------------
CMemoryPool *
CMemoryPoolManager::New
	(
	AllocType alloc_type,
	CMemoryPool *underlying_memory_pool,
	ULLONG capacity,
	BOOL thread_safe,
	BOOL owns_underlying_memory_pool
	)
{
	switch (alloc_type)
	{
		case CMemoryPoolManager::EatTracker:
			return GPOS_NEW(m_internal_memory_pool) CMemoryPoolTracker
						(
						underlying_memory_pool,
						capacity,
						thread_safe,
						owns_underlying_memory_pool
						);

		case CMemoryPoolManager::EatStack:
			return GPOS_NEW(m_internal_memory_pool) CMemoryPoolStack
						(
						underlying_memory_pool,
						capacity,
						thread_safe,
						owns_underlying_memory_pool
						);
	}

	GPOS_ASSERT(!"No matching pool type found");
	return NULL;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::CreatePoolStack
//
//	@doc:
//		Surround new pool with tracker pools
//
//---------------------------------------------------------------------------
CMemoryPool *
CMemoryPoolManager::CreatePoolStack
	(
	AllocType alloc_type,
	ULLONG capacity,
	BOOL thread_safe
	)
{
	CMemoryPool *base = m_base_memory_pool;
	BOOL malloc_type = (EatTracker == alloc_type);

	// check if tracking and fault injection on internal allocations
	// of memory pools is enabled
	if (NULL != ITask::Self() && !malloc_type  && GPOS_FTRACE(EtraceTestMemoryPools))
	{
		// put fault injector on top of base pool
		CMemoryPool *FPSim_low = GPOS_NEW(m_internal_memory_pool) CMemoryPoolInjectFault
				(
				base,
				false /*owns_underlying_memory_pool*/
				);

		// put tracker on top of fault injector
		base = New
				(
				EatTracker,
				FPSim_low,
				capacity,
				thread_safe,
				true /*owns_underlying_memory_pool*/
				);
	}

	// tracker pool goes on top
	CMemoryPool *requested = base;
	if (!malloc_type)
	{
		// put requested pool on top of underlying pool
		requested = New
				(
				alloc_type,
				base,
				capacity,
				thread_safe,
				base != m_base_memory_pool
				);
	}

	// put fault injector on top of requested pool
	CMemoryPool *FPSim = GPOS_NEW(m_internal_memory_pool) CMemoryPoolInjectFault
				(
				requested,
				!malloc_type
				);

	// put tracker on top of the stack
	return New(EatTracker, FPSim, capacity, thread_safe, true /*fOwnsUnderlying*/);
}

#endif // GPOS_DEBUG



//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::DeleteUnregistered
//
//	@doc:
//		Release returned pool
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::DeleteUnregistered
	(
	CMemoryPool *mp
	)
{
	GPOS_ASSERT(mp != NULL);

#ifdef GPOS_DEBUG
	// accessor's scope
	{
		// HERE BE DRAGONS
		// See comment in CCache::InsertEntry
		ULONG_PTR hashKey = mp->GetHashKey();
		MemoryPoolKeyAccessor acc(m_hash_table, hashKey);

		// make sure that this pool is not in the hash table
		CMemoryPool *found = acc.Find();
		while (NULL != found)
		{
			GPOS_ASSERT(found != mp && "Attempt to delete a registered memory pool");

			found = acc.Next(found);
		}
	}
#endif // GPOS_DEBUG

	GPOS_DELETE(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Destroy
//
//	@doc:
//		Release returned pool
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::Destroy
	(
	CMemoryPool *mp
	)
{
	GPOS_ASSERT(NULL != mp);

	// accessor scope
	{
		// HERE BE DRAGONS
		// See comment in CCache::InsertEntry
		const ULONG_PTR hashKey = mp->GetHashKey();
		MemoryPoolKeyAccessor acc(m_hash_table, hashKey);
		acc.Remove(mp);
	}

	mp->TearDown();

	GPOS_DELETE(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::TotalAllocatedSize
//
//	@doc:
//		Return total allocated size in bytes
//
//---------------------------------------------------------------------------
ULLONG
CMemoryPoolManager::TotalAllocatedSize()
{
	ULLONG total_size = 0;
	MemoryPoolIter iter(m_hash_table);
	while (iter.Advance())
	{
		MemoryPoolIterAccessor acc(iter);
		CMemoryPool *mp = acc.Value();
		if (NULL != mp)
		{
			total_size = total_size + mp->TotalAllocatedSize();
		}
	}

	return total_size;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::OsPrint
//
//	@doc:
//		Print contents of all allocated memory pools
//
//---------------------------------------------------------------------------
IOstream &
CMemoryPoolManager::OsPrint
	(
	IOstream &os
	)
{
	os << "Print memory pools: " << std::endl;

	MemoryPoolIter iter(m_hash_table);
	while (iter.Advance())
	{
		CMemoryPool *mp = NULL;
		{
			MemoryPoolIterAccessor acc(iter);
			mp = acc.Value();
		}

		if (NULL != mp)
		{
			os << *mp << std::endl;
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::PrintOverSizedPools
//
//	@doc:
//		Print memory pools with total allocated size above given threshold
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::PrintOverSizedPools
	(
	CMemoryPool *trace,
	ULLONG size_threshold // size threshold in bytes
	)
{
	CAutoTraceFlag Abort(EtraceSimulateAbort, false);
	CAutoTraceFlag OOM(EtraceSimulateOOM, false);
	CAutoTraceFlag Net(EtraceSimulateNetError, false);
	CAutoTraceFlag IO(EtraceSimulateIOError, false);

	MemoryPoolIter iter(m_hash_table);
	while (iter.Advance())
	{
		MemoryPoolIterAccessor acc(iter);
		CMemoryPool *mp = acc.Value();

		if (NULL != mp)
		{
			ULLONG size = mp->TotalAllocatedSize();
			if (size > size_threshold)
			{
				CAutoTrace at(trace);
				at.Os() << std::endl << "OVERSIZED MEMORY POOL: " << size << " bytes " << std::endl;
			}
		}
	}
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::DestroyMemoryPoolAtShutdown
//
//	@doc:
//		Destroy a memory pool at shutdown
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::DestroyMemoryPoolAtShutdown
	(
	CMemoryPool *mp
	)
{
	GPOS_ASSERT(NULL != mp);

#ifdef GPOS_DEBUG
	gpos::oswcerr << "Leaked " << *mp << std::endl;
#endif // GPOS_DEBUG

	mp->TearDown();
	GPOS_DELETE(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Cleanup
//
//	@doc:
//		Clean-up memory pools
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::Cleanup()
{
#ifdef GPOS_DEBUG
	if (0 < m_global_memory_pool->TotalAllocatedSize())
	{
		// allocations made by calling global new operator are not deleted
		gpos::oswcerr << "Memory leaks detected"<< std::endl << *m_global_memory_pool << std::endl;
	}
#endif // GPOS_DEBUG

	GPOS_ASSERT(NULL != m_global_memory_pool);
	Destroy(m_global_memory_pool);

	// cleanup left-over memory pools;
	// any such pool means that we have a leak
	m_hash_table.DestroyEntries(DestroyMemoryPoolAtShutdown);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Shutdown
//
//	@doc:
//		Delete memory pools and release manager
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::Shutdown()
{
	// cleanup remaining memory pools
	Cleanup();

	// save off pointers for explicit deletion
	CMemoryPool *internal = m_internal_memory_pool;
	CMemoryPool *base = m_base_memory_pool;

	GPOS_DELETE(CMemoryPoolManager::m_memory_pool_mgr);
	CMemoryPoolManager::m_memory_pool_mgr = NULL;

#ifdef GPOS_DEBUG
	internal->AssertEmpty(oswcerr);
	base->AssertEmpty(oswcerr);
#endif // GPOS_DEBUG

	Free(internal);
	Free(base);
}

// EOF

