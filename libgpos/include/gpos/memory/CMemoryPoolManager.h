//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//
//
//	@filename:
//		CMemoryPoolManager.h
//
//	@doc:
//		Central memory pool manager;
//		provides factory method to generate memory pools;
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolManager_H
#define GPOS_CMemoryPoolManager_H

#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/common/CSyncHashtableAccessByIter.h"
#include "gpos/common/CSyncHashtableIter.h"
#include "gpos/memory/CMemoryPool.h"



#define GPOS_MEMORY_POOL_HT_SIZE	(1024)		// number of hash table buckets

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPoolManager
	//
	//	@doc:
	//		Global instance of memory pool management; singleton;
	//
	//---------------------------------------------------------------------------
	class CMemoryPoolManager
	{	
		public:

			// different types of pools
			enum AllocType
			{
				EatTracker,
				EatStack
			};

		private:

			typedef CSyncHashtableAccessByKey<CMemoryPool, ULONG_PTR>
				MemoryPoolKeyAccessor;

			typedef CSyncHashtableIter<CMemoryPool, ULONG_PTR>
				MemoryPoolIter;

			typedef CSyncHashtableAccessByIter<CMemoryPool, ULONG_PTR>
				MemoryPoolIterAccessor;

			// memory pool used to get memory from the underlying system
			// all created pools use this as their underlying allocator
			CMemoryPool *m_base_memory_pool;

			// memory pool in which all objects created by the manager itself
			// are allocated - must be thread-safe
			CMemoryPool *m_internal_memory_pool;

			// memory pool in which all objects created using global new operator
			// are allocated
			CMemoryPool *m_global_memory_pool;

			// are allocations using global new operator allowed?
			BOOL m_allow_global_new;

			// hash table to maintain created pools
			CSyncHashtable<CMemoryPool, ULONG_PTR> m_hash_table;

			// global instance
			static CMemoryPoolManager *m_memory_pool_mgr;

			// private ctor
			CMemoryPoolManager
				(
				CMemoryPool *internal,
				CMemoryPool *base
				);

			// create new pool of given type
			CMemoryPool *New
				(
				AllocType alloc_type,
				CMemoryPool *underlying_memory_pool,
				ULLONG capacity,
				BOOL thread_safe,
				BOOL owns_underlying_memory_pool
				);

#ifdef GPOS_DEBUG
			// surround new pool with tracker pools
			CMemoryPool *CreatePoolStack
				(
				AllocType type,
				ULLONG capacity,
				BOOL thread_safe
				);
#endif // GPOS_DEBUG

			// no copy ctor
			CMemoryPoolManager(const CMemoryPoolManager&);

			// clean-up memory pools
			void Cleanup();

			// destroy a memory pool at shutdown
			static
			void DestroyMemoryPoolAtShutdown(CMemoryPool *mp);

		public:

			// create new memory pool
			CMemoryPool *Create
				(
				CMemoryPoolManager::AllocType alloc_type,
				BOOL thread_safe,
				ULLONG capacity
				);
				
			// release memory pool
			void Destroy(CMemoryPool *);

			// delete a pool that is not registered with the memory pool manager
			void DeleteUnregistered(CMemoryPool *);
			
#ifdef GPOS_DEBUG
			// print internal contents of allocated memory pools
			IOstream &OsPrint(IOstream &os);

			// print memory pools whose allocated size above the given threshold
			void PrintOverSizedPools(CMemoryPool *trace, ULLONG size_threshold);
#endif // GPOS_DEBUG

			// delete memory pools and release manager
			void Shutdown();

			// accessor of memory pool used in global new allocations
			CMemoryPool *GetGlobalMemoryPool()
			{
				return m_global_memory_pool;
			}

			// are allocations using global new operator allowed?
			BOOL IsGlobalNewAllowed() const
			{
				return m_allow_global_new;
			}

			// disable allocations using global new operator
			void DisableGlobalNew()
			{
				m_allow_global_new = false;
			}

			// enable allocations using global new operator
			void EnableGlobalNew()
			{
				m_allow_global_new = true;
			}

			// return total allocated size in bytes
			ULLONG TotalAllocatedSize();

			// initialize global instance
			static
			GPOS_RESULT Init(void* (*) (SIZE_T), void (*) (void*));

			// global accessor
			static
			CMemoryPoolManager *GetMemoryPoolMgr()
			{
				return m_memory_pool_mgr;
			}

	}; // class CMemoryPoolManager
}

#endif // !GPOS_CMemoryPoolManager_H

// EOF

