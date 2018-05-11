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

			typedef CSyncHashtableAccessByKey<CMemoryPool, ULONG_PTR, CSpinlockOS>
				MemoryPoolKeyAccessor;

			typedef CSyncHashtableIter<CMemoryPool, ULONG_PTR, CSpinlockOS>
				MemoryPoolIter;

			typedef CSyncHashtableAccessByIter<CMemoryPool, ULONG_PTR, CSpinlockOS>
				MemoryPoolIterAccessor;

			// memory pool used to get memory from the underlying system
			// all created pools use this as their underlying allocator
			IMemoryPool *m_base_memory_pool;

			// memory pool in which all objects created by the manager itself
			// are allocated - must be thread-safe
			IMemoryPool *m_internal_memory_pool;

			// memory pool in which all objects created using global new operator
			// are allocated
			IMemoryPool *m_global_memory_pool;

			// are allocations using global new operator allowed?
			BOOL m_allow_global_new;

			// hash table to maintain created pools
			CSyncHashtable<CMemoryPool, ULONG_PTR, CSpinlockOS> m_hash_table;

			// global instance
			static CMemoryPoolManager *m_memory_pool_mgr;

			// down-cast IMemoryPool to CMemoryPool
			CMemoryPool *Convert(IMemoryPool *mp)
			{
				GPOS_ASSERT(NULL != mp);

				return dynamic_cast<CMemoryPool*>(mp);
			}

			// private ctor
			CMemoryPoolManager
				(
				IMemoryPool *internal,
				IMemoryPool *base
				);

			// create new pool of given type
			IMemoryPool *New
				(
				AllocType alloc_type,
				IMemoryPool *underlying_memory_pool,
				ULLONG capacity,
				BOOL thread_safe,
				BOOL owns_underlying_memory_pool
				);

#ifdef GPOS_DEBUG
			// surround new pool with tracker pools
			IMemoryPool *CreatePoolStack
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
			IMemoryPool *Create
				(
				CMemoryPoolManager::AllocType alloc_type,
				BOOL thread_safe,
				ULLONG capacity
				);
				
			// release memory pool
			void Destroy(IMemoryPool *);

			// delete a pool that is not registered with the memory pool manager
			void DeleteUnregistered(IMemoryPool *);
			
#ifdef GPOS_DEBUG
			// print internal contents of allocated memory pools
			IOstream &OsPrint(IOstream &os);

			// print memory pools whose allocated size above the given threshold
			void PrintOverSizedPools(IMemoryPool *trace, ULLONG size_threshold);
#endif // GPOS_DEBUG

			// delete memory pools and release manager
			void Shutdown();

			// accessor of memory pool used in global new allocations
			IMemoryPool *GetGlobalMemoryPool()
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

