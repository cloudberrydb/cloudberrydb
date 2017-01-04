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
			enum EAllocType
			{
				EatTracker,
				EatStack,
				EatSlab
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
			IMemoryPool *m_pmpBase;

			// memory pool in which all objects created by the manager itself
			// are allocated - must be thread-safe
			IMemoryPool *m_pmpInternal;

			// memory pool in which all objects created using global new operator
			// are allocated
			IMemoryPool *m_pmpGlobal;

			// are allocations using global new operator allowed?
			BOOL m_fAllowGlobalNew;

			// hash table to maintain created pools
			CSyncHashtable<CMemoryPool, ULONG_PTR, CSpinlockOS> m_sht;

			// global instance
			static CMemoryPoolManager *m_pmpm;

			// down-cast IMemoryPool to CMemoryPool
			CMemoryPool *PmpConvert(IMemoryPool *pmp)
			{
				GPOS_ASSERT(NULL != pmp);

				return dynamic_cast<CMemoryPool*>(pmp);
			}

			// private ctor
			CMemoryPoolManager
				(
				IMemoryPool *pmpInternal,
				IMemoryPool *pmpBase
				);

			// create new pool of given type
			IMemoryPool *PmpNew
				(
				EAllocType eat,
				IMemoryPool *pmpUnderlying,
				ULLONG ullCapacity,
				BOOL fThreadSafe,
				BOOL fOwnsUnderlying
				);

#ifdef GPOS_DEBUG
			// surround new pool with tracker pools
			IMemoryPool *PmpCreatePoolStack
				(
				EAllocType eat,
				ULLONG ullCapacity,
				BOOL fThreadSafe
				);
#endif // GPOS_DEBUG

			// no copy ctor
			CMemoryPoolManager(const CMemoryPoolManager&);

			// clean-up memory pools
			void Cleanup();

			// destroy a memory pool at shutdown
			static
			void DestroyMemoryPoolAtShutdown(CMemoryPool *pmp);

		public:

			// create new memory pool
			IMemoryPool *PmpCreate
				(
				CMemoryPoolManager::EAllocType ept,
				BOOL fThreadSafe,
				ULLONG ullCapacity
				);
				
			// release memory pool
			void Destroy(IMemoryPool *);

			// delete a pool that is not registered with the memory pool manager
			void DeleteUnregistered(IMemoryPool *);
			
#ifdef GPOS_DEBUG
			// print internal contents of allocated memory pools
			IOstream &OsPrint(IOstream &os);

			// print memory pools whose allocated size above the given threshold
			void PrintOverSizedPools(IMemoryPool *pmpTrace, ULLONG ullSizeThreshold);
#endif // GPOS_DEBUG

			// delete memory pools and release manager
			void Shutdown();

			// accessor of memory pool used in global new allocations
			IMemoryPool *PmpGlobal()
			{
				return m_pmpGlobal;
			}

			// are allocations using global new operator allowed?
			BOOL FAllowGlobalNew() const
			{
				return m_fAllowGlobalNew;
			}

			// disable allocations using global new operator
			void DisableGlobalNew()
			{
				m_fAllowGlobalNew = false;
			}

			// enable allocations using global new operator
			void EnableGlobalNew()
			{
				m_fAllowGlobalNew = true;
			}

			// return total allocated size in bytes
			ULLONG UllTotalAllocatedSize();

			// initialize global instance
			static
			GPOS_RESULT EresInit(void* (*) (SIZE_T), void (*) (void*));

			// global accessor
			static
			CMemoryPoolManager *Pmpm()
			{
				return m_pmpm;
			}

	}; // class CMemoryPoolManager
}

#endif // !GPOS_CMemoryPoolManager_H

// EOF

