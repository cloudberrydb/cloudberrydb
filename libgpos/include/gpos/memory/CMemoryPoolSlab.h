//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum Inc.
//
//	@filename:
//		CMemoryPoolSlab.h
//
//	@doc:
//		Memory pool that implements a slab allocator.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolSlab_H
#define GPOS_CMemoryPoolSlab_H

#include "gpos/base.h"
#include "gpos/utils.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CSlabCache.h"

#define GPOS_MEM_SLAB_CACHE_GROUPS		(14)
#define GPOS_MEM_SLAB_CACHES_PER_GROUP	(1 << 2) // must be power of 2

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPoolSlab
	//
	//	@doc:
	//
	//		Slab allocator;
	//		contains caches of equally-sized slabs;
	//		each slab is broken to memory slots that are used for allocations;
	//
	//---------------------------------------------------------------------------
	class CMemoryPoolSlab : public CMemoryPool
	{
		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		SAllocXLHeader
			//
			//	@doc:
			//
			//		Header for XL allocations
			//
			//---------------------------------------------------------------------------
			struct SAllocXLHeader
			{
				// ID
				ULONG_PTR m_ulpId;

				// allocation size
				ULONG m_ulAllocSize;

				// link for hashtable
				SLink m_link;

				// allocation type; set to XL allocation;
				// use ULONG_PTR for alignment;
				ULONG_PTR ulpSlabAlloc;

				// invalid ID
				static
				ULONG_PTR m_ulpIdInvalid;

				// get header from allocation
				static
				SAllocXLHeader *Pallocxl(void *pv)
				{
					SAllocXLHeader *pallocxl = static_cast<SAllocXLHeader*>(pv) - 1;
					GPOS_ASSERT(CSlab::EatXLAlloc == pallocxl->ulpSlabAlloc);

					return pallocxl;
				}
			};

			typedef CSyncHashtable<SAllocXLHeader, ULONG_PTR, CSpinlockOS>
				AllocXLHashTable;

			typedef CSyncHashtableAccessByKey<SAllocXLHeader, ULONG_PTR, CSpinlockOS>
				AllocXLKeyAccessor;

			typedef CSyncHashtableIter<SAllocXLHeader, ULONG_PTR, CSpinlockOS>
				AllocXLIter;

			typedef CSyncHashtableAccessByIter<SAllocXLHeader, ULONG_PTR, CSpinlockOS>
				AllocXLIterAccessor;


			// array of cache groups
			CSlabCache *m_rgrgpsc[GPOS_MEM_SLAB_CACHE_GROUPS][GPOS_MEM_SLAB_CACHES_PER_GROUP];

			// index of last used cache per group
			ULONG_PTR m_rgulpCacheIdx[GPOS_MEM_SLAB_CACHE_GROUPS];

			// hash table of XL allocations
			AllocXLHashTable *m_pshtAllocXL;

			// size of reserved memory;
			// this includes total allocated memory and pending allocations;
			volatile ULONG_PTR m_ulpReserved;

			// max memory to allow in the pool;
			// if equal to ULLONG, checks for exceeding max memory are bypassed
			const ULLONG m_ullCapacity;

			// initialize slab caches
			void InitCaches();

			// find cache corresponding to allocation size
			CSlabCache *Psc(ULONG ulAlloc);

			// allocate XL object
			void *PvAllocateXL(const ULONG ulBytes, const CHAR *szFile, const ULONG ulLine);

			// release XL object
			void FreeXL(SAllocXLHeader *pallocxl);

			// check if capacity is set
			BOOL FSetCapacity() const
			{
				return ULLONG_MAX != m_ullCapacity;
			}

			// update reserved memory with allocation request
			BOOL FReserveMem(ULONG ulAlloc);

			// update reserved memory with release request
			void UnreserveMem(ULONG ulAlloc);

			// check if allocation is in a slab
			static
			BOOL FSlabAlloc
				(
				void *pv
				)
			{
				// check flag (ULONG_PTR) before allocation
				return (CSlab::EatSlabAlloc == *(static_cast<ULONG_PTR*>(pv) -1));
			}

#ifdef GPOS_DEBUG
			// check if a particular allocation is sound for this memory pool
			void CheckAllocation(void *pv);
#endif // GPOS_DEBUG

			// private copy ctor
			CMemoryPoolSlab(CMemoryPoolSlab &);

		public:

			// ctor
			CMemoryPoolSlab
				(
				IMemoryPool *pmp,
				ULLONG ullCapacity,
				BOOL fThreadSafe,
				BOOL fOwnsUnderlying
				);

			// dtor
			virtual
			~CMemoryPoolSlab();

			// allocate memory
			virtual
			void *PvAllocate
				(
				const ULONG ulBytes,
				const CHAR *szFile,
				const ULONG ulLine
				);

			// free memory - memory is released when the memory pool is torn down
			virtual
			void Free(void *pv);

			// return all used memory to the underlying pool and tear it down
			virtual
			void TearDown();

			// check if the pool stores a pointer to itself at the end of
			// the header of each allocated object;
			virtual
			BOOL FStoresPoolPointer() const
			{
				return true;
			}

			// return total allocated size
			virtual
			ULLONG UllTotalAllocatedSize() const
			{
				return (ULLONG) m_ulpReserved;
			}
	};
}

#endif // !GPOS_CMemoryPoolSlab_H

// EOF

