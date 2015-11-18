//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum Inc.
//
//	@filename:
//		CMemoryPoolStack.h
//
//	@doc:
//		Memory pool that allocates large blocks from the underlying pool and
//		incrementally reserves space in them.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolStack_H
#define GPOS_CMemoryPoolStack_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/common/CList.h"
#include "gpos/error/CFSimulator.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/sync/CSpinlock.h"


namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPoolStack
	//
	//	@doc:
	//
	//		Memory pool which allocates memory in blocks; memory is given away
	//		from the beginning to the end of each block. For requests that exceed
	//		the block size, the blocks are skipped and the request is satisfied
	//		from the underlying pool.
	//
	//		Note that this pool does not free up  memory, regardless of free
	//		calls, until it is torn down.
	//
	//---------------------------------------------------------------------------
	class CMemoryPoolStack : public CMemoryPool
	{
		private:

			// block descriptor
			struct SBlockDescriptor
			{
				// starting address for user data
				const void *m_pvUser;

				// total size
				ULONG m_ulTotal;

				// used size
				ULONG m_ulUsed;

				// link for block list
				SLink m_link;

				// init
				void Init
					(
					ULONG ulTotal
					)
				{
					m_pvUser = this + 1;
					m_ulTotal = ulTotal;
					m_ulUsed = GPOS_MEM_ALIGNED_STRUCT_SIZE(SBlockDescriptor);
					m_link.m_pvNext = NULL;
					m_link.m_pvPrev = NULL;
				}

				// check if there is enough space for allocation request
				BOOL FFit
					(
					ULONG ulAlloc
					)
					const
				{
					return (m_ulTotal - m_ulUsed >= ulAlloc);
				}
			};

			// currently used block
			SBlockDescriptor *m_pbd;

			// size of reserved memory;
			// this includes total allocated memory and pending allocations;
			volatile ULLONG m_ullReserved;

			// max memory to allow in the pool;
			// if equal to ULLONG, checks for exceeding max memory are bypassed
			const ULLONG m_ullCapacity;

			// default block size
			const ULONG m_ulBlockSize;

			// list of allocated blocks
			CList<SBlockDescriptor> m_listBlocks;

			// spinlock to protect allocations inside the currently used block
			CSpinlockOS m_slock;

			// allocate block from underlying pool
			SBlockDescriptor *PbdNew(ULONG ulSize);

			// find block to provide memory for allocation request
			SBlockDescriptor *PbdProvider(CAutoSpinlock &as, ULONG ulAlloc);

			// acquire spinlock if pool is thread-safe
			void SLock(CAutoSpinlock &as)
			{
				if (FThreadSafe())
				{
					as.Lock();
				}
			}

			// release spinlock if pool is thread-safe
			void SUnlock(CAutoSpinlock &as)
			{
				if (FThreadSafe())
				{
					as.Unlock();
				}
			}

#ifdef GPOS_DEBUG
			// check if a particular allocation is sound for this memory pool
			void CheckAllocation(void *pv) const;
#endif // GPOS_DEBUG

			// private copy ctor
			CMemoryPoolStack(CMemoryPoolStack &);

		public:

			// ctor
			CMemoryPoolStack
				(
				IMemoryPool *pmp,
				ULLONG ullCapacity,
				BOOL fThreadSafe,
				BOOL fOwnsUnderlying
				);

			// dtor
			virtual
			~CMemoryPoolStack();

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
			void Free
				(
#ifdef GPOS_DEBUG
				void *pv
#else
				void *
#endif // GPOS_DEBUG
				)
			{
#ifdef GPOS_DEBUG
				CheckAllocation(pv);
#endif // GPOS_DEBUG
			}

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
				return m_ullReserved;
			}
	};
}

#endif // !GPOS_CMemoryPoolStack_H

// EOF

