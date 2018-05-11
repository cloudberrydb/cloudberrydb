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
				const void *m_user;

				// total size
				ULONG m_total_size;

				// used size
				ULONG m_used_size;

				// link for block list
				SLink m_link;

				// init
				void Init
					(
					ULONG total
					)
				{
					m_user = this + 1;
					m_total_size = total;
					m_used_size = GPOS_MEM_ALIGNED_STRUCT_SIZE(SBlockDescriptor);
					m_link.m_next = NULL;
					m_link.m_prev = NULL;
				}

				// check if there is enough space for allocation request
				BOOL CanFit
					(
					ULONG alloc
					)
					const
				{
					return (m_total_size - m_used_size >= alloc);
				}
			};

			// currently used block
			SBlockDescriptor *m_block_descriptor;

			// size of reserved memory;
			// this includes total allocated memory and pending allocations;
			volatile ULLONG m_reserved;

			// max memory to allow in the pool;
			// if equal to ULLONG, checks for exceeding max memory are bypassed
			const ULLONG m_capacity;

			// default block size
			const ULONG m_blocksize;

			// list of allocated blocks
			CList<SBlockDescriptor> m_block_list;

			// spinlock to protect allocations inside the currently used block
			CSpinlockOS m_lock;

			// allocate block from underlying pool
			SBlockDescriptor *New(ULONG size);

			// find block to provide memory for allocation request
			SBlockDescriptor *FindMemoryBlock(CAutoSpinlock &as, ULONG alloc);

			// acquire spinlock if pool is thread-safe
			void SLock(CAutoSpinlock &as)
			{
				if (IsThreadSafe())
				{
					as.Lock();
				}
			}

			// release spinlock if pool is thread-safe
			void SUnlock(CAutoSpinlock &as)
			{
				if (IsThreadSafe())
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
				IMemoryPool *mp,
				ULLONG capacity,
				BOOL thread_safe,
				BOOL owns_underlying_memory_pool
				);

			// dtor
			virtual
			~CMemoryPoolStack();

			// allocate memory
			virtual
			void *Allocate
				(
				const ULONG bytes,
				const CHAR *file,
				const ULONG line
				);

			// free memory - memory is released when the memory pool is torn down
			virtual
			void Free
				(
#ifdef GPOS_DEBUG
				void *ptr
#else
				void *
#endif // GPOS_DEBUG
				)
			{
#ifdef GPOS_DEBUG
				CheckAllocation(ptr);
#endif // GPOS_DEBUG
			}

			// return all used memory to the underlying pool and tear it down
			virtual
			void TearDown();

			// check if the pool stores a pointer to itself at the end of
			// the header of each allocated object;
			virtual
			BOOL StoresPoolPointer() const
			{
				return true;
			}

			// return total allocated size
			virtual
			ULLONG TotalAllocatedSize() const
			{
				return m_reserved;
			}
	};
}

#endif // !GPOS_CMemoryPoolStack_H

// EOF

