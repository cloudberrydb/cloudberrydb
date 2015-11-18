//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolTracker.h
//
//	@doc:
//		Memory pool that allocates from an underlying allocator and adds on
//		statistics and debugging
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolTracker_H
#define GPOS_CMemoryPoolTracker_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/sync/CSpinlock.h"

namespace gpos
{
	// prototypes
	class CAutoMutex;

	// memory pool with statistics and debugging support
	class CMemoryPoolTracker : public CMemoryPool
	{
		private:

			//---------------------------------------------------------------------------
			//	@struct:
			//		SAllocHeader
			//
			//	@doc:
			//		Defines memory block header layout for all allocations;
			//	 	does not include the pointer to the pool;
			//
			//---------------------------------------------------------------------------
			struct SAllocHeader
			{
				// sequence number
				ULLONG m_ullSerial;

				// user-visible size
				ULONG m_ulSize;

				// file name
				const CHAR *m_szFilename;

				// line in file
				ULONG m_ulLine;

#ifdef GPOS_DEBUG
				// allocation stack
				CStackDescriptor m_sd;
#endif // GPOS_DEBUG

				// link for allocation list
				SLink m_link;
			};

			// lock for synchronization
			CSpinlockOS m_slock;

			// statistics
			CMemoryPoolStatistics m_mps;

			// allocation sequence number
			ULONG m_ulAllocSequence;

			// memory pool capacity;
			// if equal to ULLONG, checks for exceeding max memory are bypassed
			const ULLONG m_ullCapacity;

			// size of reserved memory;
			// this includes total allocated memory and pending allocations;
			ULLONG m_ullReserved;

			// list of allocated (live) objects
			CList<SAllocHeader> m_listAllocations;

			// attempt to reserve memory for allocation
			BOOL FReserve(CAutoSpinlock &as, ULONG ulAlloc);

			// revert memory reservation
			void Unreserve(CAutoSpinlock &as, ULONG ulAlloc, BOOL fAvailableMem);

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

			// private copy ctor
			CMemoryPoolTracker(CMemoryPoolTracker &);

		protected:

			// dtor
			virtual
			~CMemoryPoolTracker();

		public:

			// ctor
			CMemoryPoolTracker
				(
				IMemoryPool *pmpUnderlying,
				ULLONG ullSize,
				BOOL fThreadSafe,
				BOOL fOwnsUnderlying
				);

			// allocate memory
			virtual
			void *PvAllocate
				(
				const ULONG ulBytes,
				const CHAR *szFile,
				const ULONG ulLine
				);

			// free memory
			virtual
			void Free(void *pv);

			// prepare the memory pool to be deleted
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
				return m_mps.UllTotalAllocatedSize();
			}

#ifdef GPOS_DEBUG

			// check if the memory pool keeps track of live objects
			virtual
			BOOL FSupportsLiveObjectWalk() const
			{
				return true;
			}

			// walk the live objects
			virtual
			void WalkLiveObjects(gpos::IMemoryVisitor *pmov);

			// check if statistics tracking is supported
			virtual
			BOOL FSupportsStatistics() const
			{
				return true;
			}

			// return the current statistics
			virtual
			void UpdateStatistics(CMemoryPoolStatistics &mps);

#endif // GPOS_DEBUG

	};
}

#endif // !GPOS_CMemoryPoolTracker_H

// EOF

