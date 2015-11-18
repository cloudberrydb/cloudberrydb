//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum Inc.
//
//	@filename:
//		CSlabCache.h
//
//	@doc:
//		Memory slab cache;
//		creates and releases slabs, keeps track of which slabs are active
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSlabCache_H
#define GPOS_CSlabCache_H

#include "gpos/base.h"
#include "gpos/utils.h"
#include "gpos/common/CList.h"
#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/common/CSyncHashtableAccessByIter.h"
#include "gpos/common/CSyncHashtableIter.h"
#include "gpos/memory/CSlab.h"


namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSlabCache
	//
	//	@doc:
	//
	//		Slab cache; manages slabs;
	//
	//---------------------------------------------------------------------------
	class CSlabCache
	{
		private:

			// type shorthands
			typedef CList<CSlab> SlabList;

			// underlying pool
			IMemoryPool *m_pmp;

			// list of slabs that are actively used for allocations
			SlabList *m_pslablistActive;

			// list of allocated slabs
			SlabList *m_pslablistAllocated;

			// spinlock for synchronization of all operations
			CSpinlockOS m_slock;

			// slot size
			const ULONG m_ulSlotSize;

			// slots per slab
			const ULONG m_ulSlots;

			// threshold where release of unused slabs starts (upper boound)
			const ULONG m_ulThresholdHigh;

			// threshold where release of unused slabs ends (lower bound)
			const ULONG m_ulThresholdLow;

			// unused slab counter
			ULONG m_ulUnused;

			// reserved memory - shared by all caches of a pool
			volatile ULONG_PTR *m_pulpReserved;

			// memory pool capacity
			const ULLONG m_ullCapacity;

			// flag indicating if cache is thread-safe
			const BOOL m_fThreadSafe;

			// flag indicating if slab cache has been torn down
			BOOL m_fTornDown;

			// initialize slab
			CSlab *PslabCreate();

			// release slab
			void Release(CSlab *ps);

			// release unused slabs
			void Shrink(CAutoSpinlock &as);

			// allocate memory for slab
			void *PvAllocateSlab()
			{
				return m_pmp->PvAllocate
					(
					UlSlabSize(),
					NULL /*szFile*/,
					0 /*ulLine*/
					);
			}

			// attempt to allocate slot in active slabs
			void *PvAllocateSlot();

			// get slab size
			ULONG UlSlabSize() const
			{
				return CSlab::UlSize(m_ulSlots, m_ulSlotSize);
			}

			// reserve memory for new slab
			BOOL FReserveMem();

			// update reserved memory with release request
			void UnreserveMem();

			// no copy ctor
			CSlabCache(const CSlabCache&);

		public:

			// ctor
			CSlabCache
				(
				IMemoryPool *pmp,
				ULONG ulChunkSize,
				ULONG ulChunks,
				ULONG ulThresholdHigh,
				ULONG ulThresholdLow,
				volatile ULONG_PTR *pulpReserved,
				ULLONG ullCapacity,
				BOOL fThreadSafe
				);

			// dtor
			~CSlabCache();

			// allocate object
			void *PvAllocate();

			// slot size accessor
			ULONG UlSlotSize() const
			{
				return m_ulSlotSize;
			}

			// increment counter of unused slabs; may trigger shrinking
			void IncUnused
				(
				CAutoSpinlock &as
				)
			{
				GPOS_ASSERT_IMP(m_fThreadSafe, m_slock.FOwned());
				if (++m_ulUnused > m_ulThresholdHigh)
				{
					Shrink(as);
				}
			}

			// decrement counter of unused slabs
			void DecrUnused()
			{
				GPOS_ASSERT_IMP(m_fThreadSafe, m_slock.FOwned());
				m_ulUnused--;
			}

			// add slab to cache
			void Add
				(
				CSlab *pslab
				)
			{
				GPOS_ASSERT_IMP(m_fThreadSafe, m_slock.FOwned());
				m_pslablistActive->Append(pslab);
			}

			// release all slabs
			void Teardown();

			// acquire spinlock
			void SLock(CAutoSpinlock &as)
			{
				if (m_fThreadSafe)
				{
					as.Lock();
				}
			}

			// release spinlock
			void SUnlock(CAutoSpinlock &as)
			{
				if (m_fThreadSafe)
				{
					as.Unlock();
				}
			}

#ifdef GPOS_DEBUG
			// check if slab belongs to cache
			BOOL FOwns(CSlab *pslab);

			// check if cache has no slab
			BOOL FTornDown() const
			{
				return m_fTornDown;
			}
#endif // GPOS_DEBUG
	};
}

#endif // !GPOS_CSlabCache_H

// EOF

