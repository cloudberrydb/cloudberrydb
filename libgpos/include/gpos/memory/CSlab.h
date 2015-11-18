//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum Inc.
//
//	@filename:
//		CSlab.h
//
//	@doc:
//		Memory slab, divided to equally sized slots that are used to
//		allocate objects of size smaller or equal to the slot size.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSlab_H
#define GPOS_CSlab_H

#include "gpos/base.h"
#include "gpos/utils.h"
#include "gpos/common/CSyncList.h"


namespace gpos
{

	// fwd declaration
	class CSlabCache;

	//---------------------------------------------------------------------------
	//	@class:
	//		CSlab
	//
	//	@doc:
	//
	//		Memory slab; contains an array of slots;
	//
	//---------------------------------------------------------------------------
	class CSlab
	{
		// cache accesses internal functionality
		friend class CSlabCache;

		public:

			// enum indicating allocation type
			enum EAllocType
			{
				EatSlabAlloc,
				EatXLAlloc
			};

		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		SSlotHeader
			//
			//	@doc:
			//
			//		Memory slot header; used for individual allocations;
			//
			//---------------------------------------------------------------------------
			struct SSlotHeader
			{
				// pointer to owning slab
				CSlab *m_pslabOwner;

				// pointer to next free slot
				SSlotHeader *m_pshNext;

				// allocation type; set to slab allocation;
				// use ULONG_PTR for alignment;
				ULONG_PTR ulpAllocType;
			};

			// slab ID
			const ULONG_PTR m_ulpId;

			// number of slots
			const ULONG m_ulSlots;

			// slot size
			const ULONG m_ulSlotSize;

			// counter of free slots
			ULONG m_ulSlotsFree;

			// flag indicating if slab is in cache's stack of active slabs;
			BOOL m_fActive;

			// top of free slot stack
			SSlotHeader *m_pshstackFree;

			// pointer to owning cache
			CSlabCache *m_pscOwner;

			// spin lock - shared by all slabs of a cache
			CSpinlockBase &m_slock;

			// link for cache's list of active slabs
			SLink m_linkActive;

			// link for cache's list of allocated slabs
			SLink m_linkAllocated;

			// invalid slab ID
			static
			ULONG_PTR m_ulpIdInvalid;

			// get slot by offset
			SSlotHeader &GetSlot(ULONG ulOffset);

			// push slot to stack
			void Push
				(
				SSlotHeader *psh
				)
			{
				GPOS_ASSERT(NULL != psh);

				psh->m_pshNext = m_pshstackFree;
				m_pshstackFree = psh;
			}

			// pop slot from stack
			SSlotHeader *PshPop()
			{
				SSlotHeader *psh = m_pshstackFree;
				if (NULL != m_pshstackFree)
				{
					m_pshstackFree = m_pshstackFree->m_pshNext;
				}
				return psh;
			}

			// mark slab as inactive
			void SetInactive()
			{
				GPOS_ASSERT(m_fActive);
				m_fActive = false;
			}

			// no copy ctor
			CSlab(const CSlab &);

		public:

			// ctor
			CSlab
				(
				CSlabCache *pscOwner,
				ULONG ulSlots,
				ULONG ulSlotSize,
				CSpinlockBase &slock
				);

			// reserve slot
			void *PvAllocate(BOOL fNewSlab);

			// recycle slot
			void Free(void *pvObj);

			// check if slab is unused
			BOOL FUnused() const
			{
				return m_ulSlots == m_ulSlotsFree;
			}

			// owner accessor
			CSlabCache *Psc() const
			{
				return m_pscOwner;
			}

			// calculate slab size
			static
			ULONG UlSize(ULONG ulSlots, ULONG ulSlotSize);

			// get slab cache from slot
			static
			CSlab *PslabConvert(void *pv);

#ifdef GPOS_DEBUG
			BOOL FFull() const
			{
				return 0 == m_ulSlotsFree && NULL == m_pshstackFree;
			}
#endif // GPOS_DEBUG
	};
}

#endif // !GPOS_CSlab_H

// EOF

