//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSpinlock.h
//
//	@doc:
//		Spinlock implementation broken into a generic part and a template
//		specialization which incorporates the rank of a spinlock; 
//
//		Ranked Spinlock implementation: spinlocks can only be acquired in 
//		monotonic order of rank, i.e. while holding a lock of a given rank only 
//		locks of the same or a lower rank can be acquired.
//
//		Locks of rank 0 are distinguished in that they are used in the framework
//		implementation, i.e. they are used to build some of the accounting 
//		mechanisms used for tracking locks of higher rank;
//
//		The acquisition of spinlocks of rank 0 or higher are tracked in the
//		process context; the implementation is fortified with simple asserts
//		to prevent self-deadlock;
//---------------------------------------------------------------------------
#ifndef GPOS_CSpinlock_H
#define GPOS_CSpinlock_H

#include "gpos/base.h"
#include "gpos/task/IWorker.h"
#include "gpos/task/CWorkerId.h"

#include "gpos/common/CList.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CSpinlockBase
	//
	//	@doc:
	//		Abstract base class for all spinlocks; stores information about rank,
	//		used to decide whether a lock should be tracked
	//
	//---------------------------------------------------------------------------
	class CSpinlockBase 
	{
	
		private:
		
			// rank of spinlock
			ULONG m_ulRank;
		
		public:

			// ctor
			CSpinlockBase
				(
				ULONG ulRank
				)
				:
				m_ulRank(ulRank)
			{}
		
			// dtor
			virtual
			~CSpinlockBase()
			{};

			// acquire spinlock
			virtual
			void Lock() = 0;

			// release spinlock
			virtual
			void Unlock() = 0;
			

			// decide trackability of spinlock
			BOOL FTrackable()
			{
				// do not track lock of rank 0
				return 0 < m_ulRank;
			}

			ULONG UlRank() const
			{
				return m_ulRank;
			}
			
#ifdef GPOS_DEBUG
			// test whether we own the spinlock
			virtual
			BOOL FOwned() const = 0;
#endif // GPOS_ASSERT

			// link for accounting list
			SLink m_link;
			
	}; // class CSpinlockBase


	//---------------------------------------------------------------------------
	//	@class:
	//		CSpinlockRanked
	//
	//	@doc:
	//		Specialization of base class which uses rank as a template parameter
	//		to eliminate ambiguity compared to specifying the rank as parameter
	//		to the constructor.
	//
	//---------------------------------------------------------------------------
	template<ULONG ulRank>
	class CSpinlockRanked : public CSpinlockBase
	{
#ifdef GPOS_DEBUG
		// owning worker
		CWorkerId m_wid;
#endif // GPOS_DEBUG

		// lock indicator -- lock counter is not usable in asserts
		BOOL m_fLocked;

		// actual lock counter
		ULONG_PTR m_ulpLock;
		
		// counter for collisions
		ULONG_PTR m_ulpCollisions;
		
	public:

		// ctor
		CSpinlockRanked<ulRank>()
			:
			CSpinlockBase(ulRank),
#ifdef GPOS_DEBUG
			m_wid(false),
#endif // GPOS_DEBUG
			m_fLocked(false),
			m_ulpLock(0), 
			m_ulpCollisions(0)
		{}
		
		
		// dtor
		~CSpinlockRanked<ulRank>();

		// acquire lock
		void Lock();
		
		// release
		void Unlock();
		
#ifdef GPOS_DEBUG
		// test whether we own the spinlock
		BOOL FOwned() const;
#endif // GPOS_ASSERT

	}; // class CSpinlockRanked


	// GPOS SPINLOCKS - reserve range 0-200

	// core spinlock -- not tracked (used to implement tracking)
	typedef CSpinlockRanked<0>	CSpinlockOS;

	// generic cache spinlock
	typedef CSpinlockRanked<20> CSpinlockCache;


	// TEST SPINLOCKS

	typedef CSpinlockRanked<1000>	CSpinlockDummy;
	
	typedef CSpinlockRanked<1010> CSpinlockDummyLo;
	typedef CSpinlockRanked<1011> CSpinlockDummyHi;
}


// include implementation
#include "CSpinlock.inl"

#endif // !GPOS_CSpinlock_H

// EOF

