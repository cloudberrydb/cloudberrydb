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
#include "gpos/sync/atomic.h"

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
		~CSpinlockRanked<ulRank>()
        {
            // since this might burn up the CPU be defensive
            if (m_fLocked)
            {
                Unlock();
                GPOS_ASSERT(!"Tried to destruct locked spinlock.");
            }
        }

		// acquire lock
		void Lock()
        {
#ifdef GPOS_DEBUG
            GPOS_ASSERT_IMP(0 < ulRank && IWorker::PwrkrSelf(),
                            IWorker::PwrkrSelf()->FCanAcquireSpinlock(this) &&
                            "Tried to acquire spinlock in incorrect order or detected deadlock.");
#endif // GPOS_DEBUG

            ULONG ulAttempts = 0;

            // attempt getting the lock
            while(1)
            {
                // do not attempt a sync'd increment unless the counter is likely to be 0
                if (0 == m_ulpLock)
                {
                    // attempt sync'd increment
                    if (0 == gpos::UlpExchangeAdd(&m_ulpLock, 1))
                    {
                        break;
                    }

                    // count back down
                    gpos::UlpExchangeAdd(&m_ulpLock, -1);
                }

                // assert it is not us who holds the lock
                GPOS_ASSERT(!FOwned() && "self-deadlock detected");

                // trigger a back-off after a certain number of attempts
                if (ulAttempts++ > GPOS_SPIN_ATTEMPTS)
                {
                    // up stats
                    gpos::UlpExchangeAdd(&m_ulpCollisions, ulAttempts);
                    ulAttempts = 0;

                    clib::USleep(GPOS_SPIN_BACKOFF);

                    // TODO: 08/02/2009; add warning when burning
                    // a non-trackable lock; dependent on OPT-87, OPT-86

                    // non-trackable locks don't know about aborts
                    if (FTrackable())
                    {
                        // TODO: 03/09/2008; log that we're burning CPU

                        GPOS_CHECK_ABORT;
                    }
                }
            }

            // got the lock
            GPOS_ASSERT(m_ulpLock > 0);

            // final update of collision stats
            gpos::UlpExchangeAdd(&m_ulpCollisions, ulAttempts);

#ifdef GPOS_DEBUG
            if (0 < ulRank && NULL != IWorker::PwrkrSelf())
            {
                IWorker::PwrkrSelf()->RegisterSpinlock(this);
            }

            m_wid.Current();
#endif // GPOS_DEBUG
            m_fLocked = true;
        }
		
		// release
		void Unlock()
        {
#ifdef GPOS_DEBUG
            if (0 < ulRank && NULL != IWorker::PwrkrSelf())
            {
                IWorker::PwrkrSelf()->UnregisterSpinlock(this);
            }

            m_wid.Invalid();
            m_fLocked = false;
#endif // GPOS_DEBUG

            GPOS_ASSERT(m_ulpLock > 0);
            gpos::UlpExchangeAdd(&m_ulpLock, -1);
        }
		
#ifdef GPOS_DEBUG
		// test whether we own the spinlock
		BOOL FOwned() const
        {
            CWorkerId wid;
            return m_fLocked && m_wid.FEqual(wid);
        }
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

#endif // !GPOS_CSpinlock_H

// EOF

