//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSpinlock.inl
//
//	@doc:
//		Implementation of inlined functions;
//---------------------------------------------------------------------------
#ifndef GPOS_CSpinlock_INL
#define GPOS_CSpinlock_INL

#include "gpos/sync/atomic.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@function:
	//		CSpinlockRanked::Lock
	//
	//	@doc:
	//		Dtor
	//
	//---------------------------------------------------------------------------
	template<ULONG ulRank>
	CSpinlockRanked<ulRank>::~CSpinlockRanked()
	{
		// since this might burn up the CPU be defensive
		if (m_fLocked)
		{
			Unlock();
			GPOS_ASSERT(!"Tried to destruct locked spinlock.");
		}
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CSpinlockRanked::Lock
	//
	//	@doc:
	//		Acquisition of lock; implements TTAS spinlock
	//
	//---------------------------------------------------------------------------
	template<ULONG ulRank>
	void
	CSpinlockRanked<ulRank>::Lock()
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CSpinlockRanked::Unlock
	//
	//	@doc:
	//		Release lock;
	//
	//---------------------------------------------------------------------------
	template<ULONG ulRank>
	void
	CSpinlockRanked<ulRank>::Unlock()
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
	//---------------------------------------------------------------------------
	//	@function:
	//		CSpinlockRanked::FOwned
	//
	//	@doc:
	//		Check if we hold the lock; only to be used for asserts;
	//
	//---------------------------------------------------------------------------
	template<ULONG ulRank>
	BOOL
	CSpinlockRanked<ulRank>::FOwned()
	const
	{
		CWorkerId wid;
		return m_fLocked && m_wid.FEqual(wid);
	}
#endif // GPOS_ASSERT
}

#endif // !GPOS_CSpinlock_INL

// EOF
