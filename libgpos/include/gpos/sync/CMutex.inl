//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CMutex.inl
//
//	@doc:
//		Implementation of inlined functions
//---------------------------------------------------------------------------
#ifndef GPOS_CMutex_INL
#define GPOS_CMutex_INL

#include "gpos/common/pthreadwrapper.h"

namespace gpos
{

#ifdef GPOS_DEBUG
	//---------------------------------------------------------------------------
	//	@function:
	//		CMutexBase::FOwned
	//
	//	@doc:
	//		Check if caller holds mutex; only to be used in asserts;
	//
	//		Assumption: called from a place where the answer from this function
	//		is certain to be correct even if not holding the mutex
	//
	//---------------------------------------------------------------------------
	inline
	BOOL
	CMutexBase::FOwned() const
	{
		CWorkerId wid;
		return 0 != m_ulLockCount && m_wid.FEqual(wid);
	}
#endif // GPOS_DEBUG


	//---------------------------------------------------------------------------
	//	@function:
	//		CMutexTyped::CMutexTyped
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
	template<INT iMutexType, BOOL fTrackable>
	CMutexTyped<iMutexType, fTrackable>::CMutexTyped()
		: 
		CMutexBase(fTrackable)
	{
		GPOS_ASSERT(
			PTHREAD_MUTEX_DEFAULT == iMutexType ||
			PTHREAD_MUTEX_RECURSIVE == iMutexType);
	
		// initialize mutex struct
		PTHREAD_MUTEX_T tmutex = PTHREAD_MUTEX_INITIALIZER;
		m_tmutex = tmutex;
		
		PTHREAD_MUTEXATTR_T mat;
		
		// init can run out of memory
		if (0 != pthread::IPthreadMutexAttrInit(&mat))
		{
			// raise OOM exception
			GPOS_OOM_CHECK(NULL);
		}
		
		// ignore return value -- all parameters have been checked already
		pthread::PthreadMutexAttrSettype(&mat, iMutexType);

		if (0 != pthread::IPthreadMutexInit(&m_tmutex, &mat))
		{
			// cleanup
			pthread::PthreadMutexAttrDestroy(&mat);
			
			// out of memory/resources
			GPOS_OOM_CHECK(NULL);
		}
		
		// ignore return value -- parameter already checked
		pthread::PthreadMutexAttrDestroy(&mat);
	}
	

	//---------------------------------------------------------------------------
	//	@function:
	//		CMutexTyped::~CMutexTyped
	//
	//	@doc:
	//		dtor
	//
	//---------------------------------------------------------------------------
	template<INT iMutexType, BOOL fTrackable>
	CMutexTyped<iMutexType, fTrackable>::~CMutexTyped () 
	{
		// disallow destruction of locked mutex
		GPOS_ASSERT(0 == m_ulLockCount && "Tried to destruct locked mutex.");
	
		// release all locks held
		while(0 < m_ulLockCount)
		{
			this->Unlock();
		}
	
		// deallocate resources
		pthread::PthreadMutexDestroy(&m_tmutex);
	}
	

	//---------------------------------------------------------------------------
	//	@function:
	//		CMutexTyped::IAttemptLock
	//
	//	@doc:
	//		attempt to lock mutex
	//
	//---------------------------------------------------------------------------
	template<INT iMutexType, BOOL fTrackable>
	INT
	CMutexTyped<iMutexType,fTrackable>::IAttemptLock
		(
		BOOL fBlocking
		)
	{
#ifdef GPOS_DEBUG
		GPOS_ASSERT_NO_SPINLOCK;

		// check whether we own this mutex already; if so, must be recursive mutex;
		// disallow potentially deadlocking attempts even if the lock function is only trylock
		BOOL fOwnedAlready = this->FOwned();
		GPOS_ASSERT_IMP(fOwnedAlready, PTHREAD_MUTEX_RECURSIVE == iMutexType && "Self-deadlock detected");
#endif // GPOS_DEBUG

		INT iRet = 0;

		if (fBlocking)
		{
			iRet = ITimedLock();
		}
		else
		{
			// attempt to lock the mutex
			iRet = pthread::IPthreadMutexTryLock(&m_tmutex);
		}
		
		if (0 != iRet)
		{
			return iRet;
		};

#ifdef GPOS_DEBUG
		if (!fOwnedAlready && this->FTrackable())
		{
			IWorker::PwrkrSelf()->RegisterMutex(this);
		}

		// track owner
		m_wid.Current();
#endif // GPOS_DEBUG	
		
		++m_ulLockCount;

		return 0;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CMutexTyped::ITimedLock
	//
	//	@doc:
	//		lock mutex, CFA on regular intervals
	//
	//---------------------------------------------------------------------------
	template<INT iMutexType, BOOL fTrackable>
	INT
	CMutexTyped<iMutexType,fTrackable>::ITimedLock()
	{
		GPOS_ASSERT_NO_SPINLOCK;

		INT iRet = 0;

		// macosx does not have pthread_mutex_timedlock;
		// use a workaround with pthread_mutex_trylock and usleep;
		// this approach has increased overhead when lock contention
		// is high; it is therefore restricted to macosx
#ifdef GPOS_Darwin
		CWallClock timerElapsed;
		ULONG ulRetries = 0;

		// loop until we get the lock
		do
		{
			// attempt to lock the mutex
			iRet = pthread::IPthreadMutexTryLock(&m_tmutex);
			GPOS_ASSERT(EINVAL != iRet && "Invalid mutex structure");

			// check if mutex is already locked
			if (EBUSY == iRet)
			{
				if (GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC <= timerElapsed.UlElapsedMS())
				{
					GPOS_CHECK_ABORT;

					timerElapsed.Restart();
				}

				ULONG ulInterval = GPOS_MUTEX_SLEEP_SHORT_INTERVAL_USEC;
				if (GPOS_MUTEX_SLEEP_INTERVAL_THRESHOLD < ulRetries++)
				{
					ulInterval = GPOS_MUTEX_SLEEP_LONG_INTERVAL_USEC;
					ulRetries = 0;
				}

				clib::USleep(ulInterval);
			}
			else
			{
				GPOS_ASSERT(0 == iRet && "Error occurred while trying to lock mutex");
			}
		}
		while (0 != iRet);
#else
		// loop until we get the lock
		do
		{
			// set expiration timer
			TIMEVAL tv;
			syslib::GetTimeOfDay(&tv, NULL);
			ULLONG ullCurrentUs = tv.tv_sec * GPOS_USEC_IN_SEC + tv.tv_usec;
			ULLONG ullExpireUs = ullCurrentUs + GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC * GPOS_USEC_IN_MSEC;
			TIMESPEC ts;
			ts.tv_sec = (ULONG_PTR) (ullExpireUs / GPOS_USEC_IN_SEC);
			ts.tv_nsec = (ULONG_PTR) ((ullExpireUs % GPOS_USEC_IN_SEC) * (GPOS_NSEC_IN_SEC / GPOS_USEC_IN_SEC));

			iRet = pthread::IPthreadMutexTimedlock(&m_tmutex, &ts);

			// check if mutex is already locked
			if (EBUSY == iRet)
			{
				GPOS_CHECK_ABORT;
			}
		}
		while (0 != iRet);
#endif // GPOS_Darwin

		return iRet;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CMutexTyped::Lock
	//
	//	@doc:
	//		acquire lock on mutex
	//
	//---------------------------------------------------------------------------
	template<INT iMutexType, BOOL fTrackable>
	void 
	CMutexTyped<iMutexType,fTrackable>::Lock()
	{
#ifdef GPOS_DEBUG
		GPOS_ASSERT_NO_SPINLOCK;

		INT iRet =
#else
		// ignore return value in optimized builds
		(void)
#endif // GPOS_DEBUG
		IAttemptLock(true /*fBlocking*/);
		
		GPOS_ASSERT(0 == iRet && "Unexpectedly failed to lock mutex");
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CMutexTyped::FTryLock
	//
	//	@doc:
	//		try acquiring lock on mutex
	//
	//---------------------------------------------------------------------------
	template<INT iMutexType, BOOL fTrackable>
	BOOL
	CMutexTyped<iMutexType,fTrackable>::FTryLock()
	{
		GPOS_ASSERT_NO_SPINLOCK;

		INT iRet = IAttemptLock(false /*fBlocking*/);
		GPOS_ASSERT(EBUSY == iRet || 0 == iRet);
		
		return (0 == iRet);
	}
	

	//---------------------------------------------------------------------------
	//	@function:
	//		CMutexTyped::Unlock
	//
	//	@doc:
	//		release lock
	//
	//---------------------------------------------------------------------------
	template<INT iMutexType, BOOL fTrackable>
	void 
	CMutexTyped<iMutexType, fTrackable>::Unlock()
	{
#ifdef GPOS_DEBUG
		GPOS_ASSERT_NO_SPINLOCK;

		GPOS_ASSERT(FOwned());

		// prepare for final release of lock
		BOOL fUnlock = false;
		if (1 == m_ulLockCount)
		{
			fUnlock = true;
			m_wid.Invalid();
			if (this->FTrackable())
			{
				IWorker::PwrkrSelf()->UnregisterMutex(this);
			}
		}
#endif // GPOS_DEBUG

		--m_ulLockCount;
		
		// ignore return values -- parameters/context have been checked already
		(void) pthread::IPthreadMutexUnlock(&m_tmutex);
		
		GPOS_ASSERT_IMP(fUnlock, !FOwned());
	}

}

#endif // !GPOS_CMutex_INL


// EOF
