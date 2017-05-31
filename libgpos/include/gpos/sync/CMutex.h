//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CMutex.h
//
//	@doc:
//		Abstraction of mutex primitives; maps error values from mutex operations
//		onto GPOS's exception framework
//---------------------------------------------------------------------------
#ifndef GPOS_CMutex_H
#define GPOS_CMutex_H

#include "gpos/types.h"
#include "gpos/common/syslibwrapper.h"
#include "gpos/common/pthreadwrapper.h"
#include "gpos/common/CList.h"
#include "gpos/common/CWallClock.h"
#include "gpos/task/IWorker.h"
#include "gpos/task/CWorkerId.h"

#define GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC   (GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC / 4)
#if defined(GPOS_Darwin)
#define GPOS_MUTEX_SLEEP_SHORT_INTERVAL_USEC   (ULONG(0))
#define GPOS_MUTEX_SLEEP_LONG_INTERVAL_USEC    (ULONG(1000))
#define GPOS_MUTEX_SLEEP_INTERVAL_THRESHOLD    (10)
#endif // GPOS_Darwin

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CMutexBase
	//
	//	@doc:
	//		Base class for templated mutexes; provides SLink for accounting
	//
	//---------------------------------------------------------------------------
	class CMutexBase
	{
		// CEvent needs access to protected member function Pmutex
		friend class CEvent;
				
		private:
		
			// no copy ctor
			CMutexBase(const CMutexBase&);

			// trackability
			BOOL m_fTrackable;

		protected: 
		
			// pthread mutex
			PTHREAD_MUTEX_T m_tmutex;

			// counting locks
			volatile ULONG m_ulLockCount;
			
#ifdef GPOS_DEBUG
			// id of holder thread
			CWorkerId m_wid;
#endif // GPOS_DEBUG

			// relinquish mutex when waiting for an event
			void Relinquish()
			{
				GPOS_ASSERT(FOwned());

				m_ulLockCount = 0;
#ifdef GPOS_DEBUG
				m_wid.Invalid();
#endif // GPOS_DEBUG
			}

			// regaining mutex after a wait
			void Regain()
			{
				GPOS_ASSERT(!FOwned());

#ifdef GPOS_DEBUG
				m_wid.Current();
#endif // GPOS_DEBUG
				m_ulLockCount = 1;
			}

			// expose raw mutex to CEvent
			PTHREAD_MUTEX_T *Ptmutex()
			{
				return &m_tmutex;
			}
					
		public:
		
			// ctor
			CMutexBase
				(
				BOOL fTrackable
				)
				: 
				m_fTrackable(fTrackable),
				m_ulLockCount(0)
#ifdef GPOS_DEBUG
				,
				m_wid(false /*fValid*/)
#endif // GPOS_DEBUG
			{}
			
			// dtor
			virtual ~CMutexBase() {}
			
			// lock/trylock/unlock
			virtual void Lock() = 0;
			virtual BOOL FTryLock() = 0;
			virtual void Unlock() = 0;
					
#ifdef GPOS_DEBUG
			BOOL FOwned() const
            {
                CWorkerId wid;
                return 0 != m_ulLockCount && m_wid.FEqual(wid);
            }
#endif // GPOS_DEBUG

			BOOL FTrackable() const
			{
				return m_fTrackable;
			}

			// link for accounting lists
			SLink m_link;
	
	}; // class CMutexBase



	//---------------------------------------------------------------------------
	//	@class:
	//		CMutexTyped
	//
	//	@doc:
	//		Template class to defined kind (regular/recursive) and trackability
	//		at type level
	//
	//---------------------------------------------------------------------------
	template<INT iMutexType, BOOL fTrackable>
	class CMutexTyped : public CMutexBase
	{
		private:
		
			// lock mutex, CFA on regular intervals
			INT ITimedLock()
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

			// attempt to lock mutex
			INT IAttemptLock(BOOL fBlocking)
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

		public:
		
			// ctor
			CMutexTyped<iMutexType, fTrackable>()
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
					
			// dtor
			~CMutexTyped<iMutexType, fTrackable> ()
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
					
			// acquire lock
			void Lock()
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
			
			// attempt locking
			BOOL FTryLock()
            {
                GPOS_ASSERT_NO_SPINLOCK;

                INT iRet = IAttemptLock(false /*fBlocking*/);
                GPOS_ASSERT(EBUSY == iRet || 0 == iRet);

                return (0 == iRet);
            }

			// release lock
			void Unlock()
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

	}; // class CMutexTyped<int iMutexType, fTrackable>
	
	
	// define publicly used types
	
	// use this only in OS abstraction layer
	typedef CMutexTyped<PTHREAD_MUTEX_DEFAULT, false /*fTrackable*/> CMutexOS;
	typedef CMutexTyped<PTHREAD_MUTEX_RECURSIVE, false /*fTrackable*/> CMutexRecursiveOS;
	
	// use these in regular code where worker context is always available
	typedef CMutexTyped<PTHREAD_MUTEX_DEFAULT, true /*fTrackable*/> CMutex;
	typedef CMutexTyped<PTHREAD_MUTEX_RECURSIVE, true /*fTrackable*/> CMutexRecursive;
}

#endif // !GPOS_CMutex_H

// EOF

