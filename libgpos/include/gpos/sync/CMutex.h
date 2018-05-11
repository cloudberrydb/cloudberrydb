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
		// CEvent needs access to protected member function GetMutex
		friend class CEvent;
				
		private:
		
			// no copy ctor
			CMutexBase(const CMutexBase&);

			// trackability
			BOOL m_is_trackable;

		protected: 
		
			// pthread mutex
			PTHREAD_MUTEX_T m_mutex;

			// counting locks
			volatile ULONG m_lock_count;
			
#ifdef GPOS_DEBUG
			// id of holder thread
			CWorkerId m_wid;
#endif // GPOS_DEBUG

			// relinquish mutex when waiting for an event
			void Relinquish()
			{
				GPOS_ASSERT(IsOwned());

				m_lock_count = 0;
#ifdef GPOS_DEBUG
				m_wid.SetThreadToInvalid();
#endif // GPOS_DEBUG
			}

			// regaining mutex after a wait
			void Regain()
			{
				GPOS_ASSERT(!IsOwned());

#ifdef GPOS_DEBUG
				m_wid.SetThreadToCurrent();
#endif // GPOS_DEBUG
				m_lock_count = 1;
			}

			// expose raw mutex to CEvent
			PTHREAD_MUTEX_T *GetMutex()
			{
				return &m_mutex;
			}
					
		public:
		
			// ctor
			CMutexBase
				(
				BOOL trackable
				)
				: 
				m_is_trackable(trackable),
				m_lock_count(0)
#ifdef GPOS_DEBUG
				,
				m_wid(false /*fValid*/)
#endif // GPOS_DEBUG
			{}
			
			// dtor
			virtual ~CMutexBase() {}
			
			// lock/trylock/unlock
			virtual void Lock() = 0;
			virtual BOOL TryLock() = 0;
			virtual void Unlock() = 0;
					
#ifdef GPOS_DEBUG
			BOOL IsOwned() const
            {
                CWorkerId wid;
                return 0 != m_lock_count && m_wid.Equals(wid);
            }
#endif // GPOS_DEBUG

			BOOL IsTrackable() const
			{
				return m_is_trackable;
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
	template<INT mutex_type, BOOL trackable>
	class CMutexTyped : public CMutexBase
	{
		private:
		
			// lock mutex, CFA on regular intervals
			INT TimedLock()
            {
                GPOS_ASSERT_NO_SPINLOCK;

                INT ret = 0;

                // macosx does not have pthread_mutex_timedlock;
                // use a workaround with pthread_mutex_trylock and usleep;
                // this approach has increased overhead when lock contention
                // is high; it is therefore restricted to macosx
#ifdef GPOS_Darwin
                CWallClock elapsed;
                ULONG retries = 0;

                // loop until we get the lock
                do
                {
                    // attempt to lock the mutex
                    ret = pthread::MutexTryLock(&m_mutex);
                    GPOS_ASSERT(EINVAL != ret && "Invalid mutex structure");

                    // check if mutex is already locked
                    if (EBUSY == ret)
                    {
                        if (GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC <= elapsed.ElapsedMS())
                        {
                            GPOS_CHECK_ABORT;

                            elapsed.Restart();
                        }

                        ULONG interval = GPOS_MUTEX_SLEEP_SHORT_INTERVAL_USEC;
                        if (GPOS_MUTEX_SLEEP_INTERVAL_THRESHOLD < retries++)
                        {
                            interval = GPOS_MUTEX_SLEEP_LONG_INTERVAL_USEC;
                            retries = 0;
                        }

                        clib::USleep(interval);
                    }
                    else
                    {
                        GPOS_ASSERT(0 == ret && "Error occurred while trying to lock mutex");
                    }
                }
                while (0 != ret);
#else
                // loop until we get the lock
                do
                {
                    // set expiration timer
                    TIMEVAL tv;
                    syslib::GetTimeOfDay(&tv, NULL);
                    ULLONG current_us = tv.tv_sec * GPOS_USEC_IN_SEC + tv.tv_usec;
                    ULLONG expire_us = current_us + GPOS_MUTEX_CHECK_ABORT_INTERVAL_MSEC * GPOS_USEC_IN_MSEC;
                    TIMESPEC ts;
                    ts.tv_sec = (ULONG_PTR) (expire_us / GPOS_USEC_IN_SEC);
                    ts.tv_nsec = (ULONG_PTR) ((expire_us % GPOS_USEC_IN_SEC) * (GPOS_NSEC_IN_SEC / GPOS_USEC_IN_SEC));

                    ret = pthread::MutexTimedlock(&m_mutex, &ts);

                    // check if mutex is already locked
                    if (EBUSY == ret)
                    {
                        GPOS_CHECK_ABORT;
                    }
                }
                while (0 != ret);
#endif // GPOS_Darwin

                return ret;
            }

			// attempt to lock mutex
			INT AttemptLock(BOOL blocking)
            {
#ifdef GPOS_DEBUG
                GPOS_ASSERT_NO_SPINLOCK;

                // check whether we own this mutex already; if so, must be recursive mutex;
                // disallow potentially deadlocking attempts even if the lock function is only trylock
                BOOL owned_already = this->IsOwned();
                GPOS_ASSERT_IMP(owned_already, PTHREAD_MUTEX_RECURSIVE == mutex_type && "Self-deadlock detected");
#endif // GPOS_DEBUG

                INT ret = 0;

                if (blocking)
                {
                    ret = TimedLock();
                }
                else
                {
                    // attempt to lock the mutex
                    ret = pthread::MutexTryLock(&m_mutex);
                }

                if (0 != ret)
                {
                    return ret;
                };

#ifdef GPOS_DEBUG
                if (!owned_already && this->IsTrackable())
                {
                    IWorker::Self()->RegisterMutex(this);
                }

                // track owner
                m_wid.SetThreadToCurrent();
#endif // GPOS_DEBUG

                ++m_lock_count;

                return 0;
            }

		public:
		
			// ctor
			CMutexTyped<mutex_type, trackable>()
            :
            CMutexBase(trackable)
            {
                GPOS_ASSERT(
                            PTHREAD_MUTEX_DEFAULT == mutex_type ||
                            PTHREAD_MUTEX_RECURSIVE == mutex_type);

                // initialize mutex struct
                PTHREAD_MUTEX_T mutex = PTHREAD_MUTEX_INITIALIZER;
                m_mutex = mutex;

                PTHREAD_MUTEXATTR_T mutex_attr;

                // init can run out of memory
                if (0 != pthread::MutexAttrInit(&mutex_attr))
                {
                    // raise OOM exception
                    GPOS_OOM_CHECK(NULL);
                }

			// ignore return value -- all parameters have been checked already
                pthread::MutexAttrSettype(&mutex_attr, mutex_type);

                if (0 != pthread::MutexInit(&m_mutex, &mutex_attr))
                {
                    // cleanup
                    pthread::MutexAttrDestroy(&mutex_attr);

                    // out of memory/resources
                    GPOS_OOM_CHECK(NULL);
                }

			// ignore return value -- parameter already checked
                pthread::MutexAttrDestroy(&mutex_attr);
            }
					
			// dtor
			~CMutexTyped<mutex_type, trackable> ()
            {
                // disallow destruction of locked mutex
                GPOS_ASSERT(0 == m_lock_count && "Tried to destruct locked mutex.");

                // release all locks held
                while(0 < m_lock_count)
                {
                    this->Unlock();
                }

                // deallocate resources
                pthread::MutexDestroy(&m_mutex);
            }
					
			// acquire lock
			void Lock()
            {
#ifdef GPOS_DEBUG
                GPOS_ASSERT_NO_SPINLOCK;

                INT ret =
#else
			// ignore return value in optimized builds
                (void)
#endif // GPOS_DEBUG
                AttemptLock(true /*fBlocking*/);

                GPOS_ASSERT(0 == ret && "Unexpectedly failed to lock mutex");
            }
			
			// attempt locking
			BOOL TryLock()
            {
                GPOS_ASSERT_NO_SPINLOCK;

                INT ret = AttemptLock(false /*fBlocking*/);
                GPOS_ASSERT(EBUSY == ret || 0 == ret);

                return (0 == ret);
            }

			// release lock
			void Unlock()
            {
#ifdef GPOS_DEBUG
                GPOS_ASSERT_NO_SPINLOCK;

                GPOS_ASSERT(IsOwned());

                // prepare for final release of lock
                BOOL unlock = false;
                if (1 == m_lock_count)
                {
                    unlock = true;
                    m_wid.SetThreadToInvalid();
                    if (this->IsTrackable())
                    {
                        IWorker::Self()->UnregisterMutex(this);
                    }
                }
#endif // GPOS_DEBUG

                --m_lock_count;

                // ignore return values -- parameters/context have been checked already
                (void) pthread::MutexUnlock(&m_mutex);

                GPOS_ASSERT_IMP(unlock, !IsOwned());
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

