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
			BOOL FOwned() const;
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
			INT ITimedLock();

			// attempt to lock mutex
			INT IAttemptLock(BOOL fBlocking);

		public:
		
			// ctor
			CMutexTyped<iMutexType, fTrackable>();
					
			// dtor
			~CMutexTyped<iMutexType, fTrackable> ();
					
			// acquire lock
			void Lock();
			
			// attempt locking
			BOOL FTryLock();

			// release lock
			void Unlock();

	}; // class CMutexTyped<int iMutexType, fTrackable>
	
	
	// define publicly used types
	
	// use this only in OS abstraction layer
	typedef CMutexTyped<PTHREAD_MUTEX_DEFAULT, false /*fTrackable*/> CMutexOS;
	typedef CMutexTyped<PTHREAD_MUTEX_RECURSIVE, false /*fTrackable*/> CMutexRecursiveOS;
	
	// use these in regular code where worker context is always available
	typedef CMutexTyped<PTHREAD_MUTEX_DEFAULT, true /*fTrackable*/> CMutex;
	typedef CMutexTyped<PTHREAD_MUTEX_RECURSIVE, true /*fTrackable*/> CMutexRecursive;
}


// include implementation
#include "CMutex.inl"

#endif // !GPOS_CMutex_H

// EOF

