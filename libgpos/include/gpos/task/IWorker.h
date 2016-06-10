//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		IWorker.h
//
//	@doc:
//		Interface class to worker; broken into interface and implementation
//		to avoid cyclic dependencies between worker, list, spinlock etc.
//		The Worker abstraction contains only components needed to schedule,
//		execute, and abort tasks; no task specific configuration such as
//		output streams is contained in Worker;
//---------------------------------------------------------------------------
#ifndef GPOS_IWorker_H
#define GPOS_IWorker_H

#include "gpos/types.h"
#include "gpos/common/CStackObject.h"

#define GPOS_CHECK_ABORT	(IWorker::CheckAbort(__FILE__, __LINE__))

#define GPOS_CHECK_STACK_SIZE \
	do \
	{ \
		if (NULL != IWorker::PwrkrSelf()) \
		{ \
			(void) IWorker::PwrkrSelf()->FCheckStackSize(); \
		} \
	} \
	while (0)


// assert no spinlock
#define GPOS_ASSERT_NO_SPINLOCK  \
		GPOS_ASSERT_IMP \
		( \
			NULL != IWorker::PwrkrSelf(), \
			(!IWorker::PwrkrSelf()->FOwnsSpinlocks()) && "Must not hold a spinlock!" \
		)


#if (GPOS_SunOS)
#define GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC   (ULONG(2000))
#else
#define GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC   (ULONG(1500))
#endif

namespace gpos
{
	// prototypes
	class CSpinlockBase;
	class CMutexBase;
	class ITask;
	class CWorkerId;

	//---------------------------------------------------------------------------
	//	@class:
	//		IWorker
	//
	//	@doc:
	//		Interface to abstract scheduling primitive such as threads;
	//
	//---------------------------------------------------------------------------
	class IWorker : public CStackObject
	{
	
		private:

			// hidden copy ctor
			IWorker(const IWorker&);

			// check for abort request
			virtual void CheckForAbort(const CHAR *, ULONG) = 0;

		public:
		
			// dummy ctor
			IWorker () {}

			// dummy dtor
			virtual ~IWorker() {}

			// accessors
			virtual ULONG UlThreadId() const = 0;
			virtual CWorkerId Wid() const = 0;
			virtual ULONG_PTR UlpStackStart() const = 0;
			virtual ITask *Ptsk() = 0;

			// stack check
			virtual BOOL FCheckStackSize(ULONG ulRequest = 0) const = 0;
			
#ifdef GPOS_DEBUG
			virtual BOOL FCanAcquireSpinlock(const CSpinlockBase*) const = 0;
			virtual BOOL FOwnsSpinlocks() const = 0;
			virtual void RegisterSpinlock(CSpinlockBase *) = 0;
			virtual void UnregisterSpinlock(CSpinlockBase*) = 0;
			
			virtual BOOL FOwnsMutexes() const = 0;
			virtual void RegisterMutex(CMutexBase *) = 0;
			virtual void UnregisterMutex(CMutexBase *) = 0;

			static BOOL m_fEnforceTimeSlices;
#endif // GPOS_DEBUG

			// lookup worker in worker pool manager
			static IWorker *PwrkrSelf();

			// check for aborts
			static void CheckAbort(const CHAR *szFile, ULONG cLine);
		
	}; // class IWorker
}

#endif // !GPOS_IWorker_H

// EOF

