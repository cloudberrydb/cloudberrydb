//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CWorker.h
//
//	@doc:
//		Abstraction of schedule-able unit, e.g. a pthread etc.
//---------------------------------------------------------------------------
#ifndef GPOS_CWorker_H
#define GPOS_CWorker_H

#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/common/CTimerUser.h"
#include "gpos/sync/CSpinlock.h"
#include "gpos/sync/CMutex.h"
#include "gpos/task/CTask.h"
#include "gpos/task/IWorker.h"

namespace gpos
{

	class CTask;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CWorker
	//
	//	@doc:
	//		Worker abstraction keeps track of resource held by worker; management
	//		of control flow such as abort signal etc.
	//
	//---------------------------------------------------------------------------

	class CWorker : public IWorker
	{	
		friend class CThreadManager;
		friend class CAutoTaskProxy;
		
		private:

			// current task
			CTask *m_ptsk;

			// thread id
			ULONG m_ulThreadId;

			// available stack
			ULONG m_cStackSize;

			// start address of current thread's stack
			const ULONG_PTR m_ulpStackStart;

#ifdef GPOS_DEBUG
			// currently owned spinlocks
			CList<CSpinlockBase> m_listSlock;
			
			// currently owned mutexes
			CList<CMutexBase> m_listMutex;

			// stack descriptor for last abort checkpoint
			CStackDescriptor m_sdLastCA;

			// timer for measuring intervals between abort checkpoints
			CTimerUser m_timerLastCA;

			// check if interval since last abort checkpoint exceeds maximum
			void CheckTimeSlice();

#endif // GPOS_DEBUG

			// execute tasks iteratively
			void Run();

			// execute single task
			void Execute(CTask *ptsk);

			// check for abort request
			void CheckForAbort(const CHAR *szFile, ULONG cLine);

#ifdef GPOS_FPSIMULATOR
			// simulate abort request, log abort injection
			void SimulateAbort(const CHAR *szFile, ULONG ulLine);
#endif // GPOS_FPSIMULATOR

			// no copy ctor
			CWorker(const CWorker&);

		public:
		
			// ctor
			CWorker(ULONG ulThreadId, ULONG cStackSize, ULONG_PTR ulpStackStart);

			// dtor
			virtual ~CWorker();

			// thread identification
			ULONG UlThreadId() const
			{
				return m_ulThreadId;
			}

			// worker identification
			inline
			CWorkerId Wid() const
			{
				return m_wid;
			}

			// stack start accessor
			inline
			ULONG_PTR UlpStackStart() const
			{
				return m_ulpStackStart;
			}

#ifdef GPOS_DEBUG
			BOOL FCanAcquireSpinlock(const CSpinlockBase *pslock) const;
			BOOL FOwnsSpinlocks() const;
			
			void RegisterSpinlock(CSpinlockBase *pslock);
			void UnregisterSpinlock(CSpinlockBase *pslock);				

			BOOL FOwnsMutexes() const;
			void RegisterMutex(CMutexBase *pmutex);
			void UnregisterMutex(CMutexBase *pmutex);

			// reset abort-related stack descriptor and timer
			void ResetTimeSlice();

#endif // GPOS_DEBUG

			// stack check
			BOOL FCheckStackSize(ULONG ulRequest = 0) const;

			// accessor
			inline
			CTask *Ptsk()
			{
				return m_ptsk;
			}

			// slink for hashtable
			SLink m_link;

			// identification; public in order to be used as key in HTs
			CWorkerId m_wid;

			// lookup worker in worker pool manager
			static CWorker *PwrkrSelf()
			{
				return dynamic_cast<CWorker*>(IWorker::PwrkrSelf());
			}

			// host system callback function to report abort requests
			static bool (*pfnAbortRequestedBySystem) (void);

	}; // class CWorker
}

#endif // !GPOS_CWorker_H

// EOF

