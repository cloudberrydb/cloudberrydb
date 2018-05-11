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
			CTask *m_task;

			// thread id
			ULONG m_thread_id;

			// available stack
			ULONG m_stack_size;

			// start address of current thread's stack
			const ULONG_PTR m_stack_start;

#ifdef GPOS_DEBUG
			// currently owned spinlocks
			CList<CSpinlockBase> m_spin_lock_list;
			
			// currently owned mutexes
			CList<CMutexBase> m_mutex_list;

			// stack descriptor for last abort checkpoint
			CStackDescriptor m_last_ca;

			// timer for measuring intervals between abort checkpoints
			CTimerUser m_timer_last_ca;

			// check if interval since last abort checkpoint exceeds maximum
			void CheckTimeSlice();

#endif // GPOS_DEBUG

			// execute tasks iteratively
			void Run();

			// execute single task
			void Execute(CTask *task);

			// check for abort request
			void CheckForAbort(const CHAR *file, ULONG line_num);

#ifdef GPOS_FPSIMULATOR
			// simulate abort request, log abort injection
			void SimulateAbort(const CHAR *file, ULONG line_num);
#endif // GPOS_FPSIMULATOR

			// no copy ctor
			CWorker(const CWorker&);

		public:
		
			// ctor
			CWorker(ULONG thread_id, ULONG stack_size, ULONG_PTR stack_start);

			// dtor
			virtual ~CWorker();

			// thread identification
			ULONG GetThreadId() const
			{
				return m_thread_id;
			}

			// worker identification
			inline
			CWorkerId GetWid() const
			{
				return m_wid;
			}

			// stack start accessor
			inline
			ULONG_PTR GetStackStart() const
			{
				return m_stack_start;
			}

#ifdef GPOS_DEBUG
			BOOL CanAcquireSpinlock(const CSpinlockBase *slock) const;
			BOOL OwnsSpinlocks() const;
			
			void RegisterSpinlock(CSpinlockBase *slock);
			void UnregisterSpinlock(CSpinlockBase *slock);

			BOOL OwnsMutexes() const;
			void RegisterMutex(CMutexBase *mutex);
			void UnregisterMutex(CMutexBase *mutex);

			// reset abort-related stack descriptor and timer
			void ResetTimeSlice();

#endif // GPOS_DEBUG

			// stack check
			BOOL CheckStackSize(ULONG request = 0) const;

			// accessor
			inline
			CTask *GetTask()
			{
				return m_task;
			}

			// slink for hashtable
			SLink m_link;

			// identification; public in order to be used as key in HTs
			CWorkerId m_wid;

			// lookup worker in worker pool manager
			static CWorker *Self()
			{
				return dynamic_cast<CWorker*>(IWorker::Self());
			}

			// host system callback function to report abort requests
			static bool (*abort_requested_by_system) (void);

	}; // class CWorker
}

#endif // !GPOS_CWorker_H

// EOF

