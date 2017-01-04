//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CAutoTaskProxy.h
//
//	@doc:
//		Interface class for task management and execution.
//---------------------------------------------------------------------------

#ifndef GPOS_CAutoTaskProxy_H
#define GPOS_CAutoTaskProxy_H

#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/sync/CEvent.h"
#include "gpos/sync/CMutex.h"
#include "gpos/task/CTask.h"
#include "gpos/task/CTaskContext.h"
#include "gpos/task/CWorkerPoolManager.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoTaskProxy
	//
	//	@doc:
	//		Auto task proxy (ATP) to handle task creation, execution and cleanup
	//		ATP operations are not thread-safe; only one worker can use each ATP
	//		object.
	//
	//---------------------------------------------------------------------------
	class CAutoTaskProxy : CStackObject
	{
		private:

			// memory pool
			IMemoryPool *m_pmp;

			// worker pool
			CWorkerPoolManager *m_pwpm;

			// task list
			CList<CTask> m_list;

			// mutex for event
			CMutex m_mutex;

			// event to signal task completion, shared by all tasks
			CEvent m_event;

			// propagate error of sub-task or not
			BOOL m_fPropagateError;

			// find finished task;
			GPOS_RESULT
			EresFindFinished(CTask **pptsk);

			// no copy ctor
			CAutoTaskProxy(const CAutoTaskProxy&);

#ifdef GPOS_DEBUG
			// id of worker hosting ATP
			CWorkerId m_widParent;

			// check task owner
			BOOL FOwnerOf(CTask *ptsk);
#endif // GPOS_DEBUG

			// propagate the error from sub-task to current task
			void PropagateError(CTask *ptskSub);

			// check error from sub-task
			void CheckError(CTask *ptskSub);

		public:

			// ctor
			CAutoTaskProxy
				(
				IMemoryPool *pmp,
				CWorkerPoolManager *m_pwpm,
				BOOL fPropagateError = true
				);

			// dtor
			~CAutoTaskProxy();

			// task count
			ULONG UlTasks()
			{
				return m_list.UlSize();
			}

			// disable/enable error propagation
			void SetPropagateError(BOOL fPropagateError)
			{
				m_fPropagateError = fPropagateError;
			}

			// create new task
			CTask *PtskCreate(void *(*pfunc)(void*), void *pvArg, volatile BOOL *pfCancel = NULL);

			// schedule task for execution
			void Schedule(CTask *ptsk);

			// wait for task to complete
			void Wait(CTask *ptsk);

			// wait for task to complete - with timeout
			GPOS_RESULT EresTimedWait(CTask *ptsk, ULONG ulTimeoutMs);

			// wait until at least one task completes
			void WaitAny(CTask **pptsk);

			// wait until at least one task completes - with timeout
			GPOS_RESULT EresTimedWaitAny(CTask **pptsk, ULONG ulTimeoutMs);

			// execute task in thread owning ATP (synchronous execution)
			void Execute(CTask *ptsk);

			// cancel task
			void Cancel(CTask *ptsk);

			// unregister and release task
			void Destroy(CTask *ptsk);

			// unregister and release all tasks
			void DestroyAll();

	}; // class CAutoTaskProxy

}

#endif // !GPOS_CAutoTaskProxy_H_

// EOF

