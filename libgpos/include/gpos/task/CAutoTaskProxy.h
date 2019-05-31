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
			CMemoryPool *m_mp;

			// worker pool
			CWorkerPoolManager *m_pwpm;

			// task list
			CList<CTask> m_list;

			// mutex for event
			CMutex m_mutex;

			// event to signal task completion, shared by all tasks
			CEvent m_event;

			// propagate error of sub-task or not
			BOOL m_propagate_error;

			// find finished task
			GPOS_RESULT
			FindFinished(CTask **task);

			// no copy ctor
			CAutoTaskProxy(const CAutoTaskProxy&);

#ifdef GPOS_DEBUG
			// id of worker hosting ATP
			CWorkerId m_wid_parent;

			// check task owner
			BOOL OwnerOf(CTask *task);
#endif // GPOS_DEBUG

			// propagate the error from sub-task to current task
			void PropagateError(CTask *sub_task);

			// check error from sub-task
			void CheckError(CTask *sub_task);

		public:

			// ctor
			CAutoTaskProxy
				(
				CMemoryPool *mp,
				CWorkerPoolManager *m_pwpm,
				BOOL propagate_error = true
				);

			// dtor
			~CAutoTaskProxy();

			// task count
			ULONG TaskCount()
			{
				return m_list.Size();
			}

			// disable/enable error propagation
			void SetPropagateError(BOOL propagate_error)
			{
				m_propagate_error = propagate_error;
			}

			// create new task
			CTask *Create(void *(*pfunc)(void*), void *argv, volatile BOOL *cancel = NULL);

			// schedule task for execution
			void Schedule(CTask *task);

			// wait for task to complete
			void Wait(CTask *task);

			// wait for task to complete - with timeout_ms
			GPOS_RESULT TimedWait(CTask *task, ULONG timeout_ms);

			// wait until at least one task completes
			void WaitAny(CTask **ptask);

			// wait until at least one task completes - with timeout_ms
			GPOS_RESULT TimedWaitAny(CTask **ptask, ULONG timeout_ms);

			// execute task in thread owning ATP (synchronous execution)
			void Execute(CTask *task);

			// cancel task
			void Cancel(CTask *task);

			// unregister and release task
			void Destroy(CTask *task);

			// unregister and release all tasks
			void DestroyAll();

	}; // class CAutoTaskProxy

}

#endif // !GPOS_CAutoTaskProxy_H_

// EOF

