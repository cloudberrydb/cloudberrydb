//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CAutoTaskProxy.cpp
//
//	@doc:
//		Implementation of interface class for task management and execution.
//---------------------------------------------------------------------------

#include "gpos/common/CAutoP.h"
#include "gpos/common/CWallClock.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CAutoTaskProxy.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::CAutoTaskProxy
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CAutoTaskProxy::CAutoTaskProxy
	(
	IMemoryPool *pmp,
	CWorkerPoolManager *pwpm,
	BOOL fPropagateError
	)
	:
	m_pmp(pmp),
	m_pwpm(pwpm),
	m_fPropagateError(fPropagateError)
{
	m_list.Init(GPOS_OFFSET(CTask, m_linkAtp));
	m_event.Init(&m_mutex);

	// register new ATP to worker pool
	m_pwpm->AtpAddRef();

}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::~CAutoTaskProxy
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CAutoTaskProxy::~CAutoTaskProxy()
{
	// suspend cancellation - destructors should not throw
	CAutoSuspendAbort asa;

	// disable error propagation from sub-task
	SetPropagateError(false);

	// destroy all tasks
	DestroyAll();

	// remove ATP from worker pool
	m_pwpm->AtpRemoveRef();
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::DestroyAll
//
//	@doc:
//		Unregister and release all tasks
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::DestroyAll()
{
	// iterate task list
	while (!m_list.FEmpty())
	{
		Destroy(m_list.PtFirst());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Destroy
//
//	@doc:
//		Unregister and release task
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Destroy
	(
	CTask *ptsk
	)
{
	GPOS_ASSERT(FOwnerOf(ptsk) && "Task not owned by this ATP object");

	// cancel scheduled task
	if (ptsk->FScheduled() && !ptsk->FReported())
	{
		Cancel(ptsk);
		Wait(ptsk);
	}

	// unregister task from worker pool
	m_pwpm->PtskRemoveTask(ptsk->Tid());

	// remove task from list
	m_list.Remove(ptsk);

	// delete task object
	GPOS_DELETE(ptsk);
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::PtskCreate
//
//	@doc:
//		Create new task;
//		Bind task to function and argument and associate with task and error context;
//		If caller is a task, its task context is cloned and used by the new task;
//
//---------------------------------------------------------------------------
CTask *
CAutoTaskProxy::PtskCreate
	(
	void *(*pfunc)(void*),
	void *pvArg,
	volatile BOOL *pfCancel
	)
{
	// create memory pool for task
	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *pmp = amp.Pmp();

	// auto pointer to hold new task context
	CAutoP<CTaskContext> aptc;

	// check if caller is a task
	ITask *ptskParent = CWorker::PwrkrSelf()->Ptsk();
	if (NULL == ptskParent)
	{
		// create new task context
		aptc = GPOS_NEW(pmp) CTaskContext(pmp);
	}
	else
	{
		// clone parent task's context
		aptc = GPOS_NEW(pmp) CTaskContext(pmp, *ptskParent->Ptskctxt());
	}

	// auto pointer to hold error context
	CAutoP<CErrorContext> apec;
	apec = GPOS_NEW(pmp) CErrorContext();
	CTask *ptsk = CTask::PtskSelf();
	if (NULL != ptsk)
	{
		apec.Pt()->Register(ptsk->PerrctxtConvert()->Pmdr());
	}

	// auto pointer to hold new task
	// task is created inside ATP's memory pool
	CAutoP<CTask> apt;
	apt = GPOS_NEW(m_pmp) CTask(pmp, aptc.Pt(), apec.Pt(), &m_event, pfCancel);

	// reset auto pointers - task now handles task and error context
	(void) aptc.PtReset();
	(void) apec.PtReset();

	// detach task's memory pool from auto memory pool
	amp.PmpDetach();

	// bind function and argument
	ptsk = apt.Pt();
	ptsk->Bind(pfunc, pvArg);

	// add to task list
	m_list.Append(ptsk);

	// reset auto pointer - ATP now handles task
	apt.PtReset();

	// register task to worker pool
	m_pwpm->RegisterTask(ptsk);

	return ptsk;
}



//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Schedule
//
//	@doc:
//		Schedule task for execution
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Schedule
	(
	CTask *ptsk
	)
{
	GPOS_ASSERT(FOwnerOf(ptsk) && "Task not owned by this ATP object");
	GPOS_ASSERT(CTask::EtsInit == ptsk->m_estatus && "Task already scheduled");

	m_pwpm->Schedule(ptsk);
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Wait
//
//	@doc:
//		Wait for task to complete
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Wait
	(
	CTask *ptsk
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	GPOS_ASSERT(FOwnerOf(ptsk) && "Task not owned by this ATP object");
	GPOS_ASSERT(ptsk->FScheduled() && "Task not scheduled yet");
	GPOS_ASSERT(!ptsk->FReported() && "Task already reported as completed");

	// wait until task finishes
	while (!ptsk->FFinished())
	{
		m_event.Wait();
	}

	// mark task as reported
	ptsk->SetReported();

	// check error from sub-task
	CheckError(ptsk);
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::EresTimedWait
//
//	@doc:
//		Wait for task to complete - with timeout;
//		Returns GPOS_OK if task is found or GPOS_TIMEOUT if timeout expires;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxy::EresTimedWait
	(
	CTask *ptsk,
	ULONG ulTimeoutMs
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	GPOS_ASSERT(FOwnerOf(ptsk) && "Task not owned by this ATP object");
	GPOS_ASSERT(ptsk->FScheduled() && "Task not scheduled yet");
	GPOS_ASSERT(!ptsk->FReported() && "Task already reported as completed");

	CWallClock clock;
	ULONG ulElapsedMs = 0;

	// wait until task finishes or timeout expires
	while (!ptsk->FFinished() && (ulElapsedMs = clock.UlElapsedMS()) < ulTimeoutMs)
	{
		m_event.EresTimedWait(ulTimeoutMs - ulElapsedMs);
	}

	// check if timeout expired
	if (!ptsk->FFinished())
	{
		return GPOS_TIMEOUT;
	}

	// mark task as reported
	ptsk->SetReported();

	// check error from sub-task
	CheckError(ptsk);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::WaitAny
//
//	@doc:
//		Wait until at least one task completes
//
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::WaitAny
	(
	CTask **pptsk
	)
{
	GPOS_ASSERT(!m_list.FEmpty() && "ATP owns no task");

	*pptsk = NULL;

	CAutoMutex am(m_mutex);
	am.Lock();

	// check if any task has completed so far
	if (GPOS_OK != EresFindFinished(pptsk))
	{
		// wait for next task to complete
		m_event.Wait();

		// find completed task
#ifdef GPOS_DEBUG
		GPOS_RESULT eresFound =
#endif // GPOS_DEBUG
		EresFindFinished(pptsk);

		GPOS_ASSERT(GPOS_OK == eresFound);
	}

	GPOS_ASSERT(NULL != *pptsk);

	// check error from sub-task
	CheckError(*pptsk);
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::EresTimedWaitAny
//
//	@doc:
//		Wait until at least one task completes - with timeout;
//		Returns GPOS_OK if task is found or GPOS_TIMEOUT if timeout expires;
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxy::EresTimedWaitAny
	(
	CTask **pptsk,
	ULONG ulTimeoutMs
	)
{
	GPOS_ASSERT(!m_list.FEmpty() && "ATP owns no task");

	*pptsk = NULL;

	CAutoMutex am(m_mutex);
	am.Lock();

	// check if any task has completed so far
	if (GPOS_OK != EresFindFinished(pptsk))
	{
		// wait for next task to complete - with timeout
		GPOS_RESULT eresTimed = m_event.EresTimedWait(ulTimeoutMs);

		// check if timeout not expired
		if (GPOS_OK == eresTimed)
		{
#ifdef GPOS_DEBUG
			GPOS_RESULT eresFound =
#endif // GPOS_DEBUG
			EresFindFinished(pptsk);

			GPOS_ASSERT(GPOS_OK == eresFound);
		}
		else
		{
			// timeout expired, no task completed
			GPOS_ASSERT(GPOS_TIMEOUT == eresTimed);
			return GPOS_TIMEOUT;
		}
	}

	GPOS_ASSERT(NULL != *pptsk);

	// check error from sub-task
	CheckError(*pptsk);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::EresFindFinished
//
//	@doc:
//		Find finished task
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxy::EresFindFinished
	(
	CTask **pptsk
	)
{
	*pptsk = NULL;

#ifdef GPOS_DEBUG
	// check if there is any task scheduled
	BOOL fScheduled = false;

	// check if all tasks have been reported as finished
	BOOL fReportedAll = true;
#endif // GPOS_DEBUG

	// iterate task list
	for (CTask *ptsk = m_list.PtFirst();
		 NULL != ptsk;
		 ptsk = m_list.PtNext(ptsk))
	{
#ifdef GPOS_DEBUG
		// check if task has been scheduled
		if (ptsk->FScheduled())
		{
			fScheduled = true;
		}
#endif // GPOS_DEBUG

		// check if task has been reported as finished
		if (!ptsk->FReported())
		{
#ifdef GPOS_DEBUG
			fReportedAll = false;
#endif // GPOS_DEBUG

			// check if task is finished
			if (ptsk->FFinished())
			{
				// mark task as reported
				ptsk->SetReported();
				*pptsk = ptsk;

				return GPOS_OK;
			}
		}
	}

	GPOS_ASSERT(fScheduled && "No task scheduled yet");
	GPOS_ASSERT(!fReportedAll && "All tasks have been reported as finished");

	return GPOS_NOT_FOUND;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Execute
//
//	@doc:
//		Execute task in thread owning ATP (synchronous execution);
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Execute
	(
	CTask *ptsk
	)
{
	GPOS_ASSERT(FOwnerOf(ptsk) && "Task not owned by this ATP object");
	GPOS_ASSERT(CTask::EtsInit == ptsk->m_estatus && "Task already scheduled");

	// mark task as ready to execute
	ptsk->SetStatus(CTask::EtsDequeued);

	GPOS_TRY
	{
		// get worker of current thread
		CWorker *pwrkr = CWorker::PwrkrSelf();
		GPOS_ASSERT(NULL != pwrkr);

		// execute task
		pwrkr->Execute(ptsk);
	}
	GPOS_CATCH_EX(ex)
	{
		// mark task as erroneous
		ptsk->SetStatus(CTask::EtsError);

		if (m_fPropagateError)
		{
			GPOS_RETHROW(ex);
		}
	}
	GPOS_CATCH_END;

	// Raise exception if task encounters an exception
	if (ptsk->FPendingExc())
	{
		if (m_fPropagateError)
		{
			GPOS_RETHROW(ptsk->Perrctxt()->Exc());
		}
		else
		{
			ptsk->Perrctxt()->Reset();
		}
	}

	// mark task as reported
	ptsk->SetReported();
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::Cancel
//
//	@doc:
//		Cancel task
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::Cancel
	(
	CTask *ptsk
	)
{
	if (!ptsk->FFinished())
	{
		m_pwpm->Cancel(ptsk->Tid());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::CheckError
//
//	@doc:
//		Check error from sub-task
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::CheckError
	(
	CTask *ptskSub
	)
{
	// sub-task has a pending error
	if (ptskSub->FPendingExc())
	{
		// must be in error status
		GPOS_ASSERT(ITask::EtsError == ptskSub->Ets());

		if (m_fPropagateError)
		{
			// propagate error from sub task to current task
			PropagateError(ptskSub);
		}
		else
		{
			// ignore the pending error from sub task
			// and reset its error context
			ptskSub->Perrctxt()->Reset();
		}
	}
#ifdef GPOS_DEBUG
	else if (ITask::EtsError == ptskSub->Ets())
	{
		// sub-task was canceled without a pending error
		GPOS_ASSERT(!ptskSub->FPendingExc() && ptskSub->FCanceled());
	}
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::PropagateError
//
//	@doc:
//		Propagate the error from sub task to current task
//
//---------------------------------------------------------------------------
void
CAutoTaskProxy::PropagateError
	(
		CTask *ptskSub
	)
{
	GPOS_ASSERT(m_fPropagateError);

	// sub-task must be in error status and have a pending exception
	GPOS_ASSERT(ITask::EtsError == ptskSub->Ets() && ptskSub->FPendingExc());

	CTask *ptskCur = CTask::PtskSelf();

	// current task must have no pending error
	GPOS_ASSERT(NULL != ptskCur && !ptskCur->FPendingExc());

	IErrorContext *perrctxtCur = ptskCur->Perrctxt();

	// copy necessary error info for propagation
	perrctxtCur->CopyPropErrCtxt(ptskSub->Perrctxt());

	// reset error of sub task
	ptskSub->Perrctxt()->Reset();

	// propagate the error
	CException::Reraise(perrctxtCur->Exc(), true /*fPropagate*/);
}


#ifdef GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxy::FOwnerOf
//
//	@doc:
//		Check task owner
//
//---------------------------------------------------------------------------
BOOL
CAutoTaskProxy::FOwnerOf(CTask *ptsk)
{
	CWorkerId wid;
	GPOS_ASSERT(NULL != ptsk);
	GPOS_ASSERT(wid == m_widParent &&
			   "Only ATP owner can schedule and wait for task");
	return (GPOS_OK == m_list.EresFind(ptsk));
}

#endif // GPOS_DEBUG

// EOF

