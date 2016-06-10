//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CTask.cpp
//
//	@doc:
//		Task implementation
//---------------------------------------------------------------------------

#include "gpos/error/CErrorContext.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CTask.h"
#include "gpos/task/CWorker.h"

using namespace gpos;

// init CTaskId's atomic counter
CAtomicULONG_PTR CTaskId::m_aupl(0);

const CTaskId CTaskId::m_tidInvalid;

//---------------------------------------------------------------------------
//	@function:
//		CTask::~CTask
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CTask::CTask
	(
	IMemoryPool *pmp,
	CTaskContext *ptskctxt,
	IErrorContext *perrctxt,
	CEvent *pevent,
	volatile BOOL *pfCancel
	)
	:
	m_pmp(pmp),
	m_ptskctxt(ptskctxt),
	m_perrctxt(perrctxt),
	m_perrhdl(NULL),
	m_pfunc(NULL),
	m_pvArg(NULL),
	m_pvRes(NULL),
	m_pmutex(pevent->Pmutex()),
	m_pevent(pevent),
	m_estatus(EtsInit),
	m_pfCancel(pfCancel),
	m_fCancel(false),
	m_ulAbortSuspendCount(false),
	m_fReported(false)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != ptskctxt);
	GPOS_ASSERT(NULL != perrctxt);

	if (NULL == pfCancel)
	{
		m_pfCancel = &m_fCancel;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTask::~CTask
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CTask::~CTask()
{
	GPOS_ASSERT(0 == m_ulAbortSuspendCount);

	// suspend cancellation
	CAutoSuspendAbort asa;

	GPOS_DELETE(m_ptskctxt);
	GPOS_DELETE(m_perrctxt);

	CMemoryPoolManager::Pmpm()->Destroy(m_pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CTask::Bind
//
//	@doc:
//		Bind task to function and arguments
//
//---------------------------------------------------------------------------
void 
CTask::Bind
	(
	void *(*pfunc)(void*),
	void *pvArg
	)
{
	GPOS_ASSERT(NULL != pfunc);
	
	m_pfunc = pfunc;
	m_pvArg = pvArg;
}


//---------------------------------------------------------------------------
//	@function:
//		CTask::Execute
//
//	@doc:
//		Execution of task function; wrapped in asserts to prevent leaks
//
//---------------------------------------------------------------------------
void 
CTask::Execute()
{
	GPOS_ASSERT(EtsDequeued == m_estatus);

	// final task status
	ETaskStatus ets = m_estatus;

	// check for cancel
	if (*m_pfCancel)
	{
		ets = EtsError;
	}
	else
	{
		CErrorHandlerStandard errhdl;
		GPOS_TRY_HDL(&errhdl)
		{
			// mark task as running
			SetStatus(EtsRunning);

			// call executable function
			m_pvRes = m_pfunc(m_pvArg);

#ifdef GPOS_DEBUG
			// check interval since last CFA
			GPOS_CHECK_ABORT;
#endif // GPOS_DEBUG

			// task completed
			ets = EtsCompleted;
		}
		GPOS_CATCH_EX(ex)
		{
			// not reset error context with error propagation
			ets = EtsError;
		}
		GPOS_CATCH_END;
	}
	
	// signal end of task execution
	Signal(ets);
}


//---------------------------------------------------------------------------
//	@function:
//		CTask::SetStatus
//
//	@doc:
//		Set task status;
//		Locking is required if updating more than one variable;
//
//---------------------------------------------------------------------------
void
CTask::SetStatus(ETaskStatus ets)
{
	// status changes are monotonic
	GPOS_ASSERT(ets >= m_estatus && "Invalid task status transition");

	m_estatus = ets;
}


//---------------------------------------------------------------------------
//	@function:
//  	CTask::FScheduled
//
//	@doc:
//		Check if task has been scheduled
//
//---------------------------------------------------------------------------
BOOL
CTask::FScheduled() const
{
	switch (m_estatus)
	{
		case EtsInit:
			return false;
			break;
		case EtsQueued:
		case EtsDequeued:
		case EtsRunning:
		case EtsCompleted:
		case EtsError:
			return true;
			break;
		default:
			GPOS_ASSERT(!"Invalid task status");
			return false;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTask::FFinished
//
//	@doc:
//		Check if task finished executing
//
//---------------------------------------------------------------------------
BOOL
CTask::FFinished() const
{
	switch (m_estatus)
	{
		case EtsInit:
		case EtsQueued:
		case EtsDequeued:
		case EtsRunning:
			return false;
			break;
		case EtsCompleted:
		case EtsError:
			return true;
			break;
		default:
			GPOS_ASSERT(!"Invalid task status");
			return false;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTask::Signal
//
//	@doc:
//		Signal task completion or error
//
//---------------------------------------------------------------------------
void
CTask::Signal
	(
	ETaskStatus Ets
	)
	throw()
{
	GPOS_ASSERT(FScheduled() && !FFinished() && "Invalid task status after execution");

	// scope for locking mutex
	{
		// suspend cancellation, or else the mutex may throw an Abort exception
		SuspendAbort();

		// use lock to prevent a waiting worker from missing a signal
		CAutoMutex am(*m_pmutex);
		am.Lock();

		// update task status
		SetStatus(Ets);

		m_pevent->Signal();

		// resume cancellation
		ResumeAbort();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTask::ResumeAbort
//
//	@doc:
//		Decrement counter for requests to suspend abort
//
//---------------------------------------------------------------------------
void
CTask::ResumeAbort()
{
	GPOS_ASSERT(0 < m_ulAbortSuspendCount);

	m_ulAbortSuspendCount--;

#ifdef GPOS_DEBUG
	CWorker *pwrkr = CWorker::PwrkrSelf();

	GPOS_ASSERT(NULL != pwrkr);
	pwrkr->ResetTimeSlice();
#endif
}


#ifdef GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CTask::FCheckStatus
//
//	@doc:
//		Check if task has expected status
//
//---------------------------------------------------------------------------
BOOL
CTask::FCheckStatus
	(
	BOOL fCompleted
	)
{
	GPOS_ASSERT(!FCanceled());
	if (fCompleted)
	{
		// task must have completed without an error
		return (CTask::EtsCompleted == Ets());
	}
	else
	{
		// task must still be running
		return (FScheduled() && !FFinished());
	}
}

#endif // GPOS_DEBUG

// EOF

