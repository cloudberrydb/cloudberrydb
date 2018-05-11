//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CWorker.cpp
//
//	@doc:
//		Worker abstraction, e.g. thread
//---------------------------------------------------------------------------

#include "gpos/common/syslibwrapper.h"
#include "gpos/error/CFSimulator.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/task/CWorker.h"
#include "gpos/task/CWorkerPoolManager.h"

using namespace gpos;

// host system callback function to report abort requests
bool (*CWorker::abort_requested_by_system) (void);


//---------------------------------------------------------------------------
//	@function:
//		CWorker::CWorker
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CWorker::CWorker
	(
	ULONG thread_id,
	ULONG stack_size,
	ULONG_PTR stack_start
	)
	:
	m_task(NULL),
	m_thread_id(thread_id),
	m_stack_size(stack_size),
	m_stack_start(stack_start)
{
#ifdef GPOS_DEBUG			
	m_spin_lock_list.Init(GPOS_OFFSET(CSpinlockBase, m_link));
	m_mutex_list.Init(GPOS_OFFSET(CMutexBase, m_link));
#endif // GPOS_DEBUG

	GPOS_ASSERT(stack_size >= 2 * 1024 && "Worker has to have at least 2KB stack");

	// register worker
	GPOS_ASSERT(NULL == Self() && "Found registered worker!");
	
	CWorkerPoolManager::WorkerPoolManager()->RegisterWorker(this);
	GPOS_ASSERT(this == CWorkerPoolManager::WorkerPoolManager()->Self());

#ifdef GPOS_DEBUG
	ResetTimeSlice();
#endif // GPOS_DEBUG
}

			
//---------------------------------------------------------------------------
//	@function:
//		CWorker::~CWorker
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CWorker::~CWorker()
{
	// since resources are being tracked in the worker releasing the locks is not an
	// option: release/unlock require the worker to be in tact -- given that we're in
	// the destructor we must assume the worst
	//
	// CONSIDER: 03/27/2008; if this is ever a problem on a production system,
	// introduce emergency release/unlock function which suspends tracking; make 
	// tracking available in optimized builds
	GPOS_ASSERT(!OwnsSpinlocks() && "Cannot destruct worker while holding a spinlock.");
	GPOS_ASSERT(!OwnsMutexes() && "Cannot destruct worker while holding a mutex.");
	
	// unregister worker
	GPOS_ASSERT(this == Self() && "Unidentified worker found.");
	CWorkerPoolManager::WorkerPoolManager()->RemoveWorker(m_wid);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::Run
//
//	@doc:
//		Execute tasks iteratively
//
//---------------------------------------------------------------------------
void
CWorker::Run()
{
	GPOS_ASSERT(NULL == m_task);

	CTask *task = NULL;

	while (CWorkerPoolManager::EsrExecTask == CWorkerPoolManager::WorkerPoolManager()->RespondToNextTaskRequest(&task))
	{
		Execute(task);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::Execute
//
//	@doc:
//		Execute single task
//
//---------------------------------------------------------------------------
void
CWorker::Execute(CTask *task)
{
	GPOS_ASSERT(task);
	GPOS_ASSERT(NULL == m_task && "Another task is assigned to worker");

#ifdef GPOS_DEBUG
	ResetTimeSlice();
#endif // GPOS_DEBUG

	m_task = task;
	GPOS_TRY
	{
		m_task->Execute();
		m_task = NULL;
	}
	GPOS_CATCH_EX(ex)
	{
		m_task = NULL;
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::CheckForAbort
//
//	@doc:
//		Check pending abort flag; throw if abort is flagged
//
//---------------------------------------------------------------------------
void
CWorker::CheckForAbort
	(
#ifdef GPOS_FPSIMULATOR
	const CHAR *file,
	ULONG line_num
#else
	const CHAR *,
	ULONG
#endif // GPOS_FPSIMULATOR
	)
{
	// check if there is a task assigned to worker,
	// task is still running and CFA is not suspended
	if (NULL != m_task && m_task->IsRunning() && !m_task->IsAbortSuspended())
	{
		GPOS_ASSERT(!m_task->GetErrCtxt()->IsPending() &&
		            "Check-For-Abort while an exception is pending");

#ifdef GPOS_FPSIMULATOR
		SimulateAbort(file, line_num);
#endif // GPOS_FPSIMULATOR

		if ((NULL != abort_requested_by_system && abort_requested_by_system()) ||
			m_task->IsCanceled())
		{
			// raise exception
			GPOS_ABORT;
		}

#ifdef GPOS_DEBUG
		// enforce regular checks for abort
		CheckTimeSlice();
		ResetTimeSlice();
#endif
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::CheckStackSize
//
//	@doc:
//		Size of stack within context of this worker;
//		effectively calculates distance of local variable to stack start;
//		if stack space is exhausted we throw an exception;
//		else we check if requested space can fit in stack
//
//---------------------------------------------------------------------------
BOOL
CWorker::CheckStackSize(ULONG request) const
{
	ULONG_PTR ptr = 0;
	
	// get current stack size
	ULONG_PTR size = m_stack_start - (ULONG_PTR)&ptr;
	
	// check if we have exceeded stack space
	if (size >= m_stack_size)
	{
		// raise stack overflow exception
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiOutOfStack);
	}

	// check if there is enough stack space for request
	if (size + request >= m_stack_size)
	{
		return false;
	}
	return true;
}


#ifdef GPOS_FPSIMULATOR

//---------------------------------------------------------------------------
//	@function:
//		CWorker::SimulateAbort
//
//	@doc:
//		Simulate abort request, log abort injection
//
//---------------------------------------------------------------------------
void
CWorker::SimulateAbort
	(
	const CHAR *file,
	ULONG line_num
	)
{
	if (m_task->IsTraceSet(EtraceSimulateAbort) &&
		CFSimulator::FSim()->NewStack(CException::ExmaSystem, CException::ExmiAbort))
	{
		// GPOS_TRACE has CFA, disable simulation temporarily
		m_task->SetTrace(EtraceSimulateAbort, false);

		GPOS_TRACE_FORMAT_ERR("Simulating Abort at %s:%d", file, line_num);

		// resume simulation
		m_task->SetTrace(EtraceSimulateAbort, true);

		m_task->Cancel();
	}
}

#endif // GPOS_FPSIMULATOR


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CWorker::ResetCAInfo
//
//	@doc:
//		Reset abort-related stack descriptor and timer
//
//---------------------------------------------------------------------------
void
CWorker::ResetTimeSlice()
{
	if (IWorker::m_enforce_time_slices)
	{
		m_last_ca.BackTrace();
		m_timer_last_ca.Restart();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CWorker::CheckTimeSlice
//
//	@doc:
//		Check if interval since last abort checkpoint exceeds maximum
//
//---------------------------------------------------------------------------
void
CWorker::CheckTimeSlice()
{
	if (IWorker::m_enforce_time_slices)
	{
		ULONG interval = m_timer_last_ca.ElapsedMS();
		
		ULONG threads = std::max((ULONG) 1, CWorkerPoolManager::WorkerPoolManager()->GetNumWorkersRunning());
		
		if (GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC * threads <= interval && m_task->IsRunning())
		{
			// get stack trace for last checkpoint
			WCHAR buffer[GPOS_STACK_TRACE_BUFFER_SIZE];
			CWStringStatic str(buffer, GPOS_ARRAY_SIZE(buffer));
			m_last_ca.AppendTrace(&str);

			ResetTimeSlice();
			GPOS_RAISE
				(
				CException::ExmaSystem,
				CException::ExmiAbortTimeout,
				interval,
				str.GetBuffer()
				);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::OwnsSpinlocks
//
//	@doc:
//		Check whether any sync objects of a given type are currently owned
//		by this worker
//
//---------------------------------------------------------------------------
BOOL
CWorker::OwnsSpinlocks() const
{
	return !m_spin_lock_list.IsEmpty();
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::CanAcquireSpinlock
//
//	@doc:
//		Check if we can legally acquire a given spinlock
//
//---------------------------------------------------------------------------
BOOL
CWorker::CanAcquireSpinlock
	(
	const CSpinlockBase *slock
	)
	const
{
	GPOS_ASSERT(NULL != slock);

	CSpinlockBase *slock_last = m_spin_lock_list.First();
	return (NULL == slock_last) || (slock_last->Rank() >= slock->Rank());
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::RegisterSpinlock
//
//	@doc:
//		add spinlock to the list of spinlocks already held; check ranking
//
//---------------------------------------------------------------------------
void
CWorker::RegisterSpinlock
	(
	CSpinlockBase *slock
	)
{
	// make sure this one can actually be acquired
	GPOS_ASSERT(CanAcquireSpinlock(slock));

	m_spin_lock_list.Prepend(slock);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::UnregisterSpinlock
//
//	@doc:
//		remove spinlock from list
//
//---------------------------------------------------------------------------
void
CWorker::UnregisterSpinlock
	(
	CSpinlockBase *slock
	)
{
	m_spin_lock_list.Remove(slock);
	
	// must be able to re-acquire now
	GPOS_ASSERT(CanAcquireSpinlock(slock));
}



//---------------------------------------------------------------------------
//	@function:
//		CWorker::RegisterMutex
//
//	@doc:
//		add mutex to the list of mutex already held
//
//---------------------------------------------------------------------------
void
CWorker::RegisterMutex
	(
	CMutexBase *mutex
	)
{
	GPOS_ASSERT(NULL != mutex);
	GPOS_ASSERT(GPOS_OK != m_mutex_list.Find(mutex));

	m_mutex_list.Prepend(mutex);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::UnregisterMutex
//
//	@doc:
//		remove mutex from list
//
//---------------------------------------------------------------------------
void
CWorker::UnregisterMutex
	(
	CMutexBase *mutex
	)
{
	GPOS_ASSERT(NULL != mutex);
	GPOS_ASSERT(GPOS_OK == m_mutex_list.Find(mutex));
	
	m_mutex_list.Remove(mutex);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::OwnsMutexes
//
//	@doc:
//		does worker hold any mutexes?
//
//---------------------------------------------------------------------------
BOOL
CWorker::OwnsMutexes() const
{
	return !m_mutex_list.IsEmpty();
}

#endif // GPOS_DEBUG

// EOF

