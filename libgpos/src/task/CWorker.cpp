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

volatile bool
CWorker::abort_requested = false;

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
	ULONG ulThreadId,
	ULONG cStackSize,
	ULONG_PTR ulpStackStart
	)
	:
	m_ptsk(NULL),
	m_ulThreadId(ulThreadId),
	m_cStackSize(cStackSize),
	m_ulpStackStart(ulpStackStart)
{
#ifdef GPOS_DEBUG			
	m_listSlock.Init(GPOS_OFFSET(CSpinlockBase, m_link));
	m_listMutex.Init(GPOS_OFFSET(CMutexBase, m_link));
#endif // GPOS_DEBUG

	GPOS_ASSERT(cStackSize >= 2 * 1024 && "Worker has to have at least 2KB stack");

	// register worker
	GPOS_ASSERT(NULL == PwrkrSelf() && "Found registered worker!");
	
	CWorkerPoolManager::Pwpm()->RegisterWorker(this);
	GPOS_ASSERT(this == CWorkerPoolManager::Pwpm()->PwrkrSelf());

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
	GPOS_ASSERT(!FOwnsSpinlocks() && "Cannot destruct worker while holding a spinlock.");
	GPOS_ASSERT(!FOwnsMutexes() && "Cannot destruct worker while holding a mutex.");
	
	// unregister worker
	GPOS_ASSERT(this == PwrkrSelf() && "Unidentified worker found.");
	CWorkerPoolManager::Pwpm()->PwrkrRemoveWorker(m_wid);
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
	GPOS_ASSERT(NULL == m_ptsk);

	CTask *ptsk = NULL;

	while (CWorkerPoolManager::EsrExecTask == CWorkerPoolManager::Pwpm()->EsrTskNext(&ptsk))
	{
		Execute(ptsk);
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
CWorker::Execute(CTask *ptsk)
{
	GPOS_ASSERT(ptsk);
	GPOS_ASSERT(NULL == m_ptsk && "Another task is assigned to worker");

#ifdef GPOS_DEBUG
	ResetTimeSlice();
#endif // GPOS_DEBUG

	m_ptsk = ptsk;
	GPOS_TRY
	{
		m_ptsk->Execute();
		m_ptsk = NULL;
	}
	GPOS_CATCH_EX(ex)
	{
		m_ptsk = NULL;
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
	const CHAR *szFile,
	ULONG ulLine
#else
	const CHAR *,
	ULONG
#endif // GPOS_FPSIMULATOR
	)
{
	// check if there is a task assigned to worker,
	// task is still running and CFA is not suspended
	if (NULL != m_ptsk && m_ptsk->FRunning() && !m_ptsk->FAbortSuspended())
	{
		GPOS_ASSERT(!m_ptsk->Perrctxt()->FPending() &&
		            "Check-For-Abort while an exception is pending");

#ifdef GPOS_FPSIMULATOR
		SimulateAbort(szFile, ulLine);
#endif // GPOS_FPSIMULATOR

		if (CWorker::abort_requested || m_ptsk->FCanceled())
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
CWorker::FCheckStackSize(ULONG ulRequest) const
{
	ULONG_PTR ulptr = 0;
	
	// get current stack size
	ULONG_PTR ulpSize = m_ulpStackStart - (ULONG_PTR)&ulptr;
	
	// check if we have exceeded stack space
	if (ulpSize >= m_cStackSize)
	{
		// raise stack overflow exception
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiOutOfStack);
	}

	// check if there is enough stack space for request
	if (ulpSize + ulRequest >= m_cStackSize)
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
	const CHAR *szFile,
	ULONG ulLine
	)
{
	if (m_ptsk->FTrace(EtraceSimulateAbort) &&
		CFSimulator::Pfsim()->FNewStack(CException::ExmaSystem, CException::ExmiAbort))
	{
		// GPOS_TRACE has CFA, disable simulation temporarily
		m_ptsk->FTrace(EtraceSimulateAbort, false);

		GPOS_TRACE_FORMAT_ERR("Simulating Abort at %s:%d", szFile, ulLine);

		// resume simulation
		m_ptsk->FTrace(EtraceSimulateAbort, true);

		m_ptsk->Cancel();
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
	if (IWorker::m_fEnforceTimeSlices)
	{
		m_sdLastCA.BackTrace();
		m_timerLastCA.Restart();
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
	if (IWorker::m_fEnforceTimeSlices)
	{
		ULONG ulInterval = m_timerLastCA.UlElapsedMS();
		
		ULONG ulThreads = std::max((ULONG) 1, CWorkerPoolManager::Pwpm()->UlWorkersRunning());
		
		if (GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC * ulThreads <= ulInterval && m_ptsk->FRunning())
		{
			// get stack trace for last checkpoint
			WCHAR wszBuffer[GPOS_STACK_TRACE_BUFFER_SIZE];
			CWStringStatic str(wszBuffer, GPOS_ARRAY_SIZE(wszBuffer));
			m_sdLastCA.AppendTrace(&str);

			ResetTimeSlice();
			GPOS_RAISE
				(
				CException::ExmaSystem,
				CException::ExmiAbortTimeout,
				ulInterval,
				str.Wsz()
				);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::FOwnsSpinlocks
//
//	@doc:
//		Check whether any sync objects of a given type are currently owned
//		by this worker
//
//---------------------------------------------------------------------------
BOOL
CWorker::FOwnsSpinlocks() const
{
	return !m_listSlock.FEmpty();
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::FCanAcquireSpinlock
//
//	@doc:
//		Check if we can legally acquire a given spinlock
//
//---------------------------------------------------------------------------
BOOL
CWorker::FCanAcquireSpinlock
	(
	const CSpinlockBase *pslock
	)
	const
{
	GPOS_ASSERT(NULL != pslock);

	CSpinlockBase *pslockLast = m_listSlock.PtFirst();
	return (NULL == pslockLast) || (pslockLast->UlRank() >= pslock->UlRank());
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
	CSpinlockBase *pslock
	)
{
	// make sure this one can actually be acquired
	GPOS_ASSERT(FCanAcquireSpinlock(pslock));

	m_listSlock.Prepend(pslock);
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
	CSpinlockBase *pslock
	)
{
	m_listSlock.Remove(pslock);
	
	// must be able to re-acquire now
	GPOS_ASSERT(FCanAcquireSpinlock(pslock));
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
	CMutexBase *pmutex
	)
{
	GPOS_ASSERT(NULL != pmutex);
	GPOS_ASSERT(GPOS_OK != m_listMutex.EresFind(pmutex));

	m_listMutex.Prepend(pmutex);
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
	CMutexBase *pmutex
	)
{
	GPOS_ASSERT(NULL != pmutex);
	GPOS_ASSERT(GPOS_OK == m_listMutex.EresFind(pmutex));
	
	m_listMutex.Remove(pmutex);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorker::FOwnsMutexes
//
//	@doc:
//		does worker hold any mutexes?
//
//---------------------------------------------------------------------------
BOOL
CWorker::FOwnsMutexes() const
{
	return !m_listMutex.FEmpty();
}

#endif // GPOS_DEBUG

// EOF

