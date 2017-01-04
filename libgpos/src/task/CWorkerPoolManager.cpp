//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CWorkerPoolManager.cpp
//
//	@doc:
//		Central worker pool manager;
//		* maintains worker local storage
//		* hosts task scheduler
//		* assigns tasks to workers
//---------------------------------------------------------------------------


#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/IMemoryPool.h"

#include "gpos/task/CWorkerPoolManager.h"

using namespace gpos;

//---------------------------------------------------------------------------
// static singleton - global instance of worker pool manager
//---------------------------------------------------------------------------
CWorkerPoolManager *CWorkerPoolManager::m_pwpm = NULL;


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::CWorkerPoolManager
//
//	@doc:
//		Private ctor
//
//---------------------------------------------------------------------------
CWorkerPoolManager::CWorkerPoolManager
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_ulpWorkers(0),
	m_ulWorkersMin(0),
	m_ulWorkersMax(0),
	m_ulAtpCnt(0),
	m_fActive(false)
{
	// initialize hash tables
	m_shtWLS.Init
		(
		pmp,
		GPOS_WORKERPOOL_HT_SIZE,
		GPOS_OFFSET(CWorker, m_link),
		GPOS_OFFSET(CWorker, m_wid),
		&(CWorkerId::m_widInvalid),
		CWorkerId::UlHash,
		CWorkerId::FEqual
		);

	m_shtTS.Init
		(
		pmp,
		GPOS_WORKERPOOL_HT_SIZE,
		GPOS_OFFSET(CTask, m_linkWpm),
		GPOS_OFFSET(CTask, m_tid),
		&(CTaskId::m_tidInvalid),
		CTaskId::UlHash,
		CTaskId::FEqual
		);

	// initialize mutex
	m_event.Init(&m_mutex);

	// set active
	m_fActive = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Init()
//
//	@doc:
//		Initializer for global worker pool manager
//
//---------------------------------------------------------------------------
GPOS_RESULT
CWorkerPoolManager::EresInit
	(
	ULONG ulWorkersMin,
	ULONG ulWorkersMax
	)
{
	GPOS_ASSERT(NULL == Pwpm());
	GPOS_ASSERT(ulWorkersMin <= ulWorkersMax);
	GPOS_ASSERT(ulWorkersMax <= GPOS_THREAD_MAX);

	IMemoryPool *pmp =
		CMemoryPoolManager::Pmpm()->PmpCreate
			(
			CMemoryPoolManager::EatTracker,
			true /*fThreadSafe*/,
			GPOS_WORKERPOOL_MEM_POOL_SIZE
			);

	GPOS_TRY
	{
		// create worker pool
		CWorkerPoolManager::m_pwpm =
			GPOS_NEW(pmp) CWorkerPoolManager(pmp);

		// set min and max number of workers
		CWorkerPoolManager::m_pwpm->SetWorkersLim(ulWorkersMin, ulWorkersMax);
	}
	GPOS_CATCH_EX(ex)
	{
		// turn in memory pool in case of failure
		CMemoryPoolManager::Pmpm()->Destroy(pmp);

		CWorkerPoolManager::m_pwpm = NULL;

		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			return GPOS_OOM;
		}

		return GPOS_FAILED;
	}
	GPOS_CATCH_END;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Shutdown
//
//	@doc:
//		Shutdown stops workers and cleans up worker pool memory
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::Shutdown()
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::m_pwpm;

	GPOS_ASSERT(NULL != pwpm && "Worker pool has not been initialized");

	GPOS_ASSERT(0 == pwpm->m_ulAtpCnt &&
			    "AutoTaskProxy alive at worker pool shutdown");

	// scope for mutex
	{
		CAutoMutex am(pwpm->m_mutex);
		am.Lock();

		// stop scheduling tasks
		pwpm->m_fActive = false;

		// wake-up workers
		pwpm->m_event.Broadcast();
	}

	// wait until all threads exit
	pwpm->m_tm.ShutDown();


	IMemoryPool *pmp = pwpm->m_pmp;

	// destroy worker pool
	CWorkerPoolManager::m_pwpm = NULL;
	GPOS_DELETE(pwpm);

	// release allocated memory pool
    CMemoryPoolManager::Pmpm()->Destroy(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RegisterWorker()
//
//	@doc:
//		Create new worker thread
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::CreateWorkerThread()
{
	// increment worker count
	ULONG_PTR ulWorkers = UlpExchangeAdd(&m_ulpWorkers, 1);

	// check if max number of workers is exceeded
	if (ulWorkers >= m_ulWorkersMax)
	{
		// decrement number of active workers
		UlpExchangeAdd(&m_ulpWorkers, -1);

		return;
	}

	// attempt to create new thread
	if (GPOS_OK != m_tm.EresCreate())
	{
		// decrement number of active workers
		UlpExchangeAdd(&m_ulpWorkers, -1);

		GPOS_ASSERT(!"Failed to create new thread");
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RegisterWorker()
//
//	@doc:
//		Insert worker into the WLS table
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::RegisterWorker
	(
	CWorker *pwrkr
	)
{
#ifdef GPOS_DEBUG
	// make sure worker registers itself
	CWorkerId widSelf;
	GPOS_ASSERT(NULL != pwrkr);
	GPOS_ASSERT(widSelf.FEqual(pwrkr->Wid()));
#endif // GPOS_DEBUG

	// scope for hash table accessor
	{
		// get access
		CWorkerId wid = pwrkr->Wid();
		CSyncHashtableAccessByKey<CWorker, CWorkerId, CSpinlockOS> shta(m_shtWLS, wid);
		
		// must be first to register
		GPOS_ASSERT(NULL == shta.PtLookup() && "Found registered worker.");

		shta.Insert(pwrkr);
	}

	// check insertion succeeded
	GPOS_ASSERT(pwrkr == Pwrkr(pwrkr->Wid()));
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::PwrkrRemoveWorker
//
//	@doc:
//		Remover worker, given its id, from WLS table
//
//---------------------------------------------------------------------------
CWorker *
CWorkerPoolManager::PwrkrRemoveWorker
	(
	CWorkerId wid
	)
{

#ifdef GPOS_DEBUG
	// make sure regular workers can only remove themselves
	CWorkerId widSelf;
	GPOS_ASSERT(widSelf.FEqual(wid));
#endif // GPOS_DEBUG

	CWorker *pwrkr = NULL;
	
	// scope for hash table accessor
	{
		// get access
		CSyncHashtableAccessByKey<CWorker, CWorkerId, CSpinlockOS> shta(m_shtWLS, wid);
		
		pwrkr = shta.PtLookup();
		if (NULL != pwrkr)
		{
			shta.Remove(pwrkr);
		}
	}

	return pwrkr;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Pwrkr()
//
//	@doc:
//		Retrieve worker by id;
//		Note, this returns the worker beyond the scope of the spinlock
//		protection; in the standard application scenario this is used to
//		lookup ones own worker only!
//
//---------------------------------------------------------------------------
CWorker *
CWorkerPoolManager::Pwrkr
	(
	CWorkerId wid
	)
{
	// get access
	CSyncHashtableAccessByKey<CWorker, CWorkerId, CSpinlockOS> shta(m_shtWLS, wid);

	return shta.PtLookup();
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RegisterTask()
//
//	@doc:
//		Insert a task into the task table
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::RegisterTask
	(
	CTask *ptsk
	)
{
	GPOS_ASSERT(m_fActive && "Worker pool is not operating");

	// scope for hash table accessor
	{
		// get access
		CTaskId &tid = ptsk->m_tid;
		CSyncHashtableAccessByKey<CTask, CTaskId, CSpinlockOS> shta(m_shtTS, tid);

		// must be first to register
		GPOS_ASSERT(NULL == shta.PtLookup() && "Found registered task.");

		shta.Insert(ptsk);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::PtskRemoveTask
//
//	@doc:
//		Remove worker, given by id, from the task table
//
//---------------------------------------------------------------------------
CTask *
CWorkerPoolManager::PtskRemoveTask
	(
	CTaskId tid
	)
{

	CTask *ptsk = NULL;

	// scope for hash table accessor
	{
		// get access
		CSyncHashtableAccessByKey<CTask, CTaskId, CSpinlockOS> shta(m_shtTS, tid);

		ptsk = shta.PtLookup();
		if (NULL != ptsk)
		{
			shta.Remove(ptsk);
		}
	}

	return ptsk;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Schedule
//
//	@doc:
//		Add task to scheduler;
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::Schedule
	(
	CTask *ptsk
	)
{
	GPOS_ASSERT(m_fActive && "Worker pool is not operating");
	GPOS_ASSERT(0 < m_ulpWorkers && "Worker pool has no workers");

	// scope for lock
	{
		CAutoMutex am(m_mutex);
		am.Lock();

		// add task to scheduler's queue
		m_ts.Enqueue(ptsk);

		// signal arrival of new task
		m_event.Signal();
	}

	GPOS_CHECK_ABORT;

	// create new worker if needed
	if (FWorkersIncrease())
	{
		CreateWorkerThread();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::EsrTskNext
//
//	@doc:
//		Respond to worker's request for next task to execute;
//
//---------------------------------------------------------------------------
CWorkerPoolManager::EScheduleResponse
CWorkerPoolManager::EsrTskNext
	(
	CTask **pptsk
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	*pptsk = NULL;

	// check if worker pool is shutting down
	// or worker count needs to be reduced
	while (m_fActive && !FWorkersDecrease())
	{
		// check if scheduler's queue is empty
		if (!m_ts.FEmpty())
		{
			// assign queued task to worker
			*pptsk = m_ts.PtskDequeue();
			return EsrExecTask;
		}

		// wait until task is enqueued
		m_event.Wait();
	}

	// wake up another worker
	m_event.Signal();

	// decrement number of active workers
	GPOS_ASSERT(0 < m_ulpWorkers && "Negative number of workers");
	m_ulpWorkers--;

	return EsrWorkerExit;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::FIncWkrkCnt
//
//	@doc:
//		Check if worker count needs to increase
//
//---------------------------------------------------------------------------
BOOL
CWorkerPoolManager::FWorkersIncrease()
{
	return !m_ts.FEmpty();
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::FDecWkrkCnt
//
//	@doc:
//		Check if worker count needs to decrease
//
//---------------------------------------------------------------------------
BOOL
CWorkerPoolManager::FWorkersDecrease()
{
	return (m_ulpWorkers > m_ulWorkersMax);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Cancel
//
//	@doc:
//		Mark task as canceled
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::Cancel
	(
	CTaskId tid
	)
{
	BOOL fQueued = false;

	CTask *ptsk = NULL;

	// scope for hash table accessor
	{
		CSyncHashtableAccessByKey<CTask, CTaskId, CSpinlockOS> shta(m_shtTS, tid);
		ptsk = shta.PtLookup();
		if (NULL != ptsk)
		{
			ptsk->Cancel();
			fQueued = (CTask::EtsQueued == ptsk->m_estatus);
		}
	}

	// remove task from scheduler's queue
	if (fQueued)
	{
		GPOS_ASSERT(NULL != ptsk);

		GPOS_RESULT eres = GPOS_OK;

		// scope for lock
		{
			CAutoMutex am(m_mutex);
			am.Lock();

			eres = m_ts.EresCancel(ptsk);
		}

		// if task was dequeued, signal task completion
		if (GPOS_OK == eres)
		{
			ptsk->Signal(CTask::EtsError);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::SetWorkersLim
//
//	@doc:
//		Set min and max number of workers
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::SetWorkersLim
	(
	ULONG ulWorkersMin,
	ULONG ulWorkersMax
	)
{
	GPOS_ASSERT(ulWorkersMin <= ulWorkersMax);
	GPOS_ASSERT(ulWorkersMax <= GPOS_THREAD_MAX);

	m_ulWorkersMin = ulWorkersMin;
	m_ulWorkersMax = ulWorkersMax;

	// reach minimum number of workers
	while (m_ulWorkersMin > m_ulpWorkers)
	{
		CreateWorkerThread();
	}

	// signal workers if their number exceeds maximum
	if (m_ulWorkersMax < m_ulpWorkers)
	{
		CAutoMutex am(m_mutex);
		am.Lock();

		m_event.Signal();
	}

	GPOS_ASSERT(m_ulWorkersMin <= m_ulpWorkers);
	GPOS_ASSERT(m_ulWorkersMin <= m_ulWorkersMax);
}



//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::SetWorkersMin
//
//	@doc:
//		Set min number of workers
//
//---------------------------------------------------------------------------
void 
CWorkerPoolManager::SetWorkersMin
	(
	volatile ULONG ulWorkersMin
	)
{
	SetWorkersLim(ulWorkersMin, std::max(ulWorkersMin, m_ulWorkersMax));
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::SetWorkersMax
//
//	@doc:
//		Set max number of workers
//
//---------------------------------------------------------------------------
void 
CWorkerPoolManager::SetWorkersMax
	(
	volatile ULONG ulWorkersMax
	)
{
	SetWorkersLim(std::min(ulWorkersMax, m_ulWorkersMin), ulWorkersMax);
}


// EOF

