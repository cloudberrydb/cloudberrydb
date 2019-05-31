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
#include "gpos/memory/CMemoryPool.h"

#include "gpos/task/CWorkerPoolManager.h"

using namespace gpos;

//---------------------------------------------------------------------------
// static singleton - global instance of worker pool manager
//---------------------------------------------------------------------------
CWorkerPoolManager *CWorkerPoolManager::m_worker_pool_manager = NULL;


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
	CMemoryPool *mp
	)
	:
	m_mp(mp),
	m_num_workers(0),
	m_min_workers(0),
	m_max_workers(0),
	m_auto_task_proxy_counter(0),
	m_active(false)
{
	// initialize hash tables
	m_shtWLS.Init
		(
		mp,
		GPOS_WORKERPOOL_HT_SIZE,
		GPOS_OFFSET(CWorker, m_link),
		GPOS_OFFSET(CWorker, m_wid),
		&(CWorkerId::m_wid_invalid),
		CWorkerId::HashValue,
		CWorkerId::Equals
		);

	m_shtTS.Init
		(
		mp,
		GPOS_WORKERPOOL_HT_SIZE,
		GPOS_OFFSET(CTask, m_worker_pool_manager_link),
		GPOS_OFFSET(CTask, m_tid),
		&(CTaskId::m_invalid_tid),
		CTaskId::HashValue,
		CTaskId::Equals
		);

	// initialize mutex
	m_event.Init(&m_mutex);

	// set active
	m_active = true;
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
CWorkerPoolManager::Init
	(
	ULONG min_workers,
	ULONG max_workers
	)
{
	GPOS_ASSERT(NULL == WorkerPoolManager());
	GPOS_ASSERT(min_workers <= max_workers);
	GPOS_ASSERT(max_workers <= GPOS_THREAD_MAX);

	CMemoryPool *mp =
		CMemoryPoolManager::GetMemoryPoolMgr()->Create
			(
			CMemoryPoolManager::EatTracker,
			true /*fThreadSafe*/,
			GPOS_WORKERPOOL_MEM_POOL_SIZE
			);

	GPOS_TRY
	{
		// create worker pool
		CWorkerPoolManager::m_worker_pool_manager =
			GPOS_NEW(mp) CWorkerPoolManager(mp);

		// set min and max number of workers
		CWorkerPoolManager::m_worker_pool_manager->SetWorkersLimit(min_workers, max_workers);
	}
	GPOS_CATCH_EX(ex)
	{
		// turn in memory pool in case of failure
		CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);

		CWorkerPoolManager::m_worker_pool_manager = NULL;

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
	CWorkerPoolManager *worker_pool_manager = CWorkerPoolManager::m_worker_pool_manager;

	GPOS_ASSERT(NULL != worker_pool_manager && "Worker pool has not been initialized");

	GPOS_ASSERT(0 == worker_pool_manager->m_auto_task_proxy_counter &&
			    "AutoTaskProxy alive at worker pool shutdown");

	// scope for mutex
	{
		CAutoMutex am(worker_pool_manager->m_mutex);
		am.Lock();

		// stop scheduling tasks
		worker_pool_manager->m_active = false;

		// wake-up workers
		worker_pool_manager->m_event.Broadcast();
	}

	// wait until all threads exit
	worker_pool_manager->m_thread_manager.ShutDown();


	CMemoryPool *mp = worker_pool_manager->m_mp;

	// destroy worker pool
	CWorkerPoolManager::m_worker_pool_manager = NULL;
	GPOS_DELETE(worker_pool_manager);

	// release allocated memory pool
    CMemoryPoolManager::GetMemoryPoolMgr()->Destroy(mp);
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
	ULONG_PTR num_workers = ExchangeAddUlongPtrWithInt(&m_num_workers, 1);

	// check if max number of workers is exceeded
	if (num_workers >= m_max_workers)
	{
		// decrement number of active workers
		ExchangeAddUlongPtrWithInt(&m_num_workers, -1);

		return;
	}

	// attempt to create new thread
	if (GPOS_OK != m_thread_manager.Create())
	{
		// decrement number of active workers
		ExchangeAddUlongPtrWithInt(&m_num_workers, -1);

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
	CWorker *worker
	)
{
#ifdef GPOS_DEBUG
	// make sure worker registers itself
	CWorkerId self_wid;
	GPOS_ASSERT(NULL != worker);
	GPOS_ASSERT(self_wid.Equals(worker->GetWid()));
#endif // GPOS_DEBUG

	// scope for hash table accessor
	{
		// get access
		CWorkerId wid = worker->GetWid();
		CSyncHashtableAccessByKey<CWorker, CWorkerId, CSpinlockOS> shta(m_shtWLS, wid);
		
		// must be first to register
		GPOS_ASSERT(NULL == shta.Find() && "Found registered worker.");

		shta.Insert(worker);
	}

	// check insertion succeeded
	GPOS_ASSERT(worker == Worker(worker->GetWid()));
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RemoveWorker
//
//	@doc:
//		Remover worker, given its id, from WLS table
//
//---------------------------------------------------------------------------
CWorker *
CWorkerPoolManager::RemoveWorker
	(
	CWorkerId wid
	)
{

#ifdef GPOS_DEBUG
	// make sure regular workers can only remove themselves
	CWorkerId self_wid;
	GPOS_ASSERT(self_wid.Equals(wid));
#endif // GPOS_DEBUG

	CWorker *worker = NULL;
	
	// scope for hash table accessor
	{
		// get access
		CSyncHashtableAccessByKey<CWorker, CWorkerId, CSpinlockOS> shta(m_shtWLS, wid);
		
		worker = shta.Find();
		if (NULL != worker)
		{
			shta.Remove(worker);
		}
	}

	return worker;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::Worker()
//
//	@doc:
//		Retrieve worker by id;
//		Note, this returns the worker beyond the scope of the spinlock
//		protection; in the standard application scenario this is used to
//		lookup ones own worker only!
//
//---------------------------------------------------------------------------
CWorker *
CWorkerPoolManager::Worker
	(
	CWorkerId wid
	)
{
	// get access
	CSyncHashtableAccessByKey<CWorker, CWorkerId, CSpinlockOS> shta(m_shtWLS, wid);

	return shta.Find();
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
	CTask *task
	)
{
	GPOS_ASSERT(m_active && "Worker pool is not operating");

	// scope for hash table accessor
	{
		// get access
		CTaskId &tid = task->m_tid;
		CSyncHashtableAccessByKey<CTask, CTaskId, CSpinlockOS> shta(m_shtTS, tid);

		// must be first to register
		GPOS_ASSERT(NULL == shta.Find() && "Found registered task.");

		shta.Insert(task);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RemoveTask
//
//	@doc:
//		Remove worker, given by id, from the task table
//
//---------------------------------------------------------------------------
CTask *
CWorkerPoolManager::RemoveTask
	(
	CTaskId tid
	)
{

	CTask *task = NULL;

	// scope for hash table accessor
	{
		// get access
		CSyncHashtableAccessByKey<CTask, CTaskId, CSpinlockOS> shta(m_shtTS, tid);

		task = shta.Find();
		if (NULL != task)
		{
			shta.Remove(task);
		}
	}

	return task;
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
	CTask *task
	)
{
	GPOS_ASSERT(m_active && "Worker pool is not operating");
	GPOS_ASSERT(0 < m_num_workers && "Worker pool has no workers");

	// scope for lock
	{
		CAutoMutex am(m_mutex);
		am.Lock();

		// add task to scheduler's queue
		m_task_scheduler.Enqueue(task);

		// signal arrival of new task
		m_event.Signal();
	}

	GPOS_CHECK_ABORT;

	// create new worker if needed
	if (WorkersIncrease())
	{
		CreateWorkerThread();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::RespondToNextTaskRequest
//
//	@doc:
//		Respond to worker's request for next task to execute;
//
//---------------------------------------------------------------------------
CWorkerPoolManager::EScheduleResponse
CWorkerPoolManager::RespondToNextTaskRequest
	(
	CTask **task
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	*task = NULL;

	// check if worker pool is shutting down
	// or worker count needs to be reduced
	while (m_active && !WorkersDecrease())
	{
		// check if scheduler's queue is empty
		if (!m_task_scheduler.IsEmpty())
		{
			// assign queued task to worker
			*task = m_task_scheduler.Dequeue();
			return EsrExecTask;
		}

		// wait until task is enqueued
		m_event.Wait();
	}

	// wake up another worker
	m_event.Signal();

	// decrement number of active workers
	GPOS_ASSERT(0 < m_num_workers && "Negative number of workers");
	m_num_workers--;

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
CWorkerPoolManager::WorkersIncrease()
{
	return !m_task_scheduler.IsEmpty();
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
CWorkerPoolManager::WorkersDecrease()
{
	return (m_num_workers > m_max_workers);
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
	BOOL is_queued = false;

	CTask *task = NULL;

	// scope for hash table accessor
	{
		CSyncHashtableAccessByKey<CTask, CTaskId, CSpinlockOS> shta(m_shtTS, tid);
		task = shta.Find();
		if (NULL != task)
		{
			task->Cancel();
			is_queued = (CTask::EtsQueued == task->m_status);
		}
	}

	// remove task from scheduler's queue
	if (is_queued)
	{
		GPOS_ASSERT(NULL != task);

		GPOS_RESULT eres = GPOS_OK;

		// scope for lock
		{
			CAutoMutex am(m_mutex);
			am.Lock();

			eres = m_task_scheduler.Cancel(task);
		}

		// if task was dequeued, signal task completion
		if (GPOS_OK == eres)
		{
			task->Signal(CTask::EtsError);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::SetWorkersLimit
//
//	@doc:
//		Set min and max number of workers
//
//---------------------------------------------------------------------------
void
CWorkerPoolManager::SetWorkersLimit
	(
	ULONG min_workers,
	ULONG max_workers
	)
{
	GPOS_ASSERT(min_workers <= max_workers);
	GPOS_ASSERT(max_workers <= GPOS_THREAD_MAX);

	m_min_workers = min_workers;
	m_max_workers = max_workers;

	// reach minimum number of workers
	while (m_min_workers > m_num_workers)
	{
		CreateWorkerThread();
	}

	// signal workers if their number exceeds maximum
	if (m_max_workers < m_num_workers)
	{
		CAutoMutex am(m_mutex);
		am.Lock();

		m_event.Signal();
	}

	GPOS_ASSERT(m_min_workers <= m_num_workers);
	GPOS_ASSERT(m_min_workers <= m_max_workers);
}



//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::SetMinWorkers
//
//	@doc:
//		Set min number of workers
//
//---------------------------------------------------------------------------
void 
CWorkerPoolManager::SetMinWorkers
	(
	volatile ULONG min_workers
	)
{
	SetWorkersLimit(min_workers, std::max(min_workers, m_max_workers));
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManager::SetMaxWorkers
//
//	@doc:
//		Set max number of workers
//
//---------------------------------------------------------------------------
void 
CWorkerPoolManager::SetMaxWorkers
	(
	volatile ULONG max_workers
	)
{
	SetWorkersLimit(std::min(max_workers, m_min_workers), max_workers);
}


// EOF

