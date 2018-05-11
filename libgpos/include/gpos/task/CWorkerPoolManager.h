//-----------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CWorkerPoolManager.h
//
//	@doc:
//		Central worker pool manager;
//		* maintains worker local storage
//		* hosts task scheduler
//		* assigns tasks to workers
//-----------------------------------------------------------------------------
#ifndef GPOS_CWorkerPoolManager_H
#define GPOS_CWorkerPoolManager_H

#include "gpos/base.h"
#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/sync/CAtomicCounter.h"
#include "gpos/sync/CEvent.h"
#include "gpos/sync/CMutex.h"
#include "gpos/task/CTask.h"
#include "gpos/task/CTaskId.h"
#include "gpos/task/CTaskSchedulerFifo.h"
#include "gpos/task/CThreadManager.h"
#include "gpos/task/CWorker.h"
#include "gpos/task/CWorkerId.h"

#define GPOS_WORKERPOOL_HT_SIZE 			(1024)				// number of buckets in hash tables
#define GPOS_WORKER_STACK_SIZE				(500 * 1024)		// max worker stack size
#define GPOS_WORKERPOOL_MEM_POOL_SIZE 		(2 * 1024 * 1024)	// memory pool size

namespace gpos
{
	//------------------------------------------------------------------------
	//	@class:
	//		CWorkerPoolManager
	//
	//	@doc:
	//		Singleton object to handle worker pool;
	//		maintains WLS (worker local storage);
	//		assigns tasks to workers;
	//
	//------------------------------------------------------------------------
	class CWorkerPoolManager
	{	
		friend class CWorker;
		friend class CAutoTaskProxy;
		friend class CThreadManager;

		private:

			typedef CThreadManager::ThreadDescriptor ThreadDescriptor;

			// response to worker scheduling request
			enum EScheduleResponse
			{
				EsrExecTask,	// run assigned task
				EsrWorkerExit		// clean up and exit
			};

			// memory pool
			IMemoryPool *m_mp;
		
			// mutex for task scheduling and changes to WLS
			CMutexOS m_mutex;

			// event to signal task scheduling and changes to WLS
			CEvent m_event;

			// task scheduler
			CTaskSchedulerFifo m_task_scheduler;

			// thread descriptor manager
			CThreadManager m_thread_manager;

			// current, min and max number of active workers
			volatile ULONG_PTR m_num_workers;
			volatile ULONG m_min_workers;
			volatile ULONG m_max_workers;

			// auto task proxy counter
			ULONG_PTR m_auto_task_proxy_counter;

			// active flag
			BOOL m_active;

			// WLS
			CSyncHashtable
			<CWorker, CWorkerId, CSpinlockOS> m_shtWLS;

			// task storage
			CSyncHashtable
			<CTask, CTaskId, CSpinlockOS> m_shtTS;

			//-------------------------------------------------------------------
			// Interface for CAutoTaskProxy
			//-------------------------------------------------------------------

			// add task to scheduler
			void Schedule(CTask *task);

			// increment AutoTaskProxy reference counter
			void AddRef()
			{
				ExchangeAddUlongPtrWithInt(&m_auto_task_proxy_counter, 1);
			}

			// decrement AutoTaskProxy reference counter
			void RemoveRef()
			{
#ifdef GPOS_DEBUG
				ULONG_PTR auto_task_proxy_counter =
#endif // GPOS_DEBUG
				ExchangeAddUlongPtrWithInt(&m_auto_task_proxy_counter, -1);
				GPOS_ASSERT(auto_task_proxy_counter != 0 &&
							"AutoTaskProxy counter decremented from 0");
			}

			// insert task in table
			void RegisterTask(CTask *task);

			// remove task from table
			CTask *RemoveTask(CTaskId tid);

			//-------------------------------------------------------------------
			// Interface for CWorker
			//-------------------------------------------------------------------

			// insert worker in table
			void RegisterWorker(CWorker *worker);

			// remove worker from table
			CWorker *RemoveWorker(CWorkerId wid);

			// response to worker's request for next task to execute
			EScheduleResponse RespondToNextTaskRequest(CTask **task);

			//-------------------------------------------------------------------
			// Methods for internal use
			//-------------------------------------------------------------------

			// create new worker thread
			void CreateWorkerThread();

			// lookup given worker
			CWorker *Worker(CWorkerId wid);

			// set min and max number of workers
			void SetWorkersLimit(ULONG min_workers, ULONG max_workers);

			// check if worker count needs to increase
			BOOL WorkersIncrease();

			// check if worker count needs to decrease
			BOOL WorkersDecrease();

			// no copy ctor
			CWorkerPoolManager(const CWorkerPoolManager&);

			// private ctor
			CWorkerPoolManager
				(
				IMemoryPool *mp
				);

			// static singleton - global instance of worker pool manager
			static CWorkerPoolManager *m_worker_pool_manager;

		public:

			// lookup own worker
			inline
			CWorker *Self()
			{
				CWorkerId self_wid;
				return Worker(self_wid);
			}

			// dtor
			~CWorkerPoolManager()
			{
				GPOS_ASSERT(NULL == m_worker_pool_manager &&
						   "Worker pool has not been shut down");
			}

			// initialize worker pool manager
			static
			GPOS_RESULT Init(ULONG min_workers, ULONG max_workers);

			// de-init global instance
			static
			void Shutdown();

			// global accessor
			inline
			static CWorkerPoolManager *WorkerPoolManager()
			{
				return m_worker_pool_manager;
			}

			// cancel task by task id
			void Cancel(CTaskId tid);

			// worker count accessor
			ULONG GetNumWorkers() const
			{
				return (ULONG) m_num_workers;
			}

			// get min allowed number of workers
			ULONG GetMinWorkers() const
			{
				return m_min_workers;
			}

			// get max allowed number of workers
			ULONG GetMaxWorkers() const
			{
				return m_max_workers;
			}


			// running worker count 
			ULONG GetNumWorkersRunning() const
			{
				return GetNumWorkers() - m_event.GetNumWaiters();
			}

			// set min number of workers
			void SetMinWorkers(volatile ULONG min_workers);

			// set max number of workers
			void SetMaxWorkers(volatile ULONG max_workers);

			// check if given thread is owned by running threads list
			BOOL OwnedThread
				(
				PTHREAD_T thread
				)
			{
				return m_thread_manager.IsThreadRunning(thread);
			}

	}; // class CWorkerPoolManager

}

#endif // !GPOS_CWorkerPoolManager_H

// EOF

