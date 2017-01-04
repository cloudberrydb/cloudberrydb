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

			typedef CThreadManager::SThreadDescriptor SThreadDescriptor;

			// response to worker scheduling request
			enum EScheduleResponse
			{
				EsrExecTask,	// run assigned task
				EsrWorkerExit		// clean up and exit
			};

			// memory pool
			IMemoryPool *m_pmp;
		
			// mutex for task scheduling and changes to WLS
			CMutexOS m_mutex;

			// event to signal task scheduling and changes to WLS
			CEvent m_event;

			// task scheduler
			CTaskSchedulerFifo m_ts;

			// thread descriptor manager
			CThreadManager m_tm;

			// current, min and max number of active workers
			volatile ULONG_PTR m_ulpWorkers;
			volatile ULONG m_ulWorkersMin;
			volatile ULONG m_ulWorkersMax;

			// auto task proxy counter
			ULONG_PTR m_ulAtpCnt;

			// active flag
			BOOL m_fActive;

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
			void Schedule(CTask *ptsk);

			// increment AutoTaskProxy reference counter
			void AtpAddRef()
			{
				UlpExchangeAdd(&m_ulAtpCnt, 1);
			}

			// decrement AutoTaskProxy reference counter
			void AtpRemoveRef()
			{
#ifdef GPOS_DEBUG
				ULONG_PTR ulAtpCnt =
#endif // GPOS_DEBUG
				UlpExchangeAdd(&m_ulAtpCnt, -1);
				GPOS_ASSERT(ulAtpCnt != 0 &&
							"AutoTaskProxy counter decremented from 0");
			}

			// insert task in table
			void RegisterTask(CTask *ptsk);

			// remove task from table
			CTask *PtskRemoveTask(CTaskId tid);

			//-------------------------------------------------------------------
			// Interface for CWorker
			//-------------------------------------------------------------------

			// insert worker in table
			void RegisterWorker(CWorker *pwrkr);

			// remove worker from table
			CWorker *PwrkrRemoveWorker(CWorkerId wid);

			// response to worker's request for next task to execute
			EScheduleResponse EsrTskNext(CTask **pptsk);

			//-------------------------------------------------------------------
			// Methods for internal use
			//-------------------------------------------------------------------

			// create new worker thread
			void CreateWorkerThread();

			// lookup given worker
			CWorker *Pwrkr(CWorkerId wid);

			// set min and max number of workers
			void SetWorkersLim(ULONG ulWorkersMin, ULONG ulWorkersMax);

			// check if worker count needs to increase
			BOOL FWorkersIncrease();

			// check if worker count needs to decrease
			BOOL FWorkersDecrease();

			// no copy ctor
			CWorkerPoolManager(const CWorkerPoolManager&);

			// private ctor
			CWorkerPoolManager
				(
				IMemoryPool *pmp
				);

			// static singleton - global instance of worker pool manager
			static CWorkerPoolManager *m_pwpm;

		public:

			// lookup own worker
			inline
			CWorker *PwrkrSelf()
			{
				CWorkerId widSelf;
				return Pwrkr(widSelf);
			}

			// dtor
			~CWorkerPoolManager()
			{
				GPOS_ASSERT(NULL == m_pwpm &&
						   "Worker pool has not been shut down");
			}

			// initialize worker pool manager
			static
			GPOS_RESULT EresInit(ULONG ulWorkersMin, ULONG ulWorkersMax);

			// de-init global instance
			static
			void Shutdown();

			// global accessor
			inline
			static CWorkerPoolManager *Pwpm()
			{
				return m_pwpm;
			}

			// cancel task by task id
			void Cancel(CTaskId tid);

			// worker count accessor
			ULONG UlWorkers() const
			{
				return (ULONG) m_ulpWorkers;
			}

			// get min allowed number of workers
			ULONG UlWorkersMin() const
			{
				return m_ulWorkersMin;
			}

			// get max allowed number of workers
			ULONG UlWorkersMax() const
			{
				return m_ulWorkersMax;
			}


			// running worker count 
			ULONG UlWorkersRunning() const
			{
				return UlWorkers() - m_event.CWaiters();
			}

			// set min number of workers
			void SetWorkersMin(volatile ULONG ulWorkersMin);

			// set max number of workers
			void SetWorkersMax(volatile ULONG ulWorkersMax);

			// check if given thread is owned by running threads list
			BOOL FOwnedThread
				(
				PTHREAD_T pthrdt
				)
			{
				return m_tm.FRunningThread(pthrdt);
			}

	}; // class CWorkerPoolManager

}

#endif // !GPOS_CWorkerPoolManager_H

// EOF

