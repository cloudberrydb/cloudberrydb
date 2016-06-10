//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CTask.h
//
//	@doc:
//		Interface class for task abstraction
//---------------------------------------------------------------------------
#ifndef GPOS_CTask_H
#define GPOS_CTask_H

#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/error/CException.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/sync/CAutoMutex.h"
#include "gpos/sync/CEvent.h"
#include "gpos/sync/CMutex.h"
#include "gpos/task/CTaskContext.h"
#include "gpos/task/CTaskId.h"
#include "gpos/task/CTaskLocalStorage.h"
#include "gpos/task/ITask.h"
#include "gpos/task/IWorker.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CTask
	//
	//	@doc:
	//		Interface to abstract task (work unit);
	//		provides asynchronous task execution and error handling;
	//
	//---------------------------------------------------------------------------
	class CTask : public ITask
	{	

		friend class CAutoTaskProxy;
		friend class CAutoTaskProxyTest;
		friend class CTaskSchedulerFifo;
		friend class CWorker;
		friend class CWorkerPoolManager;
		friend class CUnittest;
		friend class CAutoSuspendAbort;

		private:

			// task memory pool -- exclusively used by this task
			IMemoryPool *m_pmp;
		
			// task context
			CTaskContext *m_ptskctxt;

			// error context
			IErrorContext *m_perrctxt;
				
			// error handler stack
			CErrorHandler *m_perrhdl;		

			// function to execute
			void *(*m_pfunc)(void *);
			
			// function argument
			void *m_pvArg;
			
			// function result
			void *m_pvRes;

			// TLS
			CTaskLocalStorage m_tls;
			
			// mutex for status change
			CMutexBase *m_pmutex;

			// event to signal status change
			CEvent *m_pevent;

			// task status
			volatile ETaskStatus m_estatus;

			// cancellation flag
			volatile BOOL *m_pfCancel;

			// local cancellation flag; used when no flag is externally passed
			volatile BOOL m_fCancel;

			// counter of requests to suspend cancellation
			ULONG m_ulAbortSuspendCount;

			// flag denoting task completion report
			BOOL m_fReported;

			// task identifier
			CTaskId m_tid;

			// ctor
			CTask
				(
				IMemoryPool *pmp,
				CTaskContext *ptskctxt,
				IErrorContext *perrctxt,
				CEvent *pevent,
				volatile BOOL *pfCancel
				);

			// no copy ctor
			CTask(const CTask&);

			// set task status
			void SetStatus(ETaskStatus ets);

			// signal task completion or error
			void Signal(ETaskStatus Ets) throw();

			// binding a task structure to a function and its arguments
			void Bind(void *(*pfunc)(void*), void *pvArg);

			// execution, called by the owning worker
			void Execute();

			// check if task has been scheduled
			BOOL FScheduled() const;

			// check if task finished executing
			BOOL FFinished() const;

			// check if task is currently executing
			BOOL FRunning() const
			{
				return EtsRunning == m_estatus;
			}

			// reported flag accessor
			BOOL FReported() const
			{
				return m_fReported;
			}

			// set reported flag
			void SetReported()
			{
				GPOS_ASSERT(!m_fReported && "Task already reported as completed");

				m_fReported = true;
			}

		public:

			// dtor
			virtual ~CTask();

			// accessor for memory pool, e.g. used for allocating task parameters in
			IMemoryPool *Pmp() const
			{
				return m_pmp;
			}

			// TLS accessor
			CTaskLocalStorage &Tls()
			{
				return m_tls;
			}

			// task id accessor
			CTaskId &Tid()
			{
				return m_tid;
			}

			// task context accessor
			CTaskContext *Ptskctxt() const
			{
				return m_ptskctxt;
			}

			// basic output streams
			ILogger *PlogOut() const
			{
				return this->m_ptskctxt->PlogOut();
			}
			
			ILogger *PlogErr() const
			{
				return this->m_ptskctxt->PlogErr();
			}
			
			BOOL FTrace
				(
				ULONG ulTrace,
				BOOL fVal
				)
			{
				return this->m_ptskctxt->FTrace(ulTrace, fVal);
			}
			
			BOOL FTrace
				(
				ULONG ulTrace
				)
			{
				return this->m_ptskctxt->FTrace(ulTrace);
			}

			
			// locale
			ELocale Eloc() const
			{
				return m_ptskctxt->Eloc();
			}

			// check if task is canceled
			BOOL FCanceled() const
			{
				return *m_pfCancel;
			}

			// reset cancel flag
			void ResetCancel()
			{
				*m_pfCancel = false;
			}

			// set cancel flag
			void Cancel()
			{
				*m_pfCancel = true;
			}

			// check if a request to suspend abort was received
			BOOL FAbortSuspended() const
			{
				return (0 < m_ulAbortSuspendCount);
			}

			// increment counter for requests to suspend abort
			void SuspendAbort()
			{
				m_ulAbortSuspendCount++;
			}

			// decrement counter for requests to suspend abort
 			void ResumeAbort();

			// task status accessor
			ETaskStatus Ets() const
			{
				return m_estatus;
			}

			// task result accessor
			void *PvRes() const
			{
				return m_pvRes;
			}

			// error context
			IErrorContext *Perrctxt() const
			{
				return m_perrctxt;
			}
			
			// error context
			CErrorContext *PerrctxtConvert()
			{
				return dynamic_cast<CErrorContext*>(m_perrctxt);
			}

			// pending exceptions
			BOOL FPendingExc() const
			{
				return m_perrctxt->FPending();
			}

#ifdef GPOS_DEBUG
			// check if task has expected status
			BOOL FCheckStatus(BOOL fCompleted);
#endif // GPOS_DEBUG

			// slink for auto task proxy
			SLink m_linkAtp;

			// slink for task scheduler
			SLink m_linkTs;

			// slink for worker pool manager
			SLink m_linkWpm;

			static
			CTask *PtskSelf()
			{
				return dynamic_cast<CTask *>(ITask::PtskSelf());
			}

	}; // class CTask

}

#endif // !GPOS_CTask_H

// EOF

