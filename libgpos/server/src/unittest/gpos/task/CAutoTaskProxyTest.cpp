//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CAutoTaskProxyTest.h
//
//	@doc:
//		Test for CAutoTaskProxy
//---------------------------------------------------------------------------

#include <pthread.h>

#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/task/CAutoTaskProxyTest.h"


using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_Wait),
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_WaitAny),
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_TimedWait),
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_TimedWaitAny),
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_Destroy),
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_PropagateCancelError),
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_PropagateExecError),
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_ExecuteError),
		GPOS_UNITTEST_FUNC(CAutoTaskProxyTest::EresUnittest_CheckErrorPropagation)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_Wait
//
//	@doc:
//		Wait for task to complete.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_Wait()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgPtsk[2];
		ULLONG rgRes[2];

		// create tasks
		rgPtsk[0] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Short, &rgRes[0]);
		rgPtsk[1] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Long, &rgRes[1]);

		// start two tasks
		atp.Schedule(rgPtsk[0]);
		atp.Schedule(rgPtsk[1]);

		// wait until tasks complete
		atp.Wait(rgPtsk[0]);
		atp.Wait(rgPtsk[1]);

		GPOS_ASSERT(rgRes[0] == *(ULLONG *)rgPtsk[0]->PvRes());
		GPOS_ASSERT(rgRes[1] == *(ULLONG *)rgPtsk[1]->PvRes());
	}


	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_WaitAny
//
//	@doc:
//		Wait for at least one task to complete.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_WaitAny()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgPtsk[3];
		ULLONG rgRes[3];

		// create tasks
		rgPtsk[0] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Infinite, &rgRes[0]);
		rgPtsk[1] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Infinite, &rgRes[1]);
		rgPtsk[2] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Short, &rgRes[2]);

		// start tasks
		atp.Schedule(rgPtsk[0]);
		atp.Schedule(rgPtsk[1]);
		atp.Schedule(rgPtsk[2]);

		// wait until last task completes - the other run until canceled
		CTask *ptsk = NULL;
		atp.WaitAny(&ptsk);

		GPOS_ASSERT(rgPtsk[0]->FCheckStatus(false /*fCompleted*/));
		GPOS_ASSERT(rgPtsk[1]->FCheckStatus(false /*fCompleted*/));

		GPOS_ASSERT(ptsk == rgPtsk[2]);

		// disable error propagation here
		atp.SetPropagateError(false /* fPropagateError */);

		// cancel task
		atp.Cancel(rgPtsk[0]);
		atp.WaitAny(&ptsk);

		GPOS_ASSERT(rgPtsk[1]->FCheckStatus(false /*fCompleted*/));

		GPOS_ASSERT(ptsk == rgPtsk[0]);
		GPOS_ASSERT(rgPtsk[0]->FCanceled());

		GPOS_ASSERT(CTask::EtsError == rgPtsk[0]->Ets());
		GPOS_ASSERT(CTask::EtsCompleted == rgPtsk[2]->Ets());

		GPOS_ASSERT(NULL == rgPtsk[0]->PvRes());
		GPOS_ASSERT(NULL == rgPtsk[1]->PvRes());
		GPOS_ASSERT(rgRes[2] == *(ULLONG *)rgPtsk[2]->PvRes());

		// ATP cancels running task
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_TimedWait
//
//	@doc:
//		Wait for task to complete with timeout.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_TimedWait()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgPtsk[2];
		ULLONG rgRes[2];

		// create tasks
		rgPtsk[0] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Short, &rgRes[0]);
		rgPtsk[1] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Infinite, &rgRes[1]);

		// start tasks
		atp.Schedule(rgPtsk[0]);
		atp.Schedule(rgPtsk[1]);

		// wait for first task - no timeout
#ifdef GPOS_DEBUG
		GPOS_RESULT eres =
#endif // GPOS_DEBUG
		atp.EresTimedWait(rgPtsk[0], gpos::ulong_max);
		GPOS_ASSERT(GPOS_OK == eres);

		// check second task - timeout immediately
#ifdef GPOS_DEBUG
		eres =
#endif // GPOS_DEBUG
		atp.EresTimedWait(rgPtsk[1], 0);
		GPOS_ASSERT(GPOS_TIMEOUT == eres);

		// wait for second task - timeout
#ifdef GPOS_DEBUG
		eres =
#endif // GPOS_DEBUG
		atp.EresTimedWait(rgPtsk[1], 10);
		GPOS_ASSERT(GPOS_TIMEOUT == eres);

		GPOS_ASSERT(!rgPtsk[0]->FCanceled());
		GPOS_ASSERT(!rgPtsk[1]->FCanceled());

		GPOS_ASSERT(CTask::EtsCompleted == rgPtsk[0]->Ets());
		GPOS_ASSERT(rgPtsk[1]->FScheduled() && !rgPtsk[1]->FFinished());

		GPOS_ASSERT(rgRes[0] == *(ULLONG *)rgPtsk[0]->PvRes());
		GPOS_ASSERT(NULL == rgPtsk[1]->PvRes());

		// ATP cancels running task
	}

	return GPOS_OK;

}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_TimedWait
//
//	@doc:
//		Wait for at least one task to complete with timeout.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_TimedWaitAny()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgPtsk[3];
		ULLONG rgRes[3];

		// create tasks
		rgPtsk[0] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Infinite, &rgRes[0]);
		rgPtsk[1] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Infinite, &rgRes[1]);
		rgPtsk[2] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Short, &rgRes[2]);

		// start tasks
		atp.Schedule(rgPtsk[0]);
		atp.Schedule(rgPtsk[1]);
		atp.Schedule(rgPtsk[2]);

		CTask *ptsk = NULL;

		// wait until one task completes - no timeout
#ifdef GPOS_DEBUG
		GPOS_RESULT eres =
#endif // GPOS_DEBUG
		atp.EresTimedWaitAny(&ptsk, gpos::ulong_max);
		GPOS_ASSERT(GPOS_OK == eres);
		GPOS_ASSERT(ptsk == rgPtsk[2]);
		GPOS_ASSERT(!rgPtsk[2]->FCanceled());

		GPOS_ASSERT(rgPtsk[0]->FScheduled() && !rgPtsk[0]->FFinished());
		GPOS_ASSERT(rgPtsk[1]->FScheduled() && !rgPtsk[1]->FFinished());
		GPOS_ASSERT(CTask::EtsCompleted == rgPtsk[2]->Ets());

		// check if any task is complete - immediate timeout
#ifdef GPOS_DEBUG
		eres =
#endif // GPOS_DEBUG
		atp.EresTimedWaitAny(&ptsk, 0);
		GPOS_ASSERT(GPOS_TIMEOUT == eres);
		GPOS_ASSERT(NULL == ptsk);

		// wait until one task completes - timeout
#ifdef GPOS_DEBUG
		eres =
#endif // GPOS_DEBUG
		atp.EresTimedWaitAny(&ptsk, 10);
		GPOS_ASSERT(GPOS_TIMEOUT == eres);
		GPOS_ASSERT(NULL == ptsk);

		GPOS_ASSERT(rgPtsk[0]->FScheduled() && !rgPtsk[0]->FFinished());
		GPOS_ASSERT(rgPtsk[1]->FScheduled() && !rgPtsk[1]->FFinished());
		GPOS_ASSERT(CTask::EtsCompleted == rgPtsk[2]->Ets());

		// cancel task
		atp.Cancel(rgPtsk[0]);

		atp.SetPropagateError(false /* fPropagateError */);

#ifdef GPOS_DEBUG
		eres =
#endif // GPOS_DEBUG
		atp.EresTimedWaitAny(&ptsk, gpos::ulong_max);

		GPOS_ASSERT(GPOS_OK == eres);
		GPOS_ASSERT(ptsk == rgPtsk[0]);
		GPOS_ASSERT(rgPtsk[0]->FCanceled());

		GPOS_ASSERT(CTask::EtsError == rgPtsk[0]->Ets());
		GPOS_ASSERT(rgPtsk[1]->FScheduled() && !rgPtsk[1]->FFinished());
		GPOS_ASSERT(CTask::EtsCompleted == rgPtsk[2]->Ets());

		GPOS_ASSERT(NULL == rgPtsk[0]->PvRes());
		GPOS_ASSERT(NULL == rgPtsk[1]->PvRes());
		GPOS_ASSERT(rgRes[2] == *(ULLONG *)rgPtsk[2]->PvRes());

		// ATP cancels running task
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_Cancel
//
//	@doc:
//		Cancel and destroy tasks while queued/executing
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_Destroy()
{
	const ULONG culTskCnt = 90;

	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgPtsk[culTskCnt];
		ULLONG rgRes[culTskCnt];

		CTask *ptsk = NULL;

		atp.SetPropagateError(false /* fPropagateError */);

		for (ULONG i = 0; i < culTskCnt / 3; i++)
		{
			GPOS_CHECK_ABORT;

			// create 3 tasks
			rgPtsk[3*i] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Short, &rgRes[3*i]);
			rgPtsk[3*i + 1] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Long, &rgRes[3*i + 1]);
			rgPtsk[3*i + 2] = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Infinite, &rgRes[3*i + 2]);

			atp.Schedule(rgPtsk[3*i]);
			atp.Schedule(rgPtsk[3*i + 1]);
			atp.Schedule(rgPtsk[3*i + 2]);

			// cancel one task
			atp.Cancel(rgPtsk[3*i + 2]);

			// destroy completed tasks
			while (0 < atp.UlTasks() && GPOS_OK == atp.EresTimedWaitAny(&ptsk, 0))
			{
				GPOS_ASSERT(CTask::EtsCompleted == ptsk->Ets() || ptsk->FCanceled());

				atp.Destroy(ptsk);
			}
		}

		// ATP cancels running task
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::Unittest_ExecuteWaitFunc
//
//	@doc:
//		Execute *wait* functions with/without cancel
//
//---------------------------------------------------------------------------
void
CAutoTaskProxyTest::Unittest_ExecuteWaitFunc
	(
	CAutoTaskProxy &atp,
	CTask *ptsk,
	BOOL fInvokeCancel,
	EWaitType ewtCur
	)
{
	GPOS_ASSERT(EwtInvalid < ewtCur && EwtSentinel > ewtCur);

	// loop until the task is running or finished
	while (!(ptsk->FRunning() || ptsk->FFinished()))
	{
		clib::USleep(1000);
	}

	// cancel the task or not
	if (fInvokeCancel)
	{
		GPOS_ASSERT(ptsk->FRunning());
		
		// cancel the running task
		atp.Cancel(ptsk);
	}

	GPOS_TRY
	{
		CTask* finishedTask = NULL;

		// invoke the corresponding *wait* function
		switch (ewtCur)
		{
		case EwtWait:
			atp.Wait(ptsk);
			break;
		case EwtWaitAny:
			atp.WaitAny(&finishedTask);
			break;
		case EwtTimedWait:
			atp.EresTimedWait(ptsk, gpos::ulong_max);
			break;
		case EwtTimedWaitAny:
			atp.EresTimedWaitAny(&finishedTask, gpos::ulong_max);
			break;
		case EwtDestroy:
			atp.Destroy(ptsk);
			break;
		default:
			GPOS_ASSERT(!"No such functions!");
		}

		// should NOT go here
		GPOS_ASSERT(!"No exceptions were propagated from sub-task!");
	}
	GPOS_CATCH_EX(ex)
	{
		//TODO: Find out what this is doing
		//It breaks the release build on Mac Mini OSX for concourse
		//gcc version Apple LLVM version 7.3.0 (clang-703.0.31)
		//GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiAbort);
		
		// reset error context
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::Unittest_PropagateErrorInternal
//
//	@doc:
//		Propagate error with/without invoking cancel by specific value
//
//---------------------------------------------------------------------------
void
CAutoTaskProxyTest::Unittest_PropagateErrorInternal
	(
	void *(*pfnTaskExec)(void*),
	BOOL fInvokeCancel
	)
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
		const ULONG ulTaskSize = EwtSentinel - 1;
		CTask *rgPtsk[ulTaskSize];
		ULLONG rgRes[ulTaskSize];

		for (ULONG ulIdx = 0; ulIdx < ulTaskSize; ++ulIdx)
		{
			rgPtsk[ulIdx] = atp.PtskCreate(pfnTaskExec, &rgRes[ulIdx]);
		}

		// start tasks
		for (ULONG ulIdx = 0; ulIdx < ulTaskSize; ++ulIdx)
		{
			atp.Schedule(rgPtsk[ulIdx]);
		}

		// invoke each *wait* function separately
		for(ULONG ulIdx = EwtInvalid + 1; ulIdx < EwtSentinel; ++ulIdx)
		{
			Unittest_ExecuteWaitFunc(atp, rgPtsk[ulIdx - 1], fInvokeCancel, static_cast<EWaitType>(ulIdx));
		}

	}
}

void* CAutoTaskProxyTest::Unittest_CheckExecuteErrorInternal(void* pv)
{
	GPOS_ASSERT(NULL != pv);
	STestThreadDescriptor *ptd = reinterpret_cast<STestThreadDescriptor *>(pv);
	CWorker wrkr(ptd->ulId, GPOS_WORKER_STACK_SIZE, (ULONG_PTR) &ptd);
	ptd->fException = false;
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	// scope for ATP
	{
		CAutoTaskProxy atp(ptd->m_pmp, pwpm, ptd->fPropagateException);
		CTask *task = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Error, NULL);
		GPOS_TRY
		{
			atp.Execute(task);
		}
		GPOS_CATCH_EX(ex)
		{
			GPOS_ASSERT(GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiAbort));
			ptd->fException = true;
		}
		GPOS_CATCH_END;
	}
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_PropagateCancelError
//
//	@doc:
//		Propagate error with invoking cancel method
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_PropagateCancelError()
{
	Unittest_PropagateErrorInternal(CAutoTaskProxyTest::PvUnittest_Infinite, true /* fInvokeCancel */);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_PropagateExecError
//
//	@doc:
//		Propagate error without invoking cancel method
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_PropagateExecError()
{
	Unittest_PropagateErrorInternal(CAutoTaskProxyTest::PvUnittest_Error, false /* fInvokeCancel */);

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_ExecuteError
//
//	@doc:
//		When task and autotask run in same thread by calling CAutoTaskProxy::Execute,
//		check whether exception propagation is controlled by fPropagateException
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_ExecuteError()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// Create new thread so worker for this new
	// thread is available to run the task ("CAutoTaskProxyTest::PvUnittest_Error")
	// from Unittest_CheckExecuteErrorInternal
	STestThreadDescriptor st;
	st.ulId = GPOS_THREAD_MAX + 1;
	st.m_pmp = pmp;
	st.fException = false;
	st.fPropagateException = true;

	INT res = pthread_create(&st.m_pthrdt, NULL /*pthrAttr*/, Unittest_CheckExecuteErrorInternal, &st);

	// check for error
	if (0 != res)
	{
		return GPOS_FAILED;
	}
	pthread_join(st.m_pthrdt, NULL);
	if (!st.fException)  // Expect to see exception propagated
	{
		return GPOS_FAILED;
	}

	// Disable Propagate
	st.fPropagateException = false;
	res = pthread_create(&st.m_pthrdt, NULL /*pthrAttr*/, Unittest_CheckExecuteErrorInternal, &st);

	// check for error
	if (0 != res)
	{
		return GPOS_FAILED;
	}
	pthread_join(st.m_pthrdt, NULL);
	if (st.fException)  // Don't expect to see exception propagated
	{
		return GPOS_FAILED;
	}
	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::EresUnittest_CheckErrorPropagation
//
//	@doc:
//		Check error propagation when disable/enable propagation
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoTaskProxyTest::EresUnittest_CheckErrorPropagation()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);

		CTask* ptsk = NULL;

		// error propagation is enable by default
		ptsk = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Error, NULL);
		atp.Schedule(ptsk);
		Unittest_ExecuteWaitFunc(atp, ptsk, false /* fInvokeCancel */, EwtWait);

		// disable error propagation
		ptsk = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Error, NULL);
		atp.SetPropagateError(false /* fPropagateError */);
		atp.Schedule(ptsk);

		// should not propagate error
		atp.Wait(ptsk);
		GPOS_ASSERT("Task completed");

		// enable error propagation
		ptsk = atp.PtskCreate(CAutoTaskProxyTest::PvUnittest_Error, NULL);
		atp.SetPropagateError(true /* fPropagateError */);
		atp.Schedule(ptsk);
		Unittest_ExecuteWaitFunc(atp, ptsk, false /* fInvokeCancel */, EwtWait);
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::PvUnittest_Short
//
//	@doc:
//		Short-running function
//
//---------------------------------------------------------------------------
void *
CAutoTaskProxyTest::PvUnittest_Short
	(
	void *pv
	)
{
	GPOS_ASSERT(CWorkerPoolManager::Pwpm()->FOwnedThread(gpos::pthread::PthrdtPthreadSelf()));

	ULLONG *pullSum = (ULLONG *) pv;
	*pullSum = 0;

	for (ULONG i = 0; i < 1000; i++)
	{
		*pullSum += i;
	}

	return pullSum;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::PvUnittest_Long
//
//	@doc:
//		Long-running function
//
//---------------------------------------------------------------------------
void *
CAutoTaskProxyTest::PvUnittest_Long
	(
	void *pv
	)
{
	GPOS_ASSERT(CWorkerPoolManager::Pwpm()->FOwnedThread(gpos::pthread::PthrdtPthreadSelf()));

	ULLONG *pullSum = (ULLONG *) pv;
	*pullSum = 0;

	for (ULONG i = 0; i < 1000000; i++)
	{
		if (0 == i % 1000)
		{
			GPOS_CHECK_ABORT;
		}

		*pullSum += i;
	}

	return pullSum;
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::PvUnittest_Infinite
//
//	@doc:
//		Infinite-running function
//
//---------------------------------------------------------------------------
void *
CAutoTaskProxyTest::PvUnittest_Infinite
	(
	void * // pv
	)
{
	GPOS_ASSERT(CWorkerPoolManager::Pwpm()->FOwnedThread(gpos::pthread::PthrdtPthreadSelf()));

	while (true)
	{
		GPOS_CHECK_ABORT;

		clib::USleep(1000);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTaskProxyTest::PvUnittest_Error
//
//	@doc:
//		Function with raising error
//
//---------------------------------------------------------------------------
void *
CAutoTaskProxyTest::PvUnittest_Error
	(
	void * // pv
	)
{
	ULONG sum = 0;

	// run in a short time
	for (ULONG i = 0; i < 1000; i++)
	{
		sum += i;
	}

	// throw abort exception
	GPOS_ABORT;

	return NULL;
}
// EOF

