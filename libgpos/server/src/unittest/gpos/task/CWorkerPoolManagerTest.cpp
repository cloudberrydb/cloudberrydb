//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CWorkerPoolManagerTest.h
//
//	@doc:
//		Test for CAutoTaskProxy
//---------------------------------------------------------------------------

#include "gpos/common/CAutoRg.h"
#include "gpos/common/CWallClock.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/task/CWorkerPoolManagerTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CWorkerPoolManagerTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CWorkerPoolManagerTest::EresUnittest_Performance),
		GPOS_UNITTEST_FUNC(CWorkerPoolManagerTest::EresUnittest_Stress)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::EresUnittest_Performance
//
//	@doc:
//		Compare iteratively running a function in the same task with
//		creating a new task for each iteration.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CWorkerPoolManagerTest::EresUnittest_Performance()
{
	GPOS_TRACE(GPOS_WSZ_LIT("Compare worker pool performance :"));
	GPOS_TRACE(GPOS_WSZ_LIT("\t- Short-running task : "));

	// test using one worker
	Unittest_TestTaskPerformance
			(
			1 /*workers*/,
			1000 /*iterations*/,
			CAutoTaskProxyTest::PvUnittest_Short,
			CWorkerPoolManagerTest::PvUnittest_ShortRepeated
			);

	// test using multiple workers
	Unittest_TestTaskPerformance
			(
			10 /*workers*/,
			1000 /*iterations*/,
			CAutoTaskProxyTest::PvUnittest_Short,
			CWorkerPoolManagerTest::PvUnittest_ShortRepeated
			);

	GPOS_TRACE(GPOS_WSZ_LIT("\t- Long-running task : "));

	// test using one worker
	Unittest_TestTaskPerformance
			(
			1 /*workers*/,
			40 /*iterations*/,
			CAutoTaskProxyTest::PvUnittest_Long,
			CWorkerPoolManagerTest::PvUnittest_LongRepeated
			);

	// test using multiple workers
	Unittest_TestTaskPerformance
			(
			10 /*workers*/,
			40 /*iterations*/,
			CAutoTaskProxyTest::PvUnittest_Long,
			CWorkerPoolManagerTest::PvUnittest_LongRepeated
			);


	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::EresUnittest_Stress
//
//	@doc:
//		Stress worker pool using large number of tasks and workers
//
//---------------------------------------------------------------------------
GPOS_RESULT
CWorkerPoolManagerTest::EresUnittest_Stress()
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	ULONG ulWorkersMin = pwpm->UlWorkersMin();
	ULONG ulWorkersMax = pwpm->UlWorkersMax();

#ifdef GPOS_DEBUG
	BOOL fEnforceTimeSlices = CWorker::m_fEnforceTimeSlices;
#endif // GPOS_DEBUG

	GPOS_TRY
	{
		GPOS_TRACE(GPOS_WSZ_LIT("Stress worker pool : "));

#ifdef GPOS_DEBUG
		CWorker::m_fEnforceTimeSlices = false;
#endif // GPOS_DEBUG

		// task function
		void *(*func)(void *);
		func = CAutoTaskProxyTest::PvUnittest_Short;

		// 1 task per worker
		CWorkerPoolManagerTest::Unittest_Stress(10, 10, func);
		CWorkerPoolManagerTest::Unittest_Stress(100, 100, func);

		// 10 tasks per worker
		CWorkerPoolManagerTest::Unittest_Stress(10, 100, func);
		CWorkerPoolManagerTest::Unittest_Stress(100, 1000, func);
	}
	GPOS_CATCH_EX(ex)
	{
		// restore worker count
		pwpm->SetWorkersMin(ulWorkersMin);
		pwpm->SetWorkersMax(ulWorkersMax);

#ifdef GPOS_DEBUG
		CWorker::m_fEnforceTimeSlices = fEnforceTimeSlices;
#endif // GPOS_DEBUG

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// restore worker count
	pwpm->SetWorkersMin(ulWorkersMin);
	pwpm->SetWorkersMax(ulWorkersMax);

#ifdef GPOS_DEBUG
	CWorker::m_fEnforceTimeSlices = fEnforceTimeSlices;
	CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG

	while (ulWorkersMax < pwpm->UlWorkers())
	{
		clib::USleep(1000);
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::Unittest_TestTaskPerformance
//
//	@doc:
//		Compare executing multiple instances of a function in a single task
//		with executing each instance in a separate task;
//
//---------------------------------------------------------------------------
void
CWorkerPoolManagerTest::Unittest_TestTaskPerformance
	(
	ULONG ulWorkers,
	ULONG ulIterCnt,
	void *funcSingle(void *),
	void *funcRepeated(void *)
	)
{
	ULONG timeSingle = 0;
	ULONG timeMulti = 0;

	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *pmp = amp.Pmp();

	// scope for clock
	{
		CWallClock clock;

		// execute multiple iterations in each task
		Unittest_TestSingleTaskPerformance(pmp, ulWorkers, ulIterCnt, funcRepeated);

		timeSingle = clock.UlElapsedMS();
	}

	// scope for clock
	{
		CWallClock clock;

		// execute one iteration in each task
		Unittest_TestMultiTaskPerformance(pmp, ulWorkers, ulIterCnt, funcSingle);

		timeMulti = clock.UlElapsedMS();
	}

	// print results
	GPOS_TRACE_FORMAT
		(
		"\t\t* %d worker(s):  	1 task: %dms   %d tasks: %dms ",
		ulWorkers,
		timeSingle,
		ulIterCnt,
		timeMulti
		)
		;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::Unittest_TestSingleTaskPerformance
//
//	@doc:
//		Execute multiple instances of a function in each task
//
//---------------------------------------------------------------------------
void
CWorkerPoolManagerTest::Unittest_TestSingleTaskPerformance
	(
	IMemoryPool *pmp,
	ULONG ulWorkers,
	ULONG ulIterCnt,
	void *funcRepeated(void *)
	)
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	CAutoTaskProxy atp(pmp, pwpm);
	CAutoRg<CTask *> argPtsk;
	argPtsk = GPOS_NEW_ARRAY(pmp, CTask*, ulWorkers);

	ULONG ulIterCntPerWorkers = ulIterCnt / ulWorkers;

	for (ULONG i = 0; i < ulWorkers; i++)
	{
		argPtsk[i] = atp.PtskCreate(funcRepeated, &ulIterCntPerWorkers);
	}

	for (ULONG i = 0; i < ulWorkers; i++)
	{
		atp.Schedule(argPtsk[i]);
	}

	for (ULONG i = 0; i < ulWorkers; i++)
	{
		GPOS_CHECK_ABORT;

		atp.Wait(argPtsk[i]);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::Unittest_TestMultiTaskPerformance
//
//	@doc:
//		Execute one function per task, use many tasks
//
//---------------------------------------------------------------------------
void
CWorkerPoolManagerTest::Unittest_TestMultiTaskPerformance
	(
	IMemoryPool *pmp,
	ULONG ulWorkers,
	ULONG ulIterCnt,
	void *funcSingle(void *)
	)
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	CAutoTaskProxy atp(pmp, pwpm);

	ULONG ulIterCntPerWorkers = ulIterCnt / ulWorkers;

	for (ULONG i = 0; i < ulIterCntPerWorkers; i++)
	{
		CAutoRg<CTask *> argPtsk;
		argPtsk = GPOS_NEW_ARRAY(pmp, CTask*, ulWorkers);
		ULLONG ulRes;

		for (ULONG i = 0; i < ulWorkers; i++)
		{
			argPtsk[i] = atp.PtskCreate(funcSingle, &ulRes);
		}

		for (ULONG i = 0; i < ulWorkers; i++)
		{
			atp.Schedule(argPtsk[i]);
		}

		for (ULONG i = 0; i < ulWorkers; i++)
		{
			GPOS_CHECK_ABORT;

			atp.Wait(argPtsk[i]);

			atp.Destroy(argPtsk[i]);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::Unittest_Stress
//
//	@doc:
//		Stress worker pool using a given combination of tasks and worker;
//
//---------------------------------------------------------------------------
void
CWorkerPoolManagerTest::Unittest_Stress
	(
	ULONG ulWorkers,
	ULONG culTskCnt,
	void *func(void *)
	)
{
	ULONG time = 0;

	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *pmp = amp.Pmp();

	// set worker count
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	pwpm->SetWorkersMin(ulWorkers);
	pwpm->SetWorkersMax(ulWorkers);

	// test convergence
	while (ulWorkers != pwpm->UlWorkers())
	{
		clib::USleep(1000);
	}

	clib::USleep(1000);

	// execute tasks
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CAutoRg<CTask *> argPtsk;
		argPtsk = GPOS_NEW_ARRAY(pmp, CTask*, culTskCnt);
		ULLONG ulSum = 0;

		CWallClock clock;

		for (ULONG i = 0; i < culTskCnt; i++)
		{
			argPtsk[i] = atp.PtskCreate(func, &ulSum);
		}

		for (ULONG i = 0; i < culTskCnt; i++)
		{
			atp.Schedule(argPtsk[i]);
		}

		for (ULONG i = 0; i < culTskCnt; i++)
		{
			atp.Wait(argPtsk[i]);
		}

		time = clock.UlElapsedMS();
	}

	// print results
	GPOS_TRACE_FORMAT("\t* %d worker(s) - %d tasks: %dms", ulWorkers, culTskCnt, time);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::PvUnittest_RepeatedShort
//
//	@doc:
//		Repeat short-running function
//
//---------------------------------------------------------------------------
void *
CWorkerPoolManagerTest::PvUnittest_ShortRepeated
	(
	void *pv
	)
{
	const ULONG culCnt = *(ULONG *) pv;

	for (ULONG i = 0; i < culCnt; i++)
	{
		GPOS_CHECK_ABORT;

		ULLONG ullSum = 0;
		CAutoTaskProxyTest::PvUnittest_Short(&ullSum);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerPoolManagerTest::PvUnittest_RepeatedLong
//
//	@doc:
//		Repeat short-running function
//
//---------------------------------------------------------------------------
void *
CWorkerPoolManagerTest::PvUnittest_LongRepeated
	(
	void *pv
	)
{
	const ULONG culCnt = *(ULONG *) pv;

	for (ULONG i = 0; i < culCnt; i++)
	{
		GPOS_CHECK_ABORT;

		ULLONG ullSum = 0;
		CAutoTaskProxyTest::PvUnittest_Long(&ullSum);
	}

	return NULL;
}

// EOF

