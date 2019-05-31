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
	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();
	ULONG workers_min = pwpm->GetMinWorkers();
	ULONG workers_max = pwpm->GetMaxWorkers();

#ifdef GPOS_DEBUG
	BOOL fEnforceTimeSlices = CWorker::m_enforce_time_slices;
#endif // GPOS_DEBUG

	GPOS_TRY
	{
		GPOS_TRACE(GPOS_WSZ_LIT("Stress worker pool : "));

#ifdef GPOS_DEBUG
		CWorker::m_enforce_time_slices = false;
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
		pwpm->SetMinWorkers(workers_min);
		pwpm->SetMaxWorkers(workers_max);

#ifdef GPOS_DEBUG
		CWorker::m_enforce_time_slices = fEnforceTimeSlices;
#endif // GPOS_DEBUG

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// restore worker count
	pwpm->SetMinWorkers(workers_min);
	pwpm->SetMaxWorkers(workers_max);

#ifdef GPOS_DEBUG
	CWorker::m_enforce_time_slices = fEnforceTimeSlices;
	CWorker::Self()->ResetTimeSlice();
#endif // GPOS_DEBUG

	while (workers_max < pwpm->GetNumWorkers())
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
	CMemoryPool *mp = amp.Pmp();

	// scope for clock
	{
		CWallClock clock;

		// execute multiple iterations in each task
		Unittest_TestSingleTaskPerformance(mp, ulWorkers, ulIterCnt, funcRepeated);

		timeSingle = clock.ElapsedMS();
	}

	// scope for clock
	{
		CWallClock clock;

		// execute one iteration in each task
		Unittest_TestMultiTaskPerformance(mp, ulWorkers, ulIterCnt, funcSingle);

		timeMulti = clock.ElapsedMS();
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
	CMemoryPool *mp,
	ULONG ulWorkers,
	ULONG ulIterCnt,
	void *funcRepeated(void *)
	)
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();
	CAutoTaskProxy atp(mp, pwpm);
	CAutoRg<CTask *> argPtsk;
	argPtsk = GPOS_NEW_ARRAY(mp, CTask*, ulWorkers);

	ULONG ulIterCntPerWorkers = ulIterCnt / ulWorkers;

	for (ULONG i = 0; i < ulWorkers; i++)
	{
		argPtsk[i] = atp.Create(funcRepeated, &ulIterCntPerWorkers);
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
	CMemoryPool *mp,
	ULONG ulWorkers,
	ULONG ulIterCnt,
	void *funcSingle(void *)
	)
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();
	CAutoTaskProxy atp(mp, pwpm);

	ULONG ulIterCntPerWorkers = ulIterCnt / ulWorkers;

	for (ULONG i = 0; i < ulIterCntPerWorkers; i++)
	{
		CAutoRg<CTask *> argPtsk;
		argPtsk = GPOS_NEW_ARRAY(mp, CTask*, ulWorkers);
		ULLONG ulRes;

		for (ULONG i = 0; i < ulWorkers; i++)
		{
			argPtsk[i] = atp.Create(funcSingle, &ulRes);
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
	CMemoryPool *mp = amp.Pmp();

	// set worker count
	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();
	pwpm->SetMinWorkers(ulWorkers);
	pwpm->SetMaxWorkers(ulWorkers);

	// test convergence
	while (ulWorkers != pwpm->GetNumWorkers())
	{
		clib::USleep(1000);
	}

	clib::USleep(1000);

	// execute tasks
	{
		CAutoTaskProxy atp(mp, pwpm);
		CAutoRg<CTask *> argPtsk;
		argPtsk = GPOS_NEW_ARRAY(mp, CTask*, culTskCnt);
		ULLONG ulSum = 0;

		CWallClock clock;

		for (ULONG i = 0; i < culTskCnt; i++)
		{
			argPtsk[i] = atp.Create(func, &ulSum);
		}

		for (ULONG i = 0; i < culTskCnt; i++)
		{
			atp.Schedule(argPtsk[i]);
		}

		for (ULONG i = 0; i < culTskCnt; i++)
		{
			atp.Wait(argPtsk[i]);
		}

		time = clock.ElapsedMS();
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

