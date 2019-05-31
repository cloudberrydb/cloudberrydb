//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSchedulerTest.cpp
//
//	@doc:
//		Test for CScheduler
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRg.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/search/CJobTest.h"
#include "gpopt/search/CJobFactory.h"
#include "gpopt/search/CScheduler.h"
#include "gpopt/search/CSchedulerContext.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"

#include "unittest/gpopt/engine/CEngineTest.h"
#include "unittest/gpopt/search/CSchedulerTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/md/CMDProviderMemory.h"

using namespace gpopt;
using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest
//
//	@doc:
//		Unittest for scheduler
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CSchedulerTest::EresUnittest_SpawnBasic),
//		GPOS_UNITTEST_FUNC(CSchedulerTest::EresUnittest_SpawnLight),
#ifndef GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CSchedulerTest::EresUnittest_SpawnHeavy),
#endif // GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CSchedulerTest::EresUnittest_QueueBasic),
		GPOS_UNITTEST_FUNC(CSchedulerTest::EresUnittest_QueueLight),
		GPOS_UNITTEST_FUNC(CSchedulerTest::EresUnittest_QueueHeavy),
		GPOS_UNITTEST_FUNC(CSchedulerTest::EresUnittest_BuildMemo),
		GPOS_UNITTEST_FUNC(EresUnittest_BuildMemoLargeJoins),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest_SpawnBasic
//
//	@doc:
//		Basic job scheduling test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest_SpawnBasic()
{
	ScheduleRoot
		(
		CJobTest::EttSpawn,
		1000 /*ulRounds*/,
		4 /*ulFanout*/,
		1 /*ulIters*/,
		2 /*ulWorkers*/
#ifdef GPOS_DEBUG
		,
		true /*fTrackingJobs*/
#endif // GPOS_DEBUG
		);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest_SpawnLight
//
//	@doc:
//		Test spawning of lightweight jobs
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest_SpawnLight()
{
#ifdef GPOS_DEBUG
	if (IWorker::m_enforce_time_slices)
 	{
 		return GPOS_OK;
	}
#endif // GPOS_DEBUG

	ScheduleRoot
		(
		CJobTest::EttSpawn,
		100000 /*ulRounds*/,
		10 /*ulFanout*/,
		1 /*ulIters*/,
		4 /*ulWorkers*/
		);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest_SpawnHeavy
//
//	@doc:
//		Test spawning of heavyweight jobs
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest_SpawnHeavy()
{
	ScheduleRoot
		(
		CJobTest::EttSpawn,
		10000 /*ulRounds*/,
		10 /*ulFanout*/,
		10000 /*ulIters*/,
		4 /*ulWorkers*/
		);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest_QueueBasic
//
//	@doc:
//		Basic test of job queueing
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest_QueueBasic()
{
	ScheduleRoot
		(
		CJobTest::EttStartQueue,
		1 /*ulRounds*/,
		4 /*ulFanout*/,
		100000 /*ulIters*/,
		2 /*ulWorkers*/
#ifdef GPOS_DEBUG
		,
		true /*fTrackingJobs*/
#endif // GPOS_DEBUG

		);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest_QueueLight
//
//	@doc:
//		Test queueing of lightweight job
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest_QueueLight()
{
	ScheduleRoot
		(
		CJobTest::EttStartQueue,
		1 /*ulRounds*/,
		100 /*ulFanout*/,
		1000 /*ulIters*/,
		4 /*ulWorkers*/
		);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest_QueueHeavy
//
//	@doc:
//		Test queueing of heavyweight job
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest_QueueHeavy()
{
	ScheduleRoot
		(
		CJobTest::EttStartQueue,
		1 /*ulRounds*/,
		100 /*ulFanout*/,
		10000000 /*ulIters*/,
		4 /*ulWorkers*/
		);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::ScheduleRoot
//
//	@doc:
//		Create scheduler and add root job
//
//---------------------------------------------------------------------------
void
CSchedulerTest::ScheduleRoot
	(
	CJobTest::ETestType ett,
	ULONG ulRounds,
	ULONG ulFanout,
	ULONG ulIters,
	ULONG ulWorkers
#ifdef GPOS_DEBUG
	,
	BOOL fTrackingJobs
#endif // GPOS_DEBUG
	)
{
	GPOS_ASSERT(CJobTest::EttSpawn == ett || CJobTest::EttStartQueue == ett);

	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	const ULONG ulJobs = ulRounds * ulFanout + 1;

	// initialize job factory
	CJobFactory jf(mp, ulJobs);

	// initialize scheduler
	CScheduler sched
				(
				mp,
				ulJobs,
				ulWorkers
#ifdef GPOS_DEBUG
				,
				fTrackingJobs
#endif // GPOS_DEBUG
				);

	// initialize engine
	CEngine eng(mp);

	GPOS_CHECK_ABORT;

	// add root job
	CJobTest *pjt = CJobTest::PjConvert(jf.PjCreate(CJob::EjtTest));
	CJobQueue jq;
	pjt->Init(ett, ulRounds, ulFanout, ulIters, &jq);
	pjt->ResetCnt();
	sched.Add(pjt, NULL);

	RunTasks(mp, &jf, &sched, &eng, ulWorkers);

	// print statistics
	sched.PrintStats();
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::RunTasks
//
//	@doc:
//		Run job execution tasks
//
//---------------------------------------------------------------------------
void
CSchedulerTest::RunTasks
	(
	CMemoryPool *mp,
	CJobFactory *pjf,
	CScheduler *psched,
	CEngine *peng,
	ULONG ulWorkers
	)
{
	// create task array
	CAutoRg<CTask*> a_rgptsk;
	a_rgptsk = GPOS_NEW_ARRAY(mp, CTask*, ulWorkers);

	// create scheduling contexts
	CAutoRg<CSchedulerContext> a_rgsc;
	a_rgsc = GPOS_NEW_ARRAY(mp, CSchedulerContext, ulWorkers);

	// scope for ATP
	{
		CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();
		CAutoTaskProxy atp(mp, pwpm);

		for (ULONG i = 0; i < ulWorkers; i++)
		{
			// initialize scheduling context
			a_rgsc[i].Init(mp, pjf, psched, peng);

			// create scheduling task
			a_rgptsk[i] = atp.Create(CScheduler::Run, &a_rgsc[i]);
		}

		// start tasks
		for (ULONG i = 0; i < ulWorkers; i++)
		{
			atp.Schedule(a_rgptsk[i]);
		}

		// wait for tasks to complete
		for (ULONG i = 0; i < ulWorkers; i++)
		{
			CTask *ptsk;
			atp.WaitAny(&ptsk);

			GPOS_CHECK_ABORT;
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest_BuildMemo
//
//	@doc:
//		Test of building memo from different input expressions
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest_BuildMemo()
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// array of relation names
	CWStringConst rgscRel[] =
	{
		GPOS_WSZ_LIT("Rel1"),
		GPOS_WSZ_LIT("Rel2"),
		GPOS_WSZ_LIT("Rel3"),
		GPOS_WSZ_LIT("Rel4"),
		GPOS_WSZ_LIT("Rel5"),
	};

	// array of relation IDs
	ULONG rgulRel[] =
	{
		GPOPT_TEST_REL_OID1,
		GPOPT_TEST_REL_OID2,
		GPOPT_TEST_REL_OID3,
		GPOPT_TEST_REL_OID4,
		GPOPT_TEST_REL_OID5,
	};

	// number of relations
	const ULONG ulRels = GPOS_ARRAY_SIZE(rgscRel);

	CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp);
	for (ULONG ul = 0; ul < ulRels; ul++)
	{
		(void) pbs->ExchangeSet(ul);
	}

	GPOS_RESULT eres = CEngineTest::EresOptimize(BuildMemoMultiThreaded, rgscRel, rgulRel, ulRels, pbs);
	pbs->Release();

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::EresUnittest_BuildMemoLargeJoins
//
//	@doc:
//		Test of building memo for a large number of joins
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSchedulerTest::EresUnittest_BuildMemoLargeJoins()
{
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// array of relation names
	CWStringConst rgscRel[] =
	{
		GPOS_WSZ_LIT("Rel1"),
		GPOS_WSZ_LIT("Rel2"),
		GPOS_WSZ_LIT("Rel3"),
		GPOS_WSZ_LIT("Rel4"),
		GPOS_WSZ_LIT("Rel5"),
		GPOS_WSZ_LIT("Rel6"),
		GPOS_WSZ_LIT("Rel7"),
		GPOS_WSZ_LIT("Rel8"),
		GPOS_WSZ_LIT("Rel9"),
		GPOS_WSZ_LIT("Rel10"),
	};

	// array of relation IDs
	ULONG rgulRel[] =
	{
		GPOPT_TEST_REL_OID1,
		GPOPT_TEST_REL_OID2,
		GPOPT_TEST_REL_OID3,
		GPOPT_TEST_REL_OID4,
		GPOPT_TEST_REL_OID5,
		GPOPT_TEST_REL_OID6,
		GPOPT_TEST_REL_OID7,
		GPOPT_TEST_REL_OID8,
		GPOPT_TEST_REL_OID9,
		GPOPT_TEST_REL_OID10,
	};

	// only optimize the last join expression
	CBitSet *pbs = GPOS_NEW(mp) CBitSet(mp);
	const ULONG ulRels =
#ifdef GPOS_DEBUG
		GPOS_ARRAY_SIZE(rgscRel) - 4;
#else
		GPOS_ARRAY_SIZE(rgscRel);
#endif // GPOS_DEBUG

	(void) pbs->ExchangeSet(ulRels - 1);

	GPOS_RESULT eres = CEngineTest::EresOptimize(BuildMemoMultiThreaded, rgscRel, rgulRel, ulRels, pbs);
	pbs->Release();

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CSchedulerTest::BuildMemoMultiThreaded
//
//	@doc:
//		Build memo using multiple threads
//
//---------------------------------------------------------------------------
void
CSchedulerTest::BuildMemoMultiThreaded
	(
	CMemoryPool *mp,
	CExpression *pexprInput,
	 CSearchStageArray *search_stage_array
	)
{
	CQueryContext *pqc = CTestUtils::PqcGenerate(mp, pexprInput);
	GPOS_CHECK_ABORT;

	// enable space pruning
	CAutoTraceFlag atf(EopttraceEnableSpacePruning, true /*value*/);

	CWStringDynamic str(mp);
	COstreamString oss(&str);
	oss << std::endl << std::endl;
	oss << "INPUT EXPRESSION:" <<std::endl;
	(void) pexprInput->OsPrint(oss);

	CEngine eng(mp);
	eng.Init(pqc, search_stage_array);
	eng.Optimize();

	CExpression *pexprPlan = eng.PexprExtractPlan();

	(void) pexprPlan->PrppCompute(mp, pqc->Prpp());

	oss << std::endl << "OUTPUT PLAN:" <<std::endl;
	(void) pexprPlan->OsPrint(oss);
	oss << std::endl << std::endl;

	GPOS_TRACE(str.GetBuffer());
	pexprPlan->Release();
	GPOS_DELETE(pqc);

	GPOS_CHECK_ABORT;
}

// EOF

