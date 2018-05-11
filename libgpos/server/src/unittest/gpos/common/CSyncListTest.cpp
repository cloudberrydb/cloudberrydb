//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncListTest.cpp
//
//	@doc:
//		Tests for CSyncList
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CWorkerPoolManager.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CSyncListTest.h"

#define GPOS_SLIST_SIZE 10
#define GPOS_SLIST_STRESS_TASKS 10
#define GPOS_SLIST_STRESS_ITER 100000
#define GPOS_SLIST_STRESS_CFA 1000

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::EresUnittest
//
//	@doc:
//		Unittest for sync list
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSyncListTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CSyncListTest::EresUnittest_Basics),
		GPOS_UNITTEST_FUNC(CSyncListTest::EresUnittest_Concurrency),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::EresUnittest_Basics
//
//	@doc:
//		Various sync list operations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSyncListTest::EresUnittest_Basics()
{
	CSyncList<SElem> list;
	list.Init(GPOS_OFFSET(SElem, m_link));

	SElem rgelem[GPOS_SLIST_SIZE];

	// insert all elements
	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgelem); i++)
	{
		list.Push(&rgelem[i]);

		GPOS_ASSERT(GPOS_OK == list.Find(&rgelem[i]));
	}

#ifdef GPOS_DEBUG
	// scope for auto trace
	{
		CAutoMemoryPool amp;
		IMemoryPool *mp = amp.Pmp();

		CAutoTrace trace(mp);
		IOstream &os(trace.Os());

		os << GPOS_WSZ_LIT("Sync list contents:") << std::endl;
		list.OsPrint(os);
	}
#endif // GPOS_DEBUG

	// pop elements until empty
	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgelem); i++)
	{
#ifdef GPOS_DEBUG
		SElem *pe =
#endif // GPOS_DEBUG
			list.Pop();

		GPOS_ASSERT(pe == &rgelem[GPOS_ARRAY_SIZE(rgelem) - i - 1]);
	}
	GPOS_ASSERT(NULL == list.Pop());

	// insert all elements in reverse order
	for (ULONG i = GPOS_ARRAY_SIZE(rgelem); i > 0; i--)
	{
		list.Push(&rgelem[i - 1]);

		GPOS_ASSERT(GPOS_OK == list.Find(&rgelem[i - 1]));
	}

	// pop elements until empty
	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgelem); i++)
	{
#ifdef GPOS_DEBUG
		SElem *pe =
#endif // GPOS_DEBUG
			list.Pop();

		GPOS_ASSERT(pe == &rgelem[i]);
	}
	GPOS_ASSERT(NULL == list.Pop());

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::EresUnittest_Concurrency
//
//	@doc:
//		Concurrency test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSyncListTest::EresUnittest_Concurrency()
{
#ifdef GPOS_DEBUG
	if (IWorker::m_enforce_time_slices)
 	{
 		return GPOS_OK;
	}
#endif // GPOS_DEBUG

	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *mp = amp.Pmp();

	SElem rgelem[GPOS_SLIST_SIZE];
	CSyncList<SElem> list;
	list.Init(GPOS_OFFSET(SElem, m_link));

	// insert all elements
	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgelem); i++)
	{
		list.Push(&rgelem[i]);

		GPOS_ASSERT(GPOS_OK == list.Find(&rgelem[i]));
	}

	// pool of elements to add to the list
	CSyncPool<SElem> spe(mp, GPOS_SLIST_STRESS_TASKS * GPOS_SLIST_STRESS_ITER);
	spe.Init(GPOS_OFFSET(SElem, m_id));

	SArg arg(&list, &spe, GPOS_SLIST_STRESS_TASKS);

	// run concurrent tests
	ConcurrentPush(mp, &arg);
	ConcurrentPushPop(mp, &arg);
	ConcurrentPop(mp, &arg);

	// pop element until empty
	for (ULONG i = 0; i < GPOS_SLIST_SIZE; i++)
	{
#ifdef GPOS_DEBUG
		SElem *pe =
#endif // GPOS_DEBUG
			list.Pop();

		GPOS_ASSERT(NULL != pe);
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::ConcurrentPush
//
//	@doc:
//		Run push tasks in parallel
//
//---------------------------------------------------------------------------
void
CSyncListTest::ConcurrentPush
	(
	IMemoryPool *mp,
	SArg *parg
	)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != parg);

	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

	// scope for tasks
	{
		CAutoTaskProxy atp(mp, pwpm);
		CTask *rgptsk[GPOS_SLIST_STRESS_TASKS / 2];

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk); i++)
		{
			rgptsk[i] = atp.Create(RunPush, parg);
		}

		RunTasks(&atp, rgptsk, GPOS_ARRAY_SIZE(rgptsk));
	}
}



//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::ConcurrentPushPop
//
//	@doc:
//		Run push and pop tasks in parallel
//
//---------------------------------------------------------------------------
void
CSyncListTest::ConcurrentPushPop
	(
	IMemoryPool *mp,
	SArg *parg
	)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != parg);

	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

	// scope for tasks
	{
		CAutoTaskProxy atp(mp, pwpm);
		CTask *rgptsk[GPOS_SLIST_STRESS_TASKS];

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk) / 2; i++)
		{
			rgptsk[i] = atp.Create(RunPush, parg);
			rgptsk[i + GPOS_ARRAY_SIZE(rgptsk) / 2] = atp.Create(RunPop, parg);
		}

		RunTasks(&atp, rgptsk, GPOS_ARRAY_SIZE(rgptsk));
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::ConcurrentPop
//
//	@doc:
//		Run pop tasks in parallel
//
//---------------------------------------------------------------------------
void
CSyncListTest::ConcurrentPop
	(
	IMemoryPool *mp,
	SArg *parg
	)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != parg);

	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

	// scope for tasks
	{
		CAutoTaskProxy atp(mp, pwpm);
		CTask *rgptsk[GPOS_SLIST_STRESS_TASKS / 2];

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk); i++)
		{
			rgptsk[i] = atp.Create(RunPop, parg);
		}

		RunTasks(&atp, rgptsk, GPOS_ARRAY_SIZE(rgptsk));
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::RunTasks
//
//	@doc:
//		Schedule tasks and wait until they complete
//
//---------------------------------------------------------------------------
void
CSyncListTest::RunTasks
	(
	CAutoTaskProxy *patp,
	CTask **rgptsk,
	ULONG ulTasks
	)
{
	GPOS_ASSERT(NULL != patp);
	GPOS_ASSERT(NULL != rgptsk);

	for (ULONG i = 0; i < ulTasks; i++)
	{
		GPOS_CHECK_ABORT;

		patp->Schedule(rgptsk[i]);
	}

	for (ULONG i = 0; i < ulTasks; i++)
	{
		GPOS_CHECK_ABORT;

		patp->Wait(rgptsk[i]);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::RunPush
//
//	@doc:
//		Iteratively push elements to list
//
//---------------------------------------------------------------------------
void *
CSyncListTest::RunPush
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SArg *parg = reinterpret_cast<SArg *>(pv);

	for(ULONG i = 0; i < GPOS_SLIST_STRESS_ITER; i++)
	{
		if (0 == i % GPOS_SLIST_STRESS_CFA)
		{
			GPOS_CHECK_ABORT;
		}

		// retrieve element from pool
		SElem *pe = parg->m_psp->PtRetrieve();

		// add element to list
		parg->m_plist->Push(pe);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSyncListTest::RunPop
//
//	@doc:
//		Iteratively pop elements from list
//
//---------------------------------------------------------------------------
void *
CSyncListTest::RunPop
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	SArg *parg = reinterpret_cast<SArg *>(pv);

	for(ULONG i = 0; i < GPOS_SLIST_STRESS_ITER; i++)
	{
		if (0 == i % GPOS_SLIST_STRESS_CFA)
		{
			GPOS_CHECK_ABORT;
		}

		// remove element from list
		SElem *pe = parg->m_plist->Pop();

		// return element to pool
		parg->m_psp->Recycle(pe);

		GPOS_ASSERT(NULL != pe);
	}

	return NULL;
}


// EOF
