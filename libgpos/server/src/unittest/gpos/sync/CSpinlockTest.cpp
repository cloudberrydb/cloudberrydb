//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSpinlockTest.cpp
//
//	@doc:
//		Tests for CSpinlock
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/sync/CSpinlock.h"
#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/sync/CSpinlockTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSpinlockTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CSpinlockTest::EresUnittest_LockRelease),
		GPOS_UNITTEST_FUNC(CSpinlockTest::EresUnittest_Concurrency)
#ifdef GPOS_DEBUG
		,
		GPOS_UNITTEST_FUNC_ASSERT(CSpinlockTest::EresUnittest_SelfDeadlock),
		GPOS_UNITTEST_FUNC_ASSERT(CSpinlockTest::EresUnittest_LockedDestruction),
		GPOS_UNITTEST_FUNC_ASSERT(CSpinlockTest::EresUnittest_Allocation)
#endif // GPOS_DEBUG
		};
		
	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::EresUnittest_LockRelease
//
//	@doc:
//		Simple acquiring of lock, releasing, scoping of action
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSpinlockTest::EresUnittest_LockRelease()
{
	CSpinlockDummy slock;

	slock.Lock();
	slock.Unlock();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::EresUnittest_Concurrency
//
//	@doc:
//		Spawn a number of threads to loop on a spinlock
//
//---------------------------------------------------------------------------
#define GPOS_SLOCK_THREADS	10
GPOS_RESULT
CSpinlockTest::EresUnittest_Concurrency()
{
#ifdef GPOS_DEBUG
	if (IWorker::m_enforce_time_slices)
 	{
 		return GPOS_OK;
	}
#endif // GPOS_DEBUG

	CSpinlockDummy slock;

	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	CMemoryPool *mp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

	// scope for tasks
	{
		CAutoTaskProxy atp(mp, pwpm);
		CTask *rgPtsk[GPOS_SLOCK_THREADS];

		for (ULONG i = 0; i < GPOS_SLOCK_THREADS; i++)
		{
			rgPtsk[i] = atp.Create(CSpinlockTest::PvUnittest_ConcurrencyRun, &slock);
		}

		for (ULONG i = 0; i < GPOS_SLOCK_THREADS; i++)
		{
			GPOS_CHECK_ABORT;

			atp.Schedule(rgPtsk[i]);
		}

		for (ULONG i = 0; i < GPOS_SLOCK_THREADS; i++)
		{
			GPOS_CHECK_ABORT;

			atp.Wait(rgPtsk[i]);
		}
	}

	return GPOS_OK;
}



//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::PvUnittest_ConcurrencyRun
//
//	@doc:
//		thread routine
//
//---------------------------------------------------------------------------
void *
CSpinlockTest::PvUnittest_ConcurrencyRun
	(
	void *pv
	)
{

	CSpinlockDummy *pslock = (CSpinlockDummy*)pv;

	for (ULONG i = 0; i < 10000; i++)
	{
		if (0 == i % 100)
		{
			GPOS_CHECK_ABORT;
		}

		CAutoSpinlock alock(*pslock);
		alock.Lock();
		alock.Unlock();
	}

	return NULL;
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::EresUnittest_SelfDeadlock
//
//	@doc:
//		Attempt acquiring an slock twice
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSpinlockTest::EresUnittest_SelfDeadlock()
{
	CSpinlockDummy slock;
	slock.Lock();

	// this must assert
	GPOS_TRY
	{
		slock.Lock();
	}
	GPOS_CATCH_EX(ex)
	{
		// release before going out of scope by way of exception
		slock.Unlock();

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	return GPOS_FAILED;
}


//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::EresUnittest_LockedDestruction
//
//	@doc:
//		Go out of scope while the spinlock is still being held
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSpinlockTest::EresUnittest_LockedDestruction()
{
	{
		CSpinlockDummy slock;
		slock.Lock();
	}

	return GPOS_FAILED;
}



//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::EresUnittest_Allocation
//
//	@doc:
//		Try to allocate memory while holding a spinlock
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSpinlockTest::EresUnittest_Allocation()
{
	// create mem pool for 1KB
	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	CMemoryPool *mp = amp.Pmp();

	CSpinlockDummy slock;
	{
		CAutoSpinlock alock(slock);
		alock.Lock();

		GPOS_NEW_ARRAY(mp, BYTE, 10);
	}

	return GPOS_FAILED;
}


#endif // GPOS_DEBUG

// EOF

