//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CMutexTest.cpp
//
//	@doc:
//		Tests for CMutex and others
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/sync/CAutoMutex.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/sync/CMutexTest.h"

#define GPOS_MUTEX_THREADS	8

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMutexTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMutexTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMutexTest::EresUnittest_LockRelease),
		GPOS_UNITTEST_FUNC(CMutexTest::EresUnittest_Recursion),
		GPOS_UNITTEST_FUNC(CMutexTest::EresUnittest_Concurrency)
#ifdef GPOS_DEBUG
		,
		GPOS_UNITTEST_FUNC(CMutexTest::EresUnittest_SelfDeadlock)
#endif // GPOS_DEBUG
		};
		
	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CMutexTest::EresUnittest_LockRelease
//
//	@doc:
//		Simple acquiring of lock, releasing, scoping of action
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMutexTest::EresUnittest_LockRelease()
{
	// simple locking
	CMutex mutex;
	mutex.Lock();

	GPOS_ASSERT(mutex.IsOwned());

	mutex.Unlock();
	
	GPOS_ASSERT(!mutex.IsOwned());
	
	(void)mutex.TryLock();
	
	GPOS_ASSERT(mutex.IsOwned());
	
	mutex.Unlock();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMutexTest::EresUnittest_Recursion
//
//	@doc:
//		Simple lock and unlock for recursive mutex
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMutexTest::EresUnittest_Recursion()
{
	CMutexRecursive mutex;
	ULONG c = 100;

	for(ULONG i = 0; i < c; i++)
	{
		mutex.Lock();
	}

	for(ULONG i = 0; i < c; i++)
	{
		GPOS_ASSERT(mutex.IsOwned());
		mutex.Unlock();
	}

	GPOS_ASSERT(!mutex.IsOwned());

	return GPOS_OK;

}



//---------------------------------------------------------------------------
//	@function:
//		CMutexTest::EresUnittest_Concurrency
//
//	@doc:
//		Spawn a number of threads to loop on a mutex
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMutexTest::EresUnittest_Concurrency()
{
	CMutexOS mutex;

	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *mp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::WorkerPoolManager();

	// scope for tasks
	{
		CAutoTaskProxy atp(mp, pwpm);
		CTask *rgPtsk[GPOS_MUTEX_THREADS];

		for (ULONG i = 0; i < GPOS_MUTEX_THREADS; i++)
		{
			rgPtsk[i] = atp.Create(CMutexTest::PvUnittest_ConcurrencyRun, &mutex);
		}

		for (ULONG i = 0; i < GPOS_MUTEX_THREADS; i++)
		{
			GPOS_CHECK_ABORT;

			atp.Schedule(rgPtsk[i]);
		}

		for (ULONG i = 0; i < GPOS_MUTEX_THREADS; i++)
		{
			GPOS_CHECK_ABORT;

			atp.Wait(rgPtsk[i]);
		}
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMutexTest::PvUnittest_ConcurrencyRun
//
//	@doc:
//		concurrent mutex test
//
//---------------------------------------------------------------------------
void *
CMutexTest::PvUnittest_ConcurrencyRun
	(
	void *pv
	)
{
	CMutexOS *pmutex = (CMutexOS*)pv;

	for (ULONG i = 0; i < 10000; i++)
	{
		GPOS_CHECK_ABORT;

		CAutoMutex am(*pmutex);
		am.Lock();
		am.Unlock();
		
		if (am.TryLock())
		{
			am.Unlock();
		}
	}

	return NULL;
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMutexTest::EresUnittest_SelfDeadlock
//
//	@doc:
//		Attempt to grab a mutex twice
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMutexTest::EresUnittest_SelfDeadlock()
{
	CMutex mutex;

	mutex.Lock();

	GPOS_TRY
	{
		mutex.Lock();

		return GPOS_FAILED;
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiAssert));
		mutex.Unlock();

		GPOS_RESET_EX;

		return GPOS_OK;
	}
	GPOS_CATCH_END;

	return GPOS_FAILED;
}


#endif // GPOS_DEBUG

// EOF

