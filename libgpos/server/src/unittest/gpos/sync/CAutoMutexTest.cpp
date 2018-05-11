//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoMutexTest.cpp
//
//	@doc:
//		Tests for CAutoMutex
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/sync/CAutoMutex.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/sync/CAutoMutexTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoMutexTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoMutexTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CAutoMutexTest::EresUnittest_LockRelease),
		GPOS_UNITTEST_FUNC(CAutoMutexTest::EresUnittest_Recursion)
#ifdef GPOS_DEBUG
		,
		GPOS_UNITTEST_FUNC_ASSERT(CAutoMutexTest::EresUnittest_SelfDeadlock),
		GPOS_UNITTEST_FUNC_ASSERT(CAutoMutexTest::EresUnittest_SelfDeadlockAttempt)
#endif // GPOS_DEBUG
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoMutexTest::EresUnittest_LockRelease
//
//	@doc:
//		Simple acquiring of lock, releasing, scoping of action
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoMutexTest::EresUnittest_LockRelease()
{
	// simple locking
	CMutex mutex;
	{
		CAutoMutex amx(mutex);

		amx.Lock();

		GPOS_ASSERT(mutex.IsOwned());

		amx.Unlock();
		
		// test trylock routine
		(void) amx.TryLock();
		GPOS_ASSERT(mutex.IsOwned());
		amx.Unlock();

		// dangling lock reference
		amx.Lock();
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoMutexTest::EresUnittest_Recursion
//
//	@doc:
//		Simple acquiring of lock, releasing for recursive mutex;  let auto
//		mechanism unwind all locks
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoMutexTest::EresUnittest_Recursion()
{
	CMutexRecursive mutex;

	{
		CAutoMutex amx(mutex);

		for(ULONG i = 0; i < 100; i++)
		{
			amx.Lock();
			GPOS_ASSERT(amx.TryLock());
		}

		GPOS_ASSERT(mutex.IsOwned());

	}

	return GPOS_OK;

}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CAutoMutexTest::EresUnittest_SelfDeadlock
//
//	@doc:
//		Attempt to grab a mutex twice
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoMutexTest::EresUnittest_SelfDeadlock()
{
	CMutex mutex;
	{
		CAutoMutex amx(mutex);

		amx.Lock();

		// this must throw
		amx.Lock();

	}

	return GPOS_FAILED;
}



//---------------------------------------------------------------------------
//	@function:
//		CAutoMutexTest::EresUnittest_SelfDeadlockAttempt
//
//	@doc:
//		Attempt to grab a mutex with trylock
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoMutexTest::EresUnittest_SelfDeadlockAttempt()
{
	CMutex mutex;
	{
		CAutoMutex amx(mutex);
		
		amx.Lock();
		
		// this must throw
		(void) amx.TryLock();
		
	}
	
	return GPOS_FAILED;
}


#endif // GPOS_DEBUG

// EOF

