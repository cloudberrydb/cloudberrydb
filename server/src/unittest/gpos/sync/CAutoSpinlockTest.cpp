//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoSpinlockTest.cpp
//
//	@doc:
//		Tests for CAutoSpinlock
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/sync/CAutoSpinlockTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoSpinlock::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoSpinlockTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CAutoSpinlockTest::EresUnittest_LockRelease)
#ifdef GPOS_DEBUG
		,
		GPOS_UNITTEST_FUNC_ASSERT(CAutoSpinlockTest::EresUnittest_SelfDeadlock),
		GPOS_UNITTEST_FUNC(CAutoSpinlockTest::EresUnittest_Yield),
		GPOS_UNITTEST_FUNC_ASSERT(CAutoSpinlockTest::EresUnittest_Rank)
#endif // GPOS_DEBUG
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoSpinlockTest::EresUnittest_LockRelease
//
//	@doc:
//		Simple acquiring of lock, releasing, scoping of action
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoSpinlockTest::EresUnittest_LockRelease()
{
	CSpinlockDummy slock;

	{ // scope for auto spinlock

		CAutoSpinlock alockOne(slock);
		alockOne.Lock();
		alockOne.Unlock();

		{
			CAutoSpinlock alockTwo(slock);
			alockTwo.Lock();
		}

		alockOne.Lock();
	}

	return GPOS_OK;
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CAutoSpinlockTest::EresUnittest_SelfDeadlock
//
//	@doc:
//		Attempt acquiring an slock twice
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoSpinlockTest::EresUnittest_SelfDeadlock()
{
	CSpinlockDummy slock;

	{
		CAutoSpinlock alock(slock);
		alock.Lock();

		// this must assert on the spinlock level
		alock.Lock();
	}

	return GPOS_FAILED;
}



//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::EresUnittest_Yield
//
//	@doc:
//		try yielding while holding a spinlock
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoSpinlockTest::EresUnittest_Yield()
{
	CSpinlockDummy slock;

	{
		CAutoSpinlock alock(slock);
		alock.Lock();

		// yield must assert
		GPOS_CHECK_ABORT;
	}

	return GPOS_OK;
}



//---------------------------------------------------------------------------
//	@function:
//		CSpinlock::EresUnittest_Rank
//
//	@doc:
//		try acquiring a spinlock of incorrect rank
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoSpinlockTest::EresUnittest_Rank()
{
	CSpinlockDummyLo slockLo;
	CSpinlockDummyHi slockHi;

	{
		CAutoSpinlock alockHi(slockHi);
		alockHi.Lock();

		CAutoSpinlock alockLo(slockLo);
		alockLo.Lock();

		// release in reverse order
		alockLo.Unlock();
		alockHi.Unlock();

		// try acquiring in incorrect order
		alockLo.Lock();
		alockHi.Lock();
	}

	return GPOS_OK;
}

#endif // GPOS_DEBUG

// EOF

