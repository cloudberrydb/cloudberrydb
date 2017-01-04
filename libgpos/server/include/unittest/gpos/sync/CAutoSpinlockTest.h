//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoSpinlockTest.h
//
//	@doc:
//		Test for CAutoSpinlock
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoSpinlockTest_H
#define GPOS_CAutoSpinlockTest_H

#include "gpos/sync/CSpinlock.h"


namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoSpinlockTest
	//
	//	@doc:
	//		Static unit test class for auto spinlocks
	//
	//---------------------------------------------------------------------------
	class CAutoSpinlockTest
	{

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_LockRelease();
#ifdef GPOS_DEBUG
			static GPOS_RESULT EresUnittest_SelfDeadlock();
			static GPOS_RESULT EresUnittest_Yield();
			static GPOS_RESULT EresUnittest_Rank();
#endif // GPOS_DEBUG

	}; // class CAutoSpinlockTest
}

#endif // !GPOS_CAutoSpinlockTest_H

// EOF

