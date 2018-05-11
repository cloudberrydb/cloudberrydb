//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSpinlock.h
//
//	@doc:
//		Test for CSpinlock
//---------------------------------------------------------------------------
#ifndef GPOS_CSpinlockTest_H
#define GPOS_CSpinlockTest_H

#include "gpos/base.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSpinlockTest
	//
	//	@doc:
	//		Unittests for spinlocks
	//
	//---------------------------------------------------------------------------
	class CSpinlockTest
	{

		public:

			// unittests
			static GPOS_RESULT EresUnittest();

			static GPOS_RESULT EresUnittest_LockRelease();
			static GPOS_RESULT EresUnittest_Concurrency();
			static void *PvUnittest_ConcurrencyRun(void *);
#ifdef GPOS_DEBUG
			static GPOS_RESULT EresUnittest_SelfDeadlock();
			static GPOS_RESULT EresUnittest_LockedDestruction();
			static GPOS_RESULT EresUnittest_Allocation();
#endif // GPOS_DEBUG

	}; // class CSpinlockTest
}

#endif // !GPOS_CSpinlockTest_H

// EOF

