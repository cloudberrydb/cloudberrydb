//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CMutexTest.h
//
//	@doc:
//      Unit test for CMutex
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMutexTest_H
#define GPOS_CMutexTest_H

#include "gpos/types.h"
#include "gpos/common/CList.h"
#include "gpos/task/IWorker.h"
#include "gpos/task/CWorkerId.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMutexTest
	//
	//	@doc:
	//		Static unit tests
	//
	//---------------------------------------------------------------------------
	class CMutexTest
	{
		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_LockRelease();
			static GPOS_RESULT EresUnittest_Recursion();
			static GPOS_RESULT EresUnittest_Concurrency();
			static void *PvUnittest_ConcurrencyRun(void *);

#ifdef GPOS_DEBUG
			static GPOS_RESULT EresUnittest_SelfDeadlock();
#endif // GPOS_DEBUG

	}; // CMutexTest
}

#endif // !GPOS_CMutexTest_H

// EOF

