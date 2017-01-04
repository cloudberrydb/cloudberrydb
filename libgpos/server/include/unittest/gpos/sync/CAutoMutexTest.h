//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoMutexTest.h
//
//	@doc:
//		Test for CAutoMutex
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoMutexTest_H
#define GPOS_CAutoMutexTest_H

#include "gpos/base.h"
#include "gpos/sync/CMutex.h"


namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoMutexTest
	//
	//	@doc:
	//		Unit tests for auto mutex class
	//
	//---------------------------------------------------------------------------
	class CAutoMutexTest
	{
		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_LockRelease();
			static GPOS_RESULT EresUnittest_Recursion();
#ifdef GPOS_DEBUG
			static GPOS_RESULT EresUnittest_SelfDeadlock();
			static GPOS_RESULT EresUnittest_SelfDeadlockAttempt();
#endif // GPOS_DEBUG

	}; // CAutoMutexTest
}

#endif // !GPOS_CAutoMutexTest_H

// EOF

