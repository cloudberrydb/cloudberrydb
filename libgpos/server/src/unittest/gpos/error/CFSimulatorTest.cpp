//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CFSimulatorTest.cpp
//
//	@doc:
//		Tests for
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/error/CFSimulator.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/error/CFSimulatorTest.h"

using namespace gpos;

#ifdef GPOS_FPSIMULATOR

//---------------------------------------------------------------------------
//	@function:
//		CFSimulator::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CFSimulatorTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CFSimulatorTest::EresUnittest_BasicTracking),
		GPOS_UNITTEST_FUNC(CFSimulatorTest::EresUnittest_OOM)
		};
		
	// ignore this test for FP simulation and time slicing check
	if (CFSimulator::FSimulation() ||
	    IWorker::m_enforce_time_slices)
	{
		return GPOS_OK;
	}

	// set test flag in this scope
	CAutoTraceFlag atf(EtraceTest, true);

	GPOS_RESULT eres = CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CFSimulatorTest::EresUnittest_BasicTracking
//
//	@doc:
//		Register a single occurrance
//
//---------------------------------------------------------------------------
GPOS_RESULT
CFSimulatorTest::EresUnittest_BasicTracking()
{
	BOOL fThrown = false;
	static ULONG ul = 0;
 	if (10 == ul)
 	{
 		return GPOS_OK;
	}

	GPOS_TRY
	{
		GPOS_SIMULATE_FAILURE(EtraceTest, CException::ExmaSystem, CException::ExmiOOM);
	}
	GPOS_CATCH_EX(ex)
	{
		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			// suspend CFA
			CAutoSuspendAbort asa;

			GPOS_TRACE_FORMAT("%d: Caught expected exception.", ul);

			fThrown = true;
		}
		
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	++ul;
	EresUnittest_BasicTracking();

	if (fThrown)
	{
		return GPOS_OK;
	}

	return GPOS_FAILED;
}



//---------------------------------------------------------------------------
//	@function:
//		CFSimulatorTest::EresOOM
//
//	@doc:
//		Simulate an OOM failure
//
//---------------------------------------------------------------------------
GPOS_RESULT
CFSimulatorTest::EresUnittest_OOM()
{
	// create memory pool of 128KB
	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *mp = amp.Pmp();

	GPOS_RESULT eres = GPOS_FAILED;

	// enable OOM simulation
	CAutoTraceFlag atf(EtraceSimulateOOM, true);

	GPOS_TRY
	{
		// attempt allocation
		GPOS_NEW_ARRAY(mp, CHAR, 1234);
	}
	GPOS_CATCH_EX(ex)
	{
		// must throw
		if(GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			eres = GPOS_OK;
		}
		
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	return eres;
}


#endif // GPOS_FPSIMULATOR

// EOF

