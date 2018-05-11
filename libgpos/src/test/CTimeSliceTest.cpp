//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CTimeSliceTest
//
//	@doc:
//		Extended time slicing test
//---------------------------------------------------------------------------

#include "gpos/common/CMainArgs.h"
#include "gpos/task/CWorker.h"
#include "gpos/task/CWorkerPoolManager.h"
#include "gpos/test/CTimeSliceTest.h"
#include "gpos/test/CUnittest.h"


using namespace gpos;

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CTimeSliceTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTimeSliceTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CTimeSliceTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CTimeSliceTest::EresUnittest_CheckTimeSlice)
		};

	GPOS_RESULT eres = CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CTimeSliceTest::EresUnittest_Basic
//
//	@doc:
//		Basic test for time slice enforcement
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTimeSliceTest::EresUnittest_Basic()
{
	BOOL fTimeSlices = IWorker::m_enforce_time_slices;
	IWorker::m_enforce_time_slices = true;
	CWorker::Self()->ResetTimeSlice();

	GPOS_RESULT eres = GPOS_OK;

	GPOS_TRY
	{
		CTimerUser timer;
		timer.Restart();

		// loop until time slice is exceeded
		ULONG ulThreads = std::max((ULONG) 1, CWorkerPoolManager::WorkerPoolManager()->GetNumWorkersRunning());
		while (GPOS_CHECK_ABORT_MAX_INTERVAL_MSEC * ulThreads >= timer.ElapsedMS())
		{
			// burn some CPU
			for (ULONG i = 0, j = 0; i < 1000; i++)
			{
				j = (j + i) % 11;
			}
		}

		// must throw
		GPOS_CHECK_ABORT;

		eres = GPOS_FAILED;
	}
	GPOS_CATCH_EX(ex)
	{
		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiAbortTimeout))
		{
			eres = GPOS_OK;
		}
		else
		{
			eres = GPOS_FAILED;
		}

		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	IWorker::m_enforce_time_slices = fTimeSlices;
	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CTimeSliceTest::EresUnittest_CheckTimeSlice
//
//	@doc:
//		Check all unittests for proper time slicing
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTimeSliceTest::EresUnittest_CheckTimeSlice()
{
	// assemble -u option
	const CHAR *rgsz[] = {"", "-u"};

	BOOL fTimeSlices = IWorker::m_enforce_time_slices;
	IWorker::m_enforce_time_slices = true;

	GPOS_RESULT eres = GPOS_OK;

	GPOS_TRY
	{
		CMainArgs ma(GPOS_ARRAY_SIZE(rgsz), rgsz, "u");
		CUnittest::Driver(&ma);

		eres = GPOS_OK;
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_RESET_EX;

		eres = GPOS_FAILED;
	}
	GPOS_CATCH_END;

	IWorker::m_enforce_time_slices = fTimeSlices;
	return eres;
}

#endif // GPOS_DEBUG

// EOF

