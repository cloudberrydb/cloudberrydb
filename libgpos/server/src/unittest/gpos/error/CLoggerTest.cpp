//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerTest
//
//	@doc:
//		Unit test for logger classes.
//---------------------------------------------------------------------------

#include "gpos/error/ILogger.h"
#include "gpos/error/CLoggerFile.h"
#include "gpos/error/CLoggerStream.h"
#include "gpos/error/CLoggerSyslog.h"
#include "gpos/io/ioutils.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/error/CLoggerTest.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CLoggerTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CLoggerTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CLoggerTest::EresUnittest_LoggerSyslog),
		GPOS_UNITTEST_FUNC(CLoggerTest::EresUnittest_LoggerStress),
		GPOS_UNITTEST_FUNC(CLoggerTest::EresUnittest_LoggerFile),
		};

	GPOS_RESULT eres = CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::EresUnittest_Basic
//
//	@doc:
//		Basic test for logging
//
//---------------------------------------------------------------------------
GPOS_RESULT
CLoggerTest::EresUnittest_Basic()
{
	// log trace messages
	GPOS_TRACE(GPOS_WSZ_LIT("Log trace message as built string"));
	GPOS_TRACE_FORMAT("Log trace message as %s %s", "formatted", "string");

	{
		// disable Abort simulation;
		// simulation leads to self-deadlock while trying to log injection
		CAutoTraceFlag atfSet(EtraceSimulateAbort, false);

		// log warning message
		GPOS_WARNING(CException::ExmaSystem, CException::ExmiDummyWarning, "Foo");
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::EresUnittest_LoggerFile
//
//	@doc:
//		Log to file
//
//---------------------------------------------------------------------------
GPOS_RESULT
CLoggerTest::EresUnittest_LoggerFile()
{
	CAutoMemoryPool am;
	IMemoryPool *pmp = am.Pmp();

	GPOS_TRY
	{
		// "/foobar" directory does not exist - this will throw an IOError
		CLoggerFile ploggerFile("/foobar", 10 /*ulFiles*/, 1024/*ulRotateThreshold*/);

		return GPOS_FAILED;
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiIOError));

		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	// use one worker, small file size to check rotation
	Unittest_TestLoggerFile
		(
		pmp,
		10     /*ulFiles*/,
		4096   /*ulRotateThreshold*/,
		1      /*ulWorkers*/,
		10000  /*ulIterations*/
		)
		;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::EresUnittest_Stress
//
//	@doc:
//		Log to file
//
//---------------------------------------------------------------------------
GPOS_RESULT
CLoggerTest::EresUnittest_LoggerStress()
{
	CAutoMemoryPool am;
	IMemoryPool *pmp = am.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	ULONG ulWorkersMin = pwpm->UlWorkersMin();
	ULONG ulWorkersMax = pwpm->UlWorkersMax();
	pwpm->SetWorkersMin(50);
	pwpm->SetWorkersMax(50);

	ULONG timeOne = 0;
	ULONG timeTen = 0;
	ULONG timeFifty = 0;

	const ULONG ulIterations = 10000;

	GPOS_TRY
	{

		CWallClock clock;

		Unittest_TestLoggerFile
			(
			pmp,
			10            /*ulFiles*/,
			10 * 1024     /*ulRotateThreshold*/,
			1             /*ulWorkers*/,
			ulIterations  /*ulIterations*/
			)
			;

		timeOne = clock.UlElapsedMS();

		clock.Restart();

		Unittest_TestLoggerFile
			(
			pmp,
			10           /*ulFiles*/,
			10 * 1024    /*ulRotateThreshold*/,
			10           /*ulWorkers*/,
			ulIterations /*ulIterations*/
			)
			;

		timeTen = clock.UlElapsedMS();

		clock.Restart();

		Unittest_TestLoggerFile
			(
			pmp,
			10           /*ulFiles*/,
			10 * 1024    /*ulRotateThreshold*/,
			50           /*ulWorkers*/,
			ulIterations /*ulIterations*/
			)
			;

		timeFifty = clock.UlElapsedMS();

		// print results
		GPOS_TRACE_FORMAT
			(
			"Log stress: 1 worker: %dms   10 workers: %dms   50 workers: %dms",
			timeOne,
			timeTen,
			timeFifty
			)
			;
	}
	GPOS_CATCH_EX(ex)
	{
		pwpm->SetWorkersMin(ulWorkersMin);
		pwpm->SetWorkersMax(ulWorkersMax);

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	pwpm->SetWorkersMin(ulWorkersMin);
	pwpm->SetWorkersMax(ulWorkersMax);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::EresUnittest_LoggerSyslog
//
//	@doc:
//		Log to file
//
//---------------------------------------------------------------------------
GPOS_RESULT
CLoggerTest::EresUnittest_LoggerSyslog()
{
	GPOS_SYSLOG_ALERT("This is test message 1 from GPOS to syslog");
	GPOS_SYSLOG_ALERT("This is test message 2 from GPOS to syslog");
	GPOS_SYSLOG_ALERT("This is test message 3 from GPOS to syslog");

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::Unittest_TestLoggerFile
//
//	@doc:
//		Write log messages to file using one or more workers
//
//---------------------------------------------------------------------------
void
CLoggerTest::Unittest_TestLoggerFile
	(
	IMemoryPool *pmp,
	ULONG ulFiles,
	ULONG ulRotateThreshold,
	ULONG ulWorkers,
	ULONG ulIterations
	)
{
	GPOS_ASSERT(0 < ulWorkers);

	// create unique directory name under /tmp
	CHAR sz[GPOS_FILE_NAME_BUF_SIZE];
	CStringStatic str(sz, GPOS_ARRAY_SIZE(sz));
	str.AppendFormat("/tmp/gpos_test_log.XXXXXX");

	// back up output log
	ILogger *plog = ITask::PtskSelf()->PlogOut();

	// create dir
	(void) ioutils::SzMkDTemp(sz);

	CLoggerFile *ploggerFile = NULL;

	GPOS_TRY
	{
		// initialize file logger
		ploggerFile = GPOS_NEW(pmp) CLoggerFile(str.Sz(), ulFiles, ulRotateThreshold);

		// set output log to syslog
		ITask::PtskSelf()->Ptskctxt()->SetLogOut(ploggerFile);

		if (1 == ulWorkers)
		{
			// log all messages using current worker
			PvUnittest_LogWrite(&ulIterations);
		}
		else
		{
			// create multiple logging tasks and execute them in parallel

			CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
			IMemoryPool *pmp = amp.Pmp();

			CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

			// scope for auto objects
			{
				CAutoTaskProxy atp(pmp, pwpm);
				CAutoRg<CTask *> argPtsk;
				argPtsk = GPOS_NEW_ARRAY(pmp, CTask*, ulWorkers);

				for (ULONG i = 0; i < ulWorkers; i++)
				{
					ULONG ulCount = ulIterations / ulWorkers;
					argPtsk[i] = atp.PtskCreate(PvUnittest_LogWrite, &ulCount);
				}

				for (ULONG i = 0; i < ulWorkers; i++)
				{
					atp.Schedule(argPtsk[i]);
				}

				for (ULONG i = 0; i < ulWorkers; i++)
				{
					GPOS_CHECK_ABORT;

					atp.Wait(argPtsk[i]);
				}
			}
		}

		PvUnittest_CheckFiles(str.Sz(), ulFiles);
	}
	GPOS_CATCH_EX(ex)
	{
		// restore output log
		ITask::PtskSelf()->Ptskctxt()->SetLogOut(plog);

		GPOS_DELETE(ploggerFile);

		PvUnittest_DeleteFiles(str.Sz(), ulFiles);

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// restore output log
	ITask::PtskSelf()->Ptskctxt()->SetLogOut(plog);

	GPOS_DELETE(ploggerFile);

	PvUnittest_DeleteFiles(str.Sz(), ulFiles);
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::PvUnittest_DeleteFiles
//
//	@doc:
//		Delete log files and directory
//
//---------------------------------------------------------------------------
void
CLoggerTest::PvUnittest_DeleteFiles
	(
	const CHAR *szDir,
	ULONG ulFiles
	)
{
	CAutoTraceFlag atf(EtraceSimulateIOError, false);

	// delete files in dir
	for (ULONG i = 0; i < ulFiles; i++)
	{
		CHAR szBuffer[GPOS_FILE_NAME_BUF_SIZE];
		CStringStatic strFile(szBuffer, GPOS_ARRAY_SIZE(szBuffer));

		if (i == 0)
		{
			strFile.AppendFormat("%s/log", szDir);
		}
		else
		{
			strFile.AppendFormat("%s/log.%d", szDir, i);
		}

		if (ioutils::FPathExist(strFile.Sz()))
		{
			ioutils::Unlink(strFile.Sz());
		}
	}

	// delete dir
	ioutils::RmDir(szDir);
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::PvUnittest_CheckFiles
//
//	@doc:
//		Check log files
//
//---------------------------------------------------------------------------
void
CLoggerTest::PvUnittest_CheckFiles
	(
	const CHAR *szDir,
	ULONG ulFiles
	)
{
	ULLONG ullFileSize = 0;

	// check log files; they should all have the same size apart from the first one
	for (ULONG i = 0; i < ulFiles; i++)
	{
		GPOS_CHECK_ABORT;

		CHAR szBuffer[GPOS_FILE_NAME_BUF_SIZE];
		CStringStatic strs(szBuffer, GPOS_ARRAY_SIZE(szBuffer));

		if (i == 0)
		{
			strs.AppendFormat("%s/log", szDir);
		}
		else
		{
			strs.AppendFormat("%s/log.%d", szDir, i);
		}

		if (0 < i)
		{
			if (0 == ullFileSize)
			{
				ullFileSize = ioutils::UllFileSize(strs.Sz());
			}
			else
			{
				GPOS_ASSERT(ullFileSize == ioutils::UllFileSize(strs.Sz()) && "Inconsistent log file size");
			}
		}
	}

	GPOS_ASSERT(0 < ullFileSize);
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerTest::PvUnittest_LogWrite
//
//	@doc:
//		Write multiple messages to log
//
//---------------------------------------------------------------------------
void *
CLoggerTest::PvUnittest_LogWrite
	(
	void *pv
	)
{
	ULONG ulCount = *(ULONG *)pv;

	for (ULONG i = 0; i < ulCount; i++)
	{
		GPOS_TRACE_FORMAT("************ Log test message %05d to file logger ***********", i);
	}

	return NULL;
}

// EOF

