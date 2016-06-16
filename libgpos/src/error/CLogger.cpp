//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLogger.cpp
//
//	@doc:
//		Partial implementation of interface class for logging
//---------------------------------------------------------------------------

#include "gpos/common/syslibwrapper.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CLogger.h"
#include "gpos/error/CLoggerStream.h"
#include "gpos/error/CLoggerSyslog.h"
#include "gpos/error/CMessageRepository.h"
#include "gpos/string/CWStringConst.h"
#include "gpos/sync/CAutoMutex.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CLogger::CLogger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogger::CLogger
	(
	EErrorInfoLevel eli
	)
	:
	ILogger(),
	m_wstrEntry
		(
		m_wszEntry,
		GPOS_ARRAY_SIZE(m_wszEntry)
		),
	m_wstrMsg
		(
		m_wszMsg,
		GPOS_ARRAY_SIZE(m_wszMsg)
		),
	m_eil(eli)
{}


//---------------------------------------------------------------------------
//	@function:
//		CLogger::~CLogger
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogger::~CLogger()
{}


//---------------------------------------------------------------------------
//	@function:
//		CLogger::Log
//
//	@doc:
//		Log message
//
//---------------------------------------------------------------------------
void
CLogger::Log
	(
	const WCHAR *wszMsg,
	ULONG ulSeverity,
	const CHAR *szFilename,
	ULONG ulLine
	)
{
	// get exclusive access
	CAutoMutex am(m_mutex);
	am.Lock();

	// format log message
	Format(wszMsg, ulSeverity, szFilename, ulLine);

	for (ULONG i = 0; i < GPOS_LOG_WRITE_RETRIES; i++)
	{
		GPOS_CHECK_ABORT;

		BOOL fExc = ITask::PtskSelf()->FPendingExc();

		// logging is exercised in catch blocks so it cannot throw;
		// the only propagated exception is Abort;
		GPOS_TRY
		{
			// write message to log
			Write(m_wstrEntry.Wsz(), ulSeverity);

			return;
		}
		GPOS_CATCH_EX(ex)
		{
			// propagate assert failures
			if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiAssert))
			{
				GPOS_RETHROW(ex);
			}

			// ignore anything else but aborts
			if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiAbort))
			{
				// reset any currently handled exception
				GPOS_RESET_EX;

				GPOS_ABORT;
			}

			if (!fExc)
			{
				GPOS_RESET_EX;
			}
		}
		GPOS_CATCH_END;
	}

	// alert admin for logging failure
	ReportFailure();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogger::Format
//
//	@doc:
//		Format log message
//
//---------------------------------------------------------------------------
void
CLogger::Format
	(
	const WCHAR *wszMsg,
	ULONG ulSeverity,
	const CHAR *, // szFilename
	ULONG // ulLine
	)
{
	m_wstrEntry.Reset();
	m_wstrMsg.Reset();

	CWStringConst strc(wszMsg);

	if (ILogger::EeilMsgHeader <= Eil())
	{
		// LOG ENTRY FORMAT: [date],[thread id],[severity],[message],

		ULONG ulThreadId = IWorker::PwrkrSelf()->UlThreadId();
		const CHAR *szSev = CException::m_rgszSeverity[ulSeverity];
		m_wstrMsg.Append(&strc);

		AppendDate();

		// append thread id and severity
		m_wstrEntry.AppendFormat
			(
			GPOS_WSZ_LIT(",THD%03d,%s,\"%ls\",\n"),
			ulThreadId,
			szSev,
			m_wstrMsg.Wsz()
			);
	}
	else
	{
		m_wstrEntry.Append(&strc);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CLogger::AppendDate
//
//	@doc:
//		Add date to message
//
//---------------------------------------------------------------------------
void
CLogger::AppendDate()
{
	TIMEVAL tv;
	TIME tm;

	// get local time
	syslib::GetTimeOfDay(&tv, NULL/*timezone*/);
#ifdef GPOS_DEBUG
	TIME *ptm =
#endif // GPOS_DEBUG
	clib::PtmLocalTimeR(&tv.tv_sec, &tm);

	GPOS_ASSERT(NULL != ptm && "Failed to get local time");

	// format: YYYY-MM-DD HH-MM-SS-UUUUUU TZ
	m_wstrEntry.AppendFormat
		(
		GPOS_WSZ_LIT("%04d-%02d-%02d %02d:%02d:%02d:%06d %s"),
		tm.tm_year + 1900,
		tm.tm_mon + 1,
		tm.tm_mday,
		tm.tm_hour,
		tm.tm_min,
		tm.tm_sec,
		tv.tv_usec,
#ifdef GPOS_SunOS
		clib::SzGetEnv("TZ")
#else
		tm.tm_zone
#endif // GPOS_SunOS
		)
		;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogger::AppendDate
//
//	@doc:
//		Report logging failure
//
//---------------------------------------------------------------------------
void
CLogger::ReportFailure()
{
	// check if errno was set
	if (0 < errno)
	{
		// get error description
		clib::StrErrorR(errno, m_szMsg, GPOS_ARRAY_SIZE(m_szMsg));
		m_szMsg[GPOS_ARRAY_SIZE(m_szMsg) - 1] = '\0';

		m_wstrEntry.Reset();
		m_wstrEntry.AppendFormat(GPOS_WSZ_LIT("%s\n"), m_szMsg);
		CLoggerSyslog::Alert(m_wstrEntry.Wsz());
		return;
	}

	// send generic failure message
	CLoggerSyslog::Alert
		(
		GPOS_WSZ_LIT("Log write failure, check disc space and filesystem integrity")
		)
		;
}

// EOF

