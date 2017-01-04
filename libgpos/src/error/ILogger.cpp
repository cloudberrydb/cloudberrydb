//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ILogger.cpp
//
//	@doc:
//		Worker abstraction, e.g. thread
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"
#include "gpos/common/syslibwrapper.h"
#include "gpos/error/ILogger.h"
#include "gpos/error/CLoggerStream.h"
#include "gpos/error/CLoggerSyslog.h"
#include "gpos/error/CMessageRepository.h"
#include "gpos/string/CWStringConst.h"
#include "gpos/sync/CAutoMutex.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		ILogger::ILogger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
ILogger::ILogger()
{}


//---------------------------------------------------------------------------
//	@function:
//		ILogger::~ILogger
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
ILogger::~ILogger()
{}


//---------------------------------------------------------------------------
//	@function:
//		ILogger::Warning
//
//	@doc:
//		Retrieve warning message from repository and log it to error log
//
//---------------------------------------------------------------------------
void
ILogger::Warning
	(
	const CHAR *szFilename,
	ULONG ulLine,
	ULONG ulMajor,
	ULONG ulMinor
	...
	)
{
	GPOS_CHECK_ABORT;

	// get warning
	CException exc(ulMajor, ulMinor, szFilename, ulLine);

	ITask *ptsk = ITask::PtskSelf();

	// get current task's locale
	ELocale eloc = ElocEnUS_Utf8;
	if (NULL != ptsk)
	{
		eloc = ptsk->Eloc();
	}

	// retrieve warning message from repository
	CMessage *pmsg = CMessageRepository::Pmr()->PmsgLookup(exc, eloc);

	GPOS_ASSERT(CException::ExsevWarning == pmsg->UlSev());

	WCHAR wszBuffer[GPOS_LOG_MESSAGE_BUFFER_SIZE];
	CWStringStatic str(wszBuffer, GPOS_ARRAY_SIZE(wszBuffer));

	// format warning message
	{
		VA_LIST vaArgs;

		VA_START(vaArgs, ulMinor);

		pmsg->Format(&str, vaArgs);

		VA_END(vaArgs);
	}

	LogTask(str.Wsz(), CException::ExsevWarning, true /*fErr*/, szFilename, ulLine);
}


//---------------------------------------------------------------------------
//	@function:
//		ILogger::Trace
//
//	@doc:
//		Format and log debugging message to current task's output or error log
//
//---------------------------------------------------------------------------
void
ILogger::Trace
	(
	const CHAR *szFilename,
	ULONG ulLine,
	BOOL fErr,
	const WCHAR *wszMsg
	)
{
	GPOS_CHECK_ABORT;

	LogTask(wszMsg, CException::ExsevTrace, fErr, szFilename, ulLine);
}


//---------------------------------------------------------------------------
//	@function:
//		ILogger::TraceFormat
//
//	@doc:
//		Format and log debugging message to current task's output or error log
//
//---------------------------------------------------------------------------
void
ILogger::TraceFormat
	(
	const CHAR *szFilename,
	ULONG ulLine,
	BOOL fErr,
	const WCHAR *wszFormat,
	...
	)
{
	GPOS_CHECK_ABORT;

	WCHAR wszBuffer[GPOS_LOG_TRACE_BUFFER_SIZE];
	CWStringStatic str(wszBuffer, GPOS_ARRAY_SIZE(wszBuffer));

	VA_LIST vaArgs;

	// get arguments
	VA_START(vaArgs, wszFormat);

	str.AppendFormatVA(wszFormat, vaArgs);

	// reset arguments
	VA_END(vaArgs);

	LogTask(str.Wsz(), CException::ExsevTrace, fErr, szFilename, ulLine);
}


//---------------------------------------------------------------------------
//	@function:
//		ILogger::LogTask
//
//	@doc:
//		Log message to current task's logger;
// 		Use stdout/stderr-wrapping loggers outside worker framework;
//
//---------------------------------------------------------------------------
void
ILogger::LogTask
	(
	const WCHAR *wszMsg,
	ULONG ulSeverity,
	BOOL fErr,
	const CHAR *szFilename,
	ULONG ulLine
	)
{
	CLogger *plog = NULL;

	if (fErr)
	{
		plog = &CLoggerStream::m_plogStdErr;
	}
	else
	{
		plog = &CLoggerStream::m_plogStdOut;
	}

	ITask *ptsk = ITask::PtskSelf();
	if (NULL != ptsk)
	{
		if (fErr)
		{
			plog = dynamic_cast<CLogger*>(ptsk->PlogErr());
		}
		else
		{
			plog = dynamic_cast<CLogger*>(ptsk->PlogOut());
		}
	}

	GPOS_ASSERT(NULL != plog);

	plog->Log(wszMsg, ulSeverity, szFilename, ulLine);
}

// EOF

