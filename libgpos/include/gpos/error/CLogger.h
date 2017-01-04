//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLogger.h
//
//	@doc:
//		Partial implementation of interface class for logging
//---------------------------------------------------------------------------
#ifndef GPOS_CLogger_H
#define GPOS_CLogger_H

#include "gpos/error/ILogger.h"
#include "gpos/sync/CMutex.h"
#include "gpos/string/CWStringStatic.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogger
	//
	//	@doc:
	//		Partial implementation of interface for abstracting logging primitives.
	//
	//---------------------------------------------------------------------------
	class CLogger : public ILogger
	{
		friend class ILogger;
		friend class CErrorHandlerStandard;

		private:

			// buffer used to construct the log entry
			WCHAR m_wszEntry[GPOS_LOG_ENTRY_BUFFER_SIZE];

			// buffer used to construct the log entry
			WCHAR m_wszMsg[GPOS_LOG_MESSAGE_BUFFER_SIZE];

			// buffer used to retrieve system error messages
			CHAR m_szMsg[GPOS_LOG_MESSAGE_BUFFER_SIZE];

			// entry buffer wrapper
			CWStringStatic m_wstrEntry;

			// message buffer wrapper
			CWStringStatic m_wstrMsg;

			// mutex for atomic log writes
			CMutex m_mutex;

			// error logging information level
			EErrorInfoLevel m_eil;

			// log message
			void Log
				(
				const WCHAR *wszMsg,
				ULONG ulSeverity,
				const CHAR *szFilename,
				ULONG ulLine
				);

			// format log message
			void Format
				(
				const WCHAR *wszMsg,
				ULONG ulSeverity,
				const CHAR *szFilename,
				ULONG ulLine
				);

			// add date to message
			void AppendDate();

			// report logging failure
			void ReportFailure();

			// no copy ctor
			CLogger(const CLogger&);

		protected:

			// accessor for system error buffer
			CHAR *SzMsg()
			{
				return m_szMsg;
			}

		public:

			// ctor
			explicit
			CLogger(EErrorInfoLevel eil = ILogger::EeilMsgHeaderStack);

			// dtor
			virtual
			~CLogger();

			// error level accessor
			virtual
			EErrorInfoLevel Eil() const
			{
				return m_eil;
			}

			// set error info level
			virtual
			void SetErrorInfoLevel
				(
				EErrorInfoLevel eil
				)
			{
				m_eil = eil;
			}

#ifdef GPOS_FPSIMULATOR
			// check if a message is logged
			BOOL FLogging() const
			{
				return m_mutex.FOwned();
			}
#endif // GPOS_FPSIMULATOR

	};	// class CLogger
}

#endif // !GPOS_CLogger_H

// EOF

