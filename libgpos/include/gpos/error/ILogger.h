//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		ILogger.h
//
//	@doc:
//		Interface class for logging
//---------------------------------------------------------------------------
#ifndef GPOS_ILogger_H
#define GPOS_ILogger_H

#include "gpos/types.h"

#define GPOS_LOG_MESSAGE_BUFFER_SIZE    (1024 * 128)
#define GPOS_LOG_TRACE_BUFFER_SIZE      (1024 * 8)
#define GPOS_LOG_ENTRY_BUFFER_SIZE      (GPOS_LOG_MESSAGE_BUFFER_SIZE + 256)
#define GPOS_LOG_WRITE_RETRIES          (10)

#define GPOS_WARNING(...)   \
	ILogger::Warning(__FILE__, __LINE__, __VA_ARGS__)

#define GPOS_TRACE(wszMsg)   \
	ILogger::Trace(__FILE__, __LINE__, false /*fErr*/, wszMsg)

#define GPOS_TRACE_ERR(wszMsg)   \
	ILogger::Trace(__FILE__, __LINE__, true /*fErr*/, wszMsg)

#define GPOS_TRACE_FORMAT(szFormat, ...)   \
	ILogger::TraceFormat(__FILE__, __LINE__, false /*fErr*/, GPOS_WSZ_LIT(szFormat), __VA_ARGS__)

#define GPOS_TRACE_FORMAT_ERR(szFormat, ...)   \
	ILogger::TraceFormat(__FILE__, __LINE__, true /*fErr*/, GPOS_WSZ_LIT(szFormat), __VA_ARGS__)

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		ILogger
	//
	//	@doc:
	//		Interface for abstracting logging primitives.
	//
	//---------------------------------------------------------------------------

	class ILogger
	{
		friend class CErrorHandlerStandard;

		public:

			// enum indicating error logging information
			enum EErrorInfoLevel
			{
				EeilMsg,			// log error message only
				EeilMsgHeader,		// log error header and message
				EeilMsgHeaderStack	// log error header, message and stack trace
			};

		private:

			// log message to current task's logger;
			// use stdout/stderr wrapping loggers outside worker framework;
			static
			void LogTask
				(
				const WCHAR *wszMsg,
				ULONG ulSeverity,
				BOOL fErr,
				const CHAR *szFilename,
				ULONG ulLine
				);

			// no copy ctor
			ILogger(const ILogger&);

		protected:

			// write log message
			virtual
			void Write(const WCHAR *wszLogEntry, ULONG ulSev) = 0;

		public:

			// ctor
			ILogger();

			// dtor
			virtual
			~ILogger();

			// error info level accessor
			virtual
			EErrorInfoLevel Eil() const = 0;

			// set error info level
			virtual
			void SetErrorInfoLevel(EErrorInfoLevel eil) = 0;

			// retrieve warning message from repository and log it to error log
			static void Warning
				(
				const CHAR *szFilename,
				ULONG ulLine,
				ULONG ulMajor,
				ULONG ulMinor,
				...
				);

			// log trace message to current task's output or error log
			static void Trace
				(
				const CHAR *szFilename,
				ULONG ulLine,
				BOOL fErr,
				const WCHAR *wszMsg
				);

			// format and log trace message to current task's output or error log
			static void TraceFormat
				(
				const CHAR *szFilename,
				ULONG ulLine,
				BOOL fErr,
				const WCHAR *wszFormat,
				...
				);

	};	// class ILogger
}

#endif // !GPOS_ILogger_H

// EOF

