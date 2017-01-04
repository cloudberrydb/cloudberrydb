//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerSyslog.h
//
//	@doc:
//		Implementation of logging interface over syslog
//---------------------------------------------------------------------------
#ifndef GPOS_CLoggerSyslog_H
#define GPOS_CLoggerSyslog_H

#include "gpos/error/CLogger.h"

#define GPOS_SYSLOG_ALERT(szMsg)   CLoggerSyslog::Alert(GPOS_WSZ_LIT(szMsg))

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLoggerSyslog
	//
	//	@doc:
	//		Syslog logging.
	//
	//---------------------------------------------------------------------------

	class CLoggerSyslog : public CLogger
	{
		private:

			// executable name
			const CHAR *m_szProcName;

			// initialization flags
			ULONG m_ulInitMask;

			// message priotity
			ULONG m_ulMessagePriority;

			// no copy ctor
			CLoggerSyslog(const CLoggerSyslog&);

			// write string to syslog
			void Write(const WCHAR *wszLogEntry, ULONG ulSev);

			static CLoggerSyslog m_loggerAlert;

		public:
			// ctor
			CLoggerSyslog
				(
				const CHAR *szProcName,
				ULONG ulInitMask,
				ULONG ulMessagePriority
				);

			// dtor
			virtual	~CLoggerSyslog();

			// write alert message to syslog - use ASCII characters only
			static void Alert(const WCHAR *wszMsg);

	};	// class CLoggerSyslog
}

#endif // !GPOS_CLoggerSyslog_H

// EOF

