//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerTest.h
//
//	@doc:
//      Unit test for logger classes.
//---------------------------------------------------------------------------
#ifndef GPOS_CLoggerTest_H
#define GPOS_CLoggerTest_H

#include "gpos/base.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLoggerTest
	//
	//	@doc:
	//		Unittests for log functionality
	//
	//---------------------------------------------------------------------------
	class CLoggerTest
	{
		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basic();
			static GPOS_RESULT EresUnittest_LoggerFile();
			static GPOS_RESULT EresUnittest_LoggerStress();
			static GPOS_RESULT EresUnittest_LoggerSyslog();

			static void Unittest_TestLoggerFile
				(
				IMemoryPool *pmp,
				ULONG ulFiles,
				ULONG ulMaxFileSize,
				ULONG ulWorkers,
				ULONG ulIterations
				)
				;
			static void PvUnittest_DeleteFiles(const CHAR *szDir, ULONG ulFiles);
			static void PvUnittest_CheckFiles(const CHAR *szDir, ULONG ulFiles);
			static void *PvUnittest_LogWrite(void *);

	}; // CLoggerTest
}

#endif // !GPOS_CLoggerTest_H

// EOF

