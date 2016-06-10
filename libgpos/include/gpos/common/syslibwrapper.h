//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//	       	syslibwrapper.h
//
//	@doc:
//	       	Wrapper for functions in system library
//
//---------------------------------------------------------------------------

#ifndef GPOS_syslibwrapper_H
#define GPOS_syslibwrapper_H

#include "gpos/types.h"
#include "gpos/common/clibtypes.h"

namespace gpos {

	namespace syslib {

		// get the date and time
		void GetTimeOfDay(TIMEVAL *ptv, TIMEZONE *ptz);

		// get system and user time
		void GetRusage(RUSAGE *prusage);

		// yield the processor
		void SchedYield();

		// open a connection to the system logger for a program
		void OpenLog(const CHAR *szIdent, INT iOption, INT iFacility);

		// generate a log message
		void SysLog(INT iPriority, const CHAR *szMessage);

		// close the descriptor being used to write to the system logger
		void CloseLog();


	} //namespace syslib
}

#endif
// EOF

