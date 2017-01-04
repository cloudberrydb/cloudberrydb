//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		syslibwrapper.cpp
//
//	@doc:
//		Wrapper for functions in system library
//
//---------------------------------------------------------------------------

#include <syslog.h>

#include <sys/time.h>
#include <sys/stat.h>

#include "gpos/assert.h"
#include "gpos/utils.h"

#include "gpos/common/syslibwrapper.h"
#include "gpos/error/CException.h"


using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		syslib::GetTimeOfDay
//
//	@doc:
//		Get the date and time
//
//---------------------------------------------------------------------------
void
gpos::syslib::GetTimeOfDay
	(
	TIMEVAL *ptv,
	TIMEZONE *ptz
	)
{
	GPOS_ASSERT(NULL != ptv);

#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	gettimeofday(ptv, ptz);

	GPOS_ASSERT(0 == iRes);
}


//---------------------------------------------------------------------------
//	@function:
//		syslib::GetRusage
//
//	@doc:
//		Get system and user time
//
//---------------------------------------------------------------------------
void
gpos::syslib::GetRusage
	(
	RUSAGE *prusage
	)
{
	GPOS_ASSERT(NULL != prusage);

#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	getrusage(RUSAGE_SELF, prusage);

	GPOS_ASSERT(0 == iRes);
}


//---------------------------------------------------------------------------
//	@function:
//		syslib::SchedYield
//
//	@doc:
//		Yield the processor
//
//---------------------------------------------------------------------------
void
gpos::syslib::SchedYield
	(
	)
{
#ifdef GPOS_DEBUG
	INT iRes =
#endif // GPOS_DEBUG
	sched_yield();

	GPOS_ASSERT(0 == iRes && "Failed to yield");
}


//---------------------------------------------------------------------------
//	@function:
//		syslib::OpenLog
//
//	@doc:
//		Open a connection to the system logger for a program
//
//---------------------------------------------------------------------------
void
gpos::syslib::OpenLog
	(
	const CHAR *szIdent,
	INT iOption,
	INT iFacility
	)
{
	openlog(szIdent, iOption, iFacility);
}


//---------------------------------------------------------------------------
//	@function:
//		syslib::SysLog
//
//	@doc:
//		Generate a log message
//
//---------------------------------------------------------------------------
void
gpos::syslib::SysLog
	(
	INT iPriority,
	const CHAR *szMessage
	)
{
	syslog(iPriority, "%s", szMessage);
}


//---------------------------------------------------------------------------
//	@function:
//		syslib::CloseLog
//
//	@doc:
//		Close the descriptor being used to write to the system logger
//
//---------------------------------------------------------------------------
void
gpos::syslib::CloseLog
	(
	)
{
	closelog();
}

// EOF

