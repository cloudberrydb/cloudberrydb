//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTimerUser.cpp
//
//	@doc:
//		Implementation of wall clock timer
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/syslibwrapper.h"
#include "gpos/common/CTimerUser.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CTimerUser::UlElapsedUS
//
//	@doc:
//		User time in micro-seconds since object construction
//
//---------------------------------------------------------------------------
ULONG
CTimerUser::UlElapsedUS() const
{
	RUSAGE rusage;
	syslib::GetRusage(&rusage);

	ULONG ulDiff = (ULONG)
		(((rusage.ru_utime.tv_sec - m_rusage.ru_utime.tv_sec) * GPOS_USEC_IN_SEC) +
		 (rusage.ru_utime.tv_usec - m_rusage.ru_utime.tv_usec));

	return ulDiff;
}


//---------------------------------------------------------------------------
//	@function:
//		CTimerUser::Restart
//
//	@doc:
//		Restart timer
//
//---------------------------------------------------------------------------
void
CTimerUser::Restart()
{
	syslib::GetRusage(&m_rusage);
}


// EOF

