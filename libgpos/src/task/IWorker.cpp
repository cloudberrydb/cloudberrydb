//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWorker.cpp
//
//	@doc:
//		Worker abstraction, e.g. thread
//---------------------------------------------------------------------------


#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/task/CWorkerPoolManager.h"

#include "gpos/task/IWorker.h"

using namespace gpos;

#ifdef GPOS_DEBUG
BOOL IWorker::m_fEnforceTimeSlices(false);
#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		IWorker::PwrkrSelf
//
//	@doc:
//		static function to lookup ones own worker in the pool manager
//
//---------------------------------------------------------------------------
IWorker *
IWorker::PwrkrSelf()
{
	IWorker *pwrkr = NULL;
	
	if (NULL != CWorkerPoolManager::Pwpm())
	{
		pwrkr = CWorkerPoolManager::Pwpm()->PwrkrSelf();
	}
	
	return pwrkr;
}


//---------------------------------------------------------------------------
//	@function:
//		IWorker::CheckForAbort
//
//	@doc:
//		Check for aborts
//
//---------------------------------------------------------------------------
void
IWorker::CheckAbort
	(
	const CHAR *szFile,
	ULONG cLine
	)
{
	IWorker *pwrkr = PwrkrSelf();
	if (NULL != pwrkr)
	{
		pwrkr->CheckForAbort(szFile, cLine);
	}
}

// EOF

