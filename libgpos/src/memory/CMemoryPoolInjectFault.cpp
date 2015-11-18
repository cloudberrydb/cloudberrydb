//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolInjectFault.cpp
//
//	@doc:
//		Implementation for memory pool that allocates from an underlying pool
//		and injects memory allocation failures using stack enumeration.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/memory/CMemoryPoolInjectFault.h"
#include "gpos/memory/CMemoryPoolTracker.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolInjectFault::CMemoryPoolInjectFault
//
//	@doc:
//	  Ctor
//
//---------------------------------------------------------------------------
CMemoryPoolInjectFault::CMemoryPoolInjectFault
	(
	IMemoryPool *pmp,
	BOOL fOwnsUnderlying
	)
	:
	CMemoryPool(pmp, fOwnsUnderlying, true /*fThreadSafe*/)
{
	GPOS_ASSERT(pmp != NULL);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolInjectFault::PvAllocate
//
//	@doc:
//	  Allocate memory; it will either simulate an allocation failure
//	  or call the underlying pool to reallocate the memory.
//
//---------------------------------------------------------------------------
void *
CMemoryPoolInjectFault::PvAllocate
	(
	const ULONG ulNumBytes,
	const CHAR *szFilename,
	const ULONG ulLine
	)
{
#ifdef GPOS_FPSIMULATOR
	if (FSimulateAllocFailure())
	{
		GPOS_TRACE_FORMAT_ERR("Simulating OOM at %s:%d", szFilename, ulLine);

		return NULL;
	}
#endif

	return PmpUnderlying()->PvAllocate(ulNumBytes, szFilename, ulLine);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolInjectFault::Free
//
//	@doc:
//		Free memory - delegates to the underlying pool;
//
//		note that this is only called through delegation and NOT by operator
//		delete, because the pointer in the header of the allocated memory
//		points to the underlying pool;
//
//---------------------------------------------------------------------------
void
CMemoryPoolInjectFault::Free
	(
	void *pvMemory
	)
{
	PmpUnderlying()->Free(pvMemory);
}


#ifdef GPOS_FPSIMULATOR

//---------------------------------------------------------------------------
//	@function:
//		IMemoryPool::FSimulateAllocFailure
//
//	@doc:
//		Check whether to simulate an OOM
//
//---------------------------------------------------------------------------
BOOL
CMemoryPoolInjectFault::FSimulateAllocFailure()
{
	ITask *ptsk = ITask::PtskSelf();
	if (NULL != ptsk)
	{
		return
			ptsk->FTrace(EtraceSimulateOOM) &&
			CFSimulator::Pfsim()->FNewStack(CException::ExmaSystem, CException::ExmiOOM);
	}

	return false;
}

#endif // GPOS_FPSIMULATOR


// EOF

