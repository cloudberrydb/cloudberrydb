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
	CMemoryPool *mp,
	BOOL owns_underlying_memory_pool
	)
	:
	CMemoryPool(mp, owns_underlying_memory_pool, true /*fThreadSafe*/)
{
	GPOS_ASSERT(mp != NULL);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolInjectFault::Allocate
//
//	@doc:
//	  Allocate memory; it will either simulate an allocation failure
//	  or call the underlying pool to reallocate the memory.
//
//---------------------------------------------------------------------------
void *
CMemoryPoolInjectFault::Allocate
	(
	const ULONG num_bytes,
	const CHAR *filename,
	const ULONG line
	)
{
#ifdef GPOS_FPSIMULATOR
	if (SimulateAllocFailure())
	{
		GPOS_TRACE_FORMAT_ERR("Simulating OOM at %s:%d", filename, line);

		return NULL;
	}
#endif

	return GetUnderlyingMemoryPool()->Allocate(num_bytes, filename, line);
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
	void *memory
	)
{
	GetUnderlyingMemoryPool()->Free(memory);
}


#ifdef GPOS_FPSIMULATOR

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::FSimulateAllocFailure
//
//	@doc:
//		Check whether to simulate an OOM
//
//---------------------------------------------------------------------------
BOOL
CMemoryPoolInjectFault::SimulateAllocFailure()
{
	ITask *task = ITask::Self();
	if (NULL != task)
	{
		return
			task->IsTraceSet(EtraceSimulateOOM) &&
			CFSimulator::FSim()->NewStack(CException::ExmaSystem, CException::ExmiOOM);
	}

	return false;
}

#endif // GPOS_FPSIMULATOR


// EOF

