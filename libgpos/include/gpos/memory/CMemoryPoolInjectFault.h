//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolInjectFault.h
//
//	@doc:
//		Memory pool wrapper that uses stack enumeration to simulate
//		Out-Of-Memory exceptions for all executed allocations.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolInjectFault_H
#define GPOS_CMemoryPoolInjectFault_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/error/CFSimulator.h"

namespace gpos
{
	class CMemoryPoolInjectFault : public CMemoryPool
	{
		private:

#ifdef GPOS_FPSIMULATOR
			// check for OOM simulation
			BOOL SimulateAllocFailure();
#endif // GPOS_FPSIMULATOR

			// private copy ctor
			CMemoryPoolInjectFault(CMemoryPoolInjectFault &);

		public:

			// ctor
			CMemoryPoolInjectFault(IMemoryPool *mp, BOOL owns_underlying_memory_pool);

			//dtor
			virtual
			~CMemoryPoolInjectFault()
			{}

			// allocate memory
			virtual
			void *Allocate
				(
				const ULONG num_bytes,
				const CHAR *filename,
				const ULONG line
				);

			// free memory
			virtual
			void Free(void *ptr);

			// return total allocated size
			virtual
			ULLONG TotalAllocatedSize() const
			{
				return GetUnderlyingMemoryPool()->TotalAllocatedSize();
			}
	};
}

#endif // !GPOS_CMemoryPoolInjectFault_H

// EOF

