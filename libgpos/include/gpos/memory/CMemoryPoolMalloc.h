//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolMalloc.h
//
//	@doc:
//		Implementation of memory pool that allocates memory by calling malloc.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolMalloc_H
#define GPOS_CMemoryPoolMalloc_H

#include "gpos/base.h"
#include "gpos/memory/CMemoryPool.h"

namespace gpos
{
	// memory pool wrapping clib allocation API
	class CMemoryPoolMalloc : public CMemoryPool
	{
		private:

			// private copy ctor
			CMemoryPoolMalloc(CMemoryPoolMalloc &);

		public:

			// ctor
			CMemoryPoolMalloc()
				:
				CMemoryPool
					(
					NULL /*pmpUnderlying*/,
					false /*fOwnsUnderlying*/,
					true /*fThreadSafe*/
					)
			{}

			// dtor
			virtual
			~CMemoryPoolMalloc()
			{}

			// allocate memory
			virtual
			void *PvAllocate
				(
				ULONG ulNumBytes,
				const CHAR *, // szFilename
				const ULONG   // ulLine
				)
			{
				void *pv = gpos::clib::PvMalloc(ulNumBytes);

#ifdef GPOS_DEBUG
				if (NULL != ITask::PtskSelf() && GPOS_FTRACE(EtraceSimulateOOM) && NULL == pv)
				{
					GPOS_RAISE(CException::ExmaSystem, CException::ExmiUnexpectedOOMDuringFaultSimulation);
				}
#endif // GPOS_DEBUG
				return pv; 
			}

			// free memory
			virtual
			void Free
				(
				void *pv
				)
			{
				gpos::clib::Free(pv);
			}

	};
}

#endif // !GPOS_CMemoryPoolMalloc_H

// EOF

