//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//	@filename:
//		CMemoryPoolAlloc.h
//
//	@doc:
//		Implementation of memory pool using a configurable allocator
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolAlloc_H
#define GPOS_CMemoryPoolAlloc_H

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

namespace gpos
{
	// memory pool wrapping clib allocation API
	class CMemoryPoolAlloc : public CMemoryPool
	{
		private:

			// private copy ctor
			CMemoryPoolAlloc(CMemoryPoolAlloc &);
			void* (*m_pfnAlloc) (SIZE_T);
			void (*m_pfnFree) (void*);

		public:

			// ctor
			CMemoryPoolAlloc
				(
					void* (*pfnAlloc)(SIZE_T),
					void (*pfnFree)(void*)
				)
				:
				CMemoryPool
					(
					NULL /*pmpUnderlying*/,
					false /*fOwnsUnderlying*/,
					true /*fThreadSafe*/
					),
				m_pfnAlloc(pfnAlloc),
				m_pfnFree(pfnFree)
			{}

			// dtor
			virtual
			~CMemoryPoolAlloc()
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

				void* pv = m_pfnAlloc(ulNumBytes);

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
				m_pfnFree(pv);
			}

	};
}

#endif // !GPOS_CMemoryPoolAlloc_H

// EOF

