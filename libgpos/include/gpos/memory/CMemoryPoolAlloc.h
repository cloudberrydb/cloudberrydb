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
			void* (*m_alloc) (SIZE_T);
			void (*m_free) (void*);

		public:

			// ctor
			CMemoryPoolAlloc
				(
					void* (*alloc)(SIZE_T),
					void (*free_func)(void*)
				)
				:
				CMemoryPool
					(
					NULL /*pmpUnderlying*/,
					false /*fOwnsUnderlying*/,
					true /*fThreadSafe*/
					),
				m_alloc(alloc),
				m_free(free_func)
			{}

			// dtor
			virtual
			~CMemoryPoolAlloc()
			{}

			// allocate memory
			virtual
			void *Allocate
				(
				ULONG num_bytes,
				const CHAR *, // filename
				const ULONG   // line
				)
			{

				void* ptr = m_alloc(num_bytes);

#ifdef GPOS_DEBUG
				if (NULL != ITask::Self() && GPOS_FTRACE(EtraceSimulateOOM) && NULL == ptr)
				{
					GPOS_RAISE(CException::ExmaSystem, CException::ExmiUnexpectedOOMDuringFaultSimulation);
				}
#endif // GPOS_DEBUG
				return ptr;
			}

			// free memory
			virtual
			void Free
				(
				void *ptr
				)
			{
				m_free(ptr);
			}

	};
}

#endif // !GPOS_CMemoryPoolAlloc_H

// EOF

