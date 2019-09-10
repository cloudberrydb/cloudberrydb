//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolTracker.h
//
//	@doc:
//		Memory pool that allocates from malloc() and adds on
//		statistics and debugging
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolTracker_H
#define GPOS_CMemoryPoolTracker_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/common/CList.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/memory/CMemoryPool.h"

namespace gpos
{
	// memory pool with statistics and debugging support
	class CMemoryPoolTracker : public CMemoryPool
	{
		private:

			//---------------------------------------------------------------------------
			//	@struct:
			//		AllocHeader
			//
			//	@doc:
			//		Defines memory block header layout for all allocations;
			//	 	does not include the pointer to the pool;
			//
			//---------------------------------------------------------------------------
			struct SAllocHeader
			{
				// sequence number
				ULLONG m_serial;

				// user-visible size
				ULONG m_size;

				// file name
				const CHAR *m_filename;

				// line in file
				ULONG m_line;

#ifdef GPOS_DEBUG
				// allocation stack
				CStackDescriptor m_stack_desc;
#endif // GPOS_DEBUG

				// link for allocation list
				SLink m_link;
			};

			// statistics
			CMemoryPoolStatistics m_memory_pool_statistics;

			// allocation sequence number
			ULONG m_alloc_sequence;

			// list of allocated (live) objects
			CList<SAllocHeader> m_allocations_list;

			// private copy ctor
			CMemoryPoolTracker(CMemoryPoolTracker &);

		protected:

			// dtor
			virtual
			~CMemoryPoolTracker();

		public:

			// ctor
			CMemoryPoolTracker();

			// allocate memory
			virtual
			void *Allocate
				(
				const ULONG bytes,
				const CHAR *file,
				const ULONG line
				);

			// free memory
			virtual
			void Free(void *ptr);

			// prepare the memory pool to be deleted
			virtual
			void TearDown();

			// return total allocated size
			virtual
			ULLONG TotalAllocatedSize() const
			{
				return m_memory_pool_statistics.TotalAllocatedSize();
			}

#ifdef GPOS_DEBUG

			// check if the memory pool keeps track of live objects
			virtual
			BOOL SupportsLiveObjectWalk() const
			{
				return true;
			}

			// walk the live objects
			virtual
			void WalkLiveObjects(gpos::IMemoryVisitor *visitor);

#endif // GPOS_DEBUG

	};
}

#endif // !GPOS_CMemoryPoolTracker_H

// EOF

