//---------------------------------------------------------------------------
//	Greenplum Database 
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPool.h
//
//	@doc:
//		Abstraction of memory pool management. Memory pool types are derived
//		from this interface class as drop-in replacements. The interface
//		defines New() operator that is used for dynamic memory allocation.
//
//	@owner: 
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPool_H
#define GPOS_CMemoryPool_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/common/CList.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/memory/IMemoryPool.h"


// 8-byte alignment
#define GPOS_MEM_ARCH				(8)

#define GPOS_MEM_ALIGNED_SIZE(x)	(GPOS_MEM_ARCH * (((x)/GPOS_MEM_ARCH) + \
									 ((x) % GPOS_MEM_ARCH ? 1 : 0)))

// padded size for a given data structure
#define GPOS_MEM_ALIGNED_STRUCT_SIZE(x)	GPOS_MEM_ALIGNED_SIZE(GPOS_SIZEOF(x))

// sanity check: char and ulong always fits into the basic unit of alignment
GPOS_CPL_ASSERT(GPOS_MEM_ALIGNED_STRUCT_SIZE(gpos::CHAR) == GPOS_MEM_ARCH);
GPOS_CPL_ASSERT(GPOS_MEM_ALIGNED_STRUCT_SIZE(gpos::ULONG) == GPOS_MEM_ARCH);

// static pattern to init memory
#define GPOS_MEM_INIT_PATTERN_CHAR	(0xCC)

// max allocation per request: 1GB
#define GPOS_MEM_ALLOC_MAX			(0x40000000)
#define GPOS_MEM_GUARD_SIZE			(GPOS_SIZEOF(BYTE))

#define GPOS_MEM_OFFSET_POS(p,ullOffset) ((void *)((BYTE*)(p) + ullOffset))


namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPool
	//
	//	@doc:
	//		Interface class for memory pool
	//
	//---------------------------------------------------------------------------
	class CMemoryPool : public IMemoryPool
	{

		// manager accesses internal functionality
		friend class CMemoryPoolManager;

		private:

			// common header for each allocation
			struct SAllocHeader
			{
				// pointer to pool
				IMemoryPool *m_pmp;

				// allocation request size
				ULONG m_ulAlloc;
			};

			// reference counter
			ULONG m_ulRef;

			// hash key is only set by pool manager
			ULONG_PTR m_ulpKey;

			// underlying memory pool - optional
			IMemoryPool *m_pmpUnderlying;

			// flag indicating if this pool owns the underlying pool
			const BOOL m_fOwnsUnderlying;

			// flag indicating if memory pool is thread-safe
			const BOOL m_fThreadSafe;

#ifdef GPOS_DEBUG
			// stack where pool is created
			CStackDescriptor m_sd;
#endif // GPOS_DEBUG

			// link structure to manage pools
			SLink m_link;

		protected:

			// ctor
			CMemoryPool
				(
				IMemoryPool *pmpUnderlying,
				BOOL fOwnsUnderlying,
				BOOL fThreadSafe
				);

			// underlying pool accessor
			IMemoryPool *PmpUnderlying() const
			{
				return m_pmpUnderlying;
			}

			// invalid memory pool key
			static
			const ULONG_PTR m_ulpInvalid;

		public:

			// dtor
			virtual
			~CMemoryPool() = 0;

			// prepare the memory pool to be deleted
			virtual
			void TearDown()
			{
				if (m_fOwnsUnderlying)
				{
					m_pmpUnderlying->TearDown();
				}
			}

			// check if memory pool is thread-safe
			virtual
			BOOL FThreadSafe() const
			{
				return m_fThreadSafe;
			}

			// hash key accessor
			virtual
			ULONG_PTR UlpKey() const
			{
				return m_ulpKey;
			}

			// get allocation size
			static
			ULONG UlAllocSize
				(
				ULONG ulRequested
				)
			{
				return GPOS_SIZEOF(SAllocHeader) +
				       GPOS_MEM_ALIGNED_SIZE(ulRequested + GPOS_MEM_GUARD_SIZE);
			}

			// set allocation header and footer, return pointer to user data
			void *PvFinalizeAlloc(void *pv, ULONG ulAlloc, EAllocationType eat);

			// return allocation to owning memory pool
			static
			void FreeAlloc(void *pv, EAllocationType eat);

			// check if the pool stores a pointer to itself at the end of
			// the header of each allocated object;
			virtual
			BOOL FStoresPoolPointer() const
			{
				return false;
			}

			// return total allocated size
			virtual
			ULLONG UllTotalAllocatedSize() const
			{
				GPOS_ASSERT(!"not supported");
				return 0;
			}

			// determine the size (in bytes) of an allocation that
			// was made from a CMemoryPool
			static
			ULONG UlSizeOfAlloc(const void *pv);

#ifdef GPOS_DEBUG

			// check if the memory pool keeps track of live objects
			virtual
			BOOL FSupportsLiveObjectWalk() const
			{
				return false;
			}

			// walk the live objects, calling pVisitor.visit() for each one
			virtual
			void WalkLiveObjects
				(
				IMemoryVisitor *
				)
			{
				GPOS_ASSERT(!"not supported");
			}

			// check if statistics tracking is supported
			virtual
			BOOL FSupportsStatistics() const
			{
				return false;
			}

			// return the current statistics
			virtual
			void UpdateStatistics
				(
				CMemoryPoolStatistics &
				)
			{
				GPOS_ASSERT(!"not supported");
			}

			// dump memory pool to given stream
			virtual
			IOstream &OsPrint(IOstream &os);

			// check if a memory pool is empty
			virtual
			void AssertEmpty(IOstream &os);

#endif // GPOS_DEBUG

	};	// class CMemoryPool

} // gpos


#endif // !GPOS_CMemoryPool_H

// EOF

