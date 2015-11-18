//---------------------------------------------------------------------------
//	Greenplum Database 
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMemoryPool.h
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
#ifndef GPOS_IMemoryPool_H
#define GPOS_IMemoryPool_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/io/IOstream.h"
#include "gpos/memory/CMemoryPoolStatistics.h"


namespace gpos
{
	// prototypes
	class IMemoryVisitor;

	//---------------------------------------------------------------------------
	//	@class:
	//		IMemoryPool
	//
	//	@doc:
	//		Interface class for memory pool
	//
	//---------------------------------------------------------------------------
	class IMemoryPool
	{
		public:

			// type of allocation, simple singleton or array
			enum EAllocationType
			{
				EatSingleton = 0x7f,
				EatArray = 0x7e
			};

			// ctor
			IMemoryPool()
			{}

			// dtor
			virtual
			~IMemoryPool()
			{}

			// allocate memory; return NULL if the memory could not be allocated
			virtual
			void *PvAllocate
				(
				const ULONG ulNumBytes,
				const CHAR *szFilename,
				const ULONG ulLine
				) = 0;

			// free memory previously allocated by a call to pvAllocate; NULL may be passed
			virtual
			void Free(void *pMemory) = 0;

			// prepare the memory pool to be deleted
			virtual
			void TearDown() = 0;

			// check if the pool stores a pointer to itself at the end of
			// the header of each allocated object;
			// this is used by the new/delete operators to determine the
			// memory pool that was used for allocation;
			virtual
			BOOL FStoresPoolPointer() const = 0;

			// check if memory pool is thread-safe
			virtual
			BOOL FThreadSafe() const = 0;

			// return the hash key of the memory pool
			virtual
			ULONG_PTR UlpKey() const = 0;

			// return total allocated size
			virtual
			ULLONG UllTotalAllocatedSize() const = 0;

#ifdef GPOS_DEBUG

			// check if the memory pool keeps track of live objects
			virtual
			BOOL FSupportsLiveObjectWalk() const = 0;

			// walk the live objects, calling pVisitor.visit() for each one
			virtual
			void WalkLiveObjects(IMemoryVisitor *pmov) = 0;

			// check if statistics tracking is supported
			virtual
			BOOL FSupportsStatistics() const = 0;

			// return the current statistics
			virtual
			void UpdateStatistics(CMemoryPoolStatistics &mps) = 0;

			// dump memory pool to given stream
			virtual
			IOstream &OsPrint(IOstream &os) = 0;

			// check if a memory pool is empty
			virtual
			void AssertEmpty(IOstream &os) = 0;

#endif // GPOS_DEBUG

	};	// class IMemoryPool

#ifdef GPOS_DEBUG

	//---------------------------------------------------------------------------
	//	@function:
	//		IMemoryPool::operator <<
	//
	//	@doc:
	//		Print function for memory pools
	//
	//---------------------------------------------------------------------------
	inline IOstream &
	operator <<
		(
		IOstream &os,
		IMemoryPool &mp
		)
	{
		return mp.OsPrint(os);
	}
#endif // GPOS_DEBUG

} // gpos

// placement new definition
#define New(pmp) new(pmp, __FILE__, __LINE__)

// new implementation
void* NewImpl
	(
	gpos::IMemoryPool *pmp,
	gpos::SIZE_T cSize,
	const gpos::CHAR *szFilename,
	gpos::ULONG ulLine,
	gpos::IMemoryPool::EAllocationType eat
	);

// new implementation, no exception throwing
void* NewImplNoThrow
	(
	gpos::IMemoryPool *pmp,
	gpos::SIZE_T cSize,
	const gpos::CHAR *szFilename,
	gpos::ULONG ulLine,
	gpos::IMemoryPool::EAllocationType eat
	);

// delte implementation
void DeleteImpl
	(
	void *pv,
	gpos::IMemoryPool::EAllocationType eat
	);

// delete implementation, no exception throwing
void DeleteImplNoThrow
	(
	void *pv,
	gpos::IMemoryPool::EAllocationType eat
	);


#endif // !GPOS_IMemoryPool_H

// EOF

