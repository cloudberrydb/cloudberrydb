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

			// forwards to CMemoryPool implementation
			static
			ULONG UlSizeOfAlloc(const void *pv);

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

	// new implementation
	void* NewImpl
		(
		gpos::IMemoryPool *pmp,
		gpos::SIZE_T cSize,
		const gpos::CHAR *szFilename,
		gpos::ULONG ulLine,
		gpos::IMemoryPool::EAllocationType eat
		);

	template <typename T>
	T* NewArrayImpl
		(
		IMemoryPool *pmp,
		SIZE_T cElements,
		const CHAR *szFilename,
		ULONG ulLine
		)
	{
		T *rgTArray = static_cast<T*>(NewImpl(pmp, sizeof(T) * cElements, szFilename, ulLine, IMemoryPool::EatArray));
		for (SIZE_T uIdx = 0; uIdx < cElements; ++uIdx) {
			new(rgTArray + uIdx) T();
		}
		return rgTArray;
	}

	// delete implementation
	void DeleteImpl
		(
		void *pv,
		gpos::IMemoryPool::EAllocationType eat
		);

namespace delete_detail {

// All-static helper class. Base version deletes unqualified pointers / arrays.
template <typename T>
class CDeleter {
	public:
		static void Delete(T* object) {
			if (NULL == object) {
				return;
			}
			object->~T();
			DeleteImpl(object, IMemoryPool::EatSingleton);
		}

		static void DeleteArray(T* object_array) {
			if (NULL == object_array) {
				return;
			}

			const SIZE_T cElements = IMemoryPool::UlSizeOfAlloc(object_array) / sizeof(T);
			for (SIZE_T uIdx = 0; uIdx < cElements; ++uIdx) {
				object_array[uIdx].~T();
			}

			DeleteImpl(object_array, IMemoryPool::EatArray);
		}
};

// Specialization for const-qualified types.
template <typename T>
class CDeleter<const T> {
	public:
		static void Delete(const T* object) {
			CDeleter<T>::Delete(const_cast<T*>(object));
		}

		static void DeleteArray(const T* object_array) {
			CDeleter<T>::DeleteArray(const_cast<T*>(object_array));
		}
};

// Specialization for volatile-qualified types.
template <typename T>
class CDeleter<volatile T> {
	public:
		static void Delete(volatile T* object) {
			CDeleter<T>::Delete(const_cast<T*>(object));
		}

		static void DeleteArray(volatile T* object_array) {
			CDeleter<T>::DeleteArray(const_cast<T*>(object_array));
		}
};

}  // namespace delete_detail

} // gpos

//---------------------------------------------------------------------------
// Overloading placement variant of singleton new operator. Used to allocate
// arbitrary objects from an IMemoryPool. This does not affect the ordinary
// built-in 'new', and is used only when placement-new is invoked with the
// specific type signature defined below.
//---------------------------------------------------------------------------
inline void *operator new
	(
	gpos::SIZE_T cSize,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	)
{
	return gpos::NewImpl(pmp, cSize, szFilename, cLine, gpos::IMemoryPool::EatSingleton);
}

//---------------------------------------------------------------------------
// Corresponding placement variants of delete operator. Note that, for delete
// statements in general, the compiler can not determine which overloaded
// version of new was used to allocate memory originally, and the global
// non-placement version is used. These placement versions of 'delete' are used
// only when a constructor throws an exception, and the version of 'new' is
// known to be the one declared above.
//---------------------------------------------------------------------------
inline void operator delete
	(
	void *pv,
	gpos::IMemoryPool*,
	const gpos::CHAR*,
	gpos::ULONG
	)
{
	gpos::DeleteImpl(pv, gpos::IMemoryPool::EatSingleton);
}

// placement new definition
#define New(pmp) new(pmp, __FILE__, __LINE__)

#define GPOS_NEW_ARRAY(pmp, datatype, count) \
	NewArrayImpl<datatype>(pmp, count, __FILE__, __LINE__)

template <typename T>
void GPOS_DELETE(T* object) {
	::gpos::delete_detail::CDeleter<T>::Delete(object);
}

template <typename T>
void GPOS_DELETE_ARRAY(T* object_array) {
	::gpos::delete_detail::CDeleter<T>::DeleteArray(object_array);
}


#endif // !GPOS_IMemoryPool_H

// EOF

