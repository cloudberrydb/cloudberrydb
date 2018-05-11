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

			// implementation of placement new with memory pool
			void *NewImpl
				(
				SIZE_T size,
				const CHAR *filename,
				ULONG line,
				EAllocationType type
				);

			// implementation of array-new with memory pool
			template <typename T>
			T* NewArrayImpl
				(
				SIZE_T num_elements,
				const CHAR *filename,
				ULONG line
				)
			{
				T *array = static_cast<T*>(NewImpl(
					sizeof(T) * num_elements,
					filename,
					line,
					EatArray));
				for (SIZE_T idx = 0; idx < num_elements; ++idx) {
					try {
						new(array + idx) T();
					} catch (...) {
						// If any element's constructor throws, deconstruct
						// previous objects and reclaim memory before rethrowing.
						for (SIZE_T destroy_idx = idx - 1; destroy_idx < idx; --destroy_idx) {
							array[destroy_idx].~T();
						}
						DeleteImpl(array, EatArray);
						throw;
					}
				}
				return array;
			}

			// delete implementation
			static
			void DeleteImpl
				(
				void *ptr,
				EAllocationType type
				);

			// allocate memory; return NULL if the memory could not be allocated
			virtual
			void *Allocate
				(
				const ULONG num_bytes,
				const CHAR *filename,
				const ULONG line
				) = 0;

			// free memory previously allocated by a call to pvAllocate; NULL may be passed
			virtual
			void Free(void *memory) = 0;

			// prepare the memory pool to be deleted
			virtual
			void TearDown() = 0;

			// check if the pool stores a pointer to itself at the end of
			// the header of each allocated object;
			// this is used by the new/delete operators to determine the
			// memory pool that was used for allocation;
			virtual
			BOOL StoresPoolPointer() const = 0;

			// check if memory pool is thread-safe
			virtual
			BOOL IsThreadSafe() const = 0;

			// return the hash key of the memory pool
			virtual
			ULONG_PTR GetHashKey() const = 0;

			// return total allocated size
			virtual
			ULLONG TotalAllocatedSize() const = 0;

			// forwards to CMemoryPool implementation
			static
			ULONG SizeOfAlloc(const void *pv);

#ifdef GPOS_DEBUG

			// check if the memory pool keeps track of live objects
			virtual
			BOOL SupportsLiveObjectWalk() const = 0;

			// walk the live objects, calling pVisitor.visit() for each one
			virtual
			void WalkLiveObjects(IMemoryVisitor *visitor) = 0;

			// check if statistics tracking is supported
			virtual
			BOOL SupportsStatistics() const = 0;

			// return the current statistics
			virtual
			void UpdateStatistics(CMemoryPoolStatistics &statistics) = 0;

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

// Nested detail namespace for templated helper classes.
namespace delete_detail
{

// All-static helper class. Base version deletes unqualified pointers / arrays.
template <typename T>
class CDeleter {
	public:
		static void Delete(T* object) {
			if (NULL == object) {
				return;
			}

			// Invoke destructor, then free memory.
			object->~T();
			IMemoryPool::DeleteImpl(object, IMemoryPool::EatSingleton);
		}

		static void DeleteArray(T* object_array) {
			if (NULL == object_array) {
				return;
			}

			// Invoke destructor on each array element in reverse
			// order from construction.
			const SIZE_T  num_elements = IMemoryPool::SizeOfAlloc(object_array) / sizeof(T);
			for (SIZE_T idx = num_elements - 1; idx < num_elements; --idx) {
				object_array[idx].~T();
			}

			// Free memory.
			IMemoryPool::DeleteImpl(object_array, IMemoryPool::EatArray);
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

// Overloading placement variant of singleton new operator. Used to allocate
// arbitrary objects from an IMemoryPool. This does not affect the ordinary
// built-in 'new', and is used only when placement-new is invoked with the
// specific type signature defined below.
inline void *operator new
	(
	gpos::SIZE_T size,
	gpos::IMemoryPool *mp,
	const gpos::CHAR *filename,
	gpos::ULONG line
	)
{
	return mp->NewImpl(size, filename, line, gpos::IMemoryPool::EatSingleton);
}

// Corresponding placement variant of delete operator. Note that, for delete
// statements in general, the compiler can not determine which overloaded
// version of new was used to allocate memory originally, and the global
// non-placement version is used. This placement version of 'delete' is used
// *only* when a constructor throws an exception, and the version of 'new' is
// known to be the one declared above.
inline void operator delete
	(
	void *ptr,
	gpos::IMemoryPool*,
	const gpos::CHAR*,
	gpos::ULONG
	)
{
	// Reclaim memory after constructor throws exception.
	gpos::IMemoryPool::DeleteImpl(ptr, gpos::IMemoryPool::EatSingleton);
}

// Placement new-style macro to do 'new' with a memory pool. Anything allocated
// with this *must* be deleted by GPOS_DELETE, *not* the ordinary delete
// operator.
#define GPOS_NEW(mp) new(mp, __FILE__, __LINE__)

// Replacement for array-new. Conceptually equivalent to
// 'new(mp) datatype[count]'. Any arrays allocated with this *must* be deleted
// by GPOS_DELETE_ARRAY, *not* the ordinary delete[] operator.
//
// NOTE: Unlike singleton new, we do not overload the built-in new operator for
// arrays, because when we do so the C++ compiler adds its own book-keeping
// information to the allocation in a non-portable way such that we can not
// recover GPOS' own book-keeping information reliably.
#define GPOS_NEW_ARRAY(mp, datatype, count) \
	mp->NewArrayImpl<datatype>(count, __FILE__, __LINE__)

// Delete a singleton object allocated by GPOS_NEW().
template <typename T>
void GPOS_DELETE(T* object) {
	::gpos::delete_detail::CDeleter<T>::Delete(object);
}

// Delete an array allocated by GPOS_NEW_ARRAY().
template <typename T>
void GPOS_DELETE_ARRAY(T* object_array) {
	::gpos::delete_detail::CDeleter<T>::DeleteArray(object_array);
}


#endif // !GPOS_IMemoryPool_H

// EOF

