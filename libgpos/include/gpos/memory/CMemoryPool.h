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
#include "gpos/error/CException.h"
#include "gpos/common/CLink.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/memory/CMemoryPoolStatistics.h"


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
	// prototypes
	class IMemoryVisitor;
	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPool
	//
	//	@doc:
	//		Interface class for memory pool
	//
	//---------------------------------------------------------------------------
	class CMemoryPool
	{

		// manager accesses internal functionality
		friend class CMemoryPoolManager;

		private:

			// common header for each allocation
			struct AllocHeader
			{
				// pointer to pool
				CMemoryPool *m_mp;

				// allocation request size
				ULONG m_alloc;
			};

			// reference counter
			ULONG m_ref_counter;

			// hash key is only set by pool manager
			ULONG_PTR m_hash_key;

			// underlying memory pool - optional
			CMemoryPool *m_underlying_memory_pool;

			// flag indicating if this pool owns the underlying pool
			const BOOL m_owns_underlying_memory_pool;

			// flag indicating if memory pool is thread-safe
			const BOOL m_thread_safe;

#ifdef GPOS_DEBUG
			// stack where pool is created
			CStackDescriptor m_stack_desc;
#endif // GPOS_DEBUG

			// link structure to manage pools
			SLink m_link;

		protected:

			// ctor
			CMemoryPool
				(
				CMemoryPool *underlying_memory_pool,
				BOOL owns_underlying_memory_pool,
				BOOL thread_safe
				);

			// underlying pool accessor
			CMemoryPool *GetUnderlyingMemoryPool() const
			{
				return m_underlying_memory_pool;
			}

			// invalid memory pool key
			static
			const ULONG_PTR m_invalid;

		public:

			enum EAllocationType
			{
				EatSingleton = 0x7f,
				EatArray = 0x7e
			};

			// dtor
			virtual
			~CMemoryPool() = 0;

			// prepare the memory pool to be deleted
			virtual
			void TearDown()
			{
				if (m_owns_underlying_memory_pool)
				{
					m_underlying_memory_pool->TearDown();
				}
			}

			// check if memory pool is thread-safe
			virtual
			BOOL IsThreadSafe() const
			{
				return m_thread_safe;
			}

			// hash key accessor
			virtual
			ULONG_PTR GetHashKey() const
			{
				return m_hash_key;
			}

			// get allocation size
			static
			ULONG GetAllocSize
				(
				ULONG requested
				)
			{
				return GPOS_SIZEOF(AllocHeader) +
				       GPOS_MEM_ALIGNED_SIZE(requested + GPOS_MEM_GUARD_SIZE);
			}

			// set allocation header and footer, return pointer to user data
			void *FinalizeAlloc(void *ptr, ULONG alloc, EAllocationType eat);

			// return allocation to owning memory pool
			inline static void
			FreeAlloc
				(
				void *ptr,
				EAllocationType eat
				)
			{
				GPOS_ASSERT(ptr != NULL);

				AllocHeader *header = static_cast<AllocHeader*>(ptr) - 1;
				BYTE *alloc_type = static_cast<BYTE*>(ptr) + header->m_alloc;
				GPOS_RTL_ASSERT(*alloc_type == eat);
				header->m_mp->Free(header);
			}

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


			// check if the pool stores a pointer to itself at the end of
			// the header of each allocated object;
			virtual
			BOOL StoresPoolPointer() const
			{
				return false;
			}

			// return total allocated size
			virtual
			ULLONG TotalAllocatedSize() const
			{
				GPOS_ASSERT(!"not supported");
				return 0;
			}

			// determine the size (in bytes) of an allocation that
			// was made from a CMemoryPool
			static
			ULONG SizeOfAlloc(const void *ptr);

#ifdef GPOS_DEBUG

			// check if the memory pool keeps track of live objects
			virtual
			BOOL SupportsLiveObjectWalk() const
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
			BOOL SupportsStatistics() const
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
	// Overloading placement variant of singleton new operator. Used to allocate
	// arbitrary objects from an CMemoryPool. This does not affect the ordinary
	// built-in 'new', and is used only when placement-new is invoked with the
	// specific type signature defined below.

#ifdef GPOS_DEBUG

	//---------------------------------------------------------------------------
	//	@function:
	//		CMemoryPool::operator <<
	//
	//	@doc:
	//		Print function for memory pools
	//
	//---------------------------------------------------------------------------
	inline IOstream &
	operator <<
		(
		IOstream &os,
		CMemoryPool &mp
		)
	{
		return mp.OsPrint(os);
	}
#endif // GPOS_DEBUG

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
				CMemoryPool::DeleteImpl(object, CMemoryPool::EatSingleton);
			}

			static void DeleteArray(T* object_array) {
				if (NULL == object_array) {
					return;
				}

				// Invoke destructor on each array element in reverse
				// order from construction.
				const SIZE_T  num_elements = CMemoryPool::SizeOfAlloc(object_array) / sizeof(T);
				for (SIZE_T idx = num_elements - 1; idx < num_elements; --idx) {
					object_array[idx].~T();
				}

				// Free memory.
				CMemoryPool::DeleteImpl(object_array, CMemoryPool::EatArray);
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
// arbitrary objects from an CMemoryPool. This does not affect the ordinary
// built-in 'new', and is used only when placement-new is invoked with the
// specific type signature defined below.
inline void *operator new
	(
	gpos::SIZE_T size,
	gpos::CMemoryPool *mp,
	const gpos::CHAR *filename,
	gpos::ULONG line
	)
{
	return mp->NewImpl(size, filename, line, gpos::CMemoryPool::EatSingleton);
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
	gpos::CMemoryPool*,
	const gpos::CHAR*,
	gpos::ULONG
	)
{
	// Reclaim memory after constructor throws exception.
	gpos::CMemoryPool::DeleteImpl(ptr, gpos::CMemoryPool::EatSingleton);
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
#endif // !GPOS_CMemoryPool_H

// EOF

