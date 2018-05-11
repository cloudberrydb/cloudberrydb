//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCacheEntry.h
//
//	@doc:
//		Definition of cache class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CCACHEENTRY_H_
#define GPOS_CCACHEENTRY_H_

#include "gpos/memory/IMemoryPool.h"
#include "gpos/common/CList.h"


// setting the cache quota to 0 means unlimited
#define UNLIMITED_CACHE_QUOTA 0

// no. of hashtable buckets
#define CACHE_HT_NUM_OF_BUCKETS 1000

using namespace gpos;

namespace gpos
{
	//prototype
	template <class T, class K>
	class CCacheAccessor;



	// Template to convert object (any data type) to pointer
	template <class T>
	struct ptr
	{
		T* operator()(T& obj)
		{
			return &obj;
		}

		const T* operator()(const T& obj) {
			return &obj;
		}
	};

	// Template specialization for pointer type
	template <class T>
	struct ptr<T*>
	{
		T* operator()(T* ptr)
		{
			return ptr;
		}
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CCacheEntry
	//
	//	@doc:
	//		Definition of cache entry;
	//
	//		Each cache entry is a container that holds information about one
	//		cached object.
	//
	//---------------------------------------------------------------------------
	template <class T, class K>
	class CCacheEntry
	{
		private:

			// allocated memory pool to the cached object
			IMemoryPool *m_pmp;

			// value that needs to be cached
			T m_pVal;

			// true if this entry is marked for deletion
			BOOL m_fDeleted;

			// gclock counter; an entry is eligible for eviction if this
			// counter drops to 0 and the entry is not pinned
			ULONG m_ulGClockCounter;

		public:

			// ctor
			CCacheEntry
				(
				IMemoryPool *pmp,
				K pKey,
				T pVal,
				ULONG ulGClockCounter
				)
				:
				m_pmp(pmp),
				m_pVal(pVal),
				m_fDeleted(false),
				m_ulGClockCounter(ulGClockCounter),
				m_pKey(pKey)
			{
				// CCache entry has the ownership now. So ideally any time ref count can't go lesser than 1.
				// In destructor, we decrease it from 1 to 0.
				IncRefCount();
			}

			// dtor
			virtual
			~CCacheEntry()
			{
				// Decrease ref count of m_pVal to get destroyed by itself if ref count is 0
				DecRefCount();
			}

			// gets the key of cached object
			K PKey() const
			{
				return m_pKey;
			}

			// gets the value of cached object
			T PVal() const
			{
				return m_pVal;
			}

			// gets the memory pool of cached object
			IMemoryPool *Pmp() const
			{
				return m_pmp;
			}

			// marks entry as deleted
			void MarkForDeletion()
			{
				m_fDeleted = true;
			}

			// returns true if entry is marked as deleted
			BOOL FMarkedForDeletion() const
			{
				return m_fDeleted;
			}

			// get value's ref-count
			ULONG UlRefCount() const
			{
				return (ULONG)ptr<T>()(m_pVal)->UlpRefCount();
			}

			// increments value's ref-count
			void IncRefCount()
			{
				ptr<T>()(m_pVal)->AddRef();
			}

			//decrements value's ref-count
			void DecRefCount()
			{
				ptr<T>()(m_pVal)->Release();
			}

			// sets the gclock counter for an entry; useful for updating counter upon access
			void SetGClockCounter(ULONG ulGClockCounter)
			{
				m_ulGClockCounter = ulGClockCounter;
			}

			// decrements the gclock counter for an entry during eviction process
			void DecrementGClockCounter()
			{
				m_ulGClockCounter--;
			}

			// returns the current value of the gclock counter
			ULONG ULGetGClockCounter()
			{
				return m_ulGClockCounter;
			}

			// the following data members are public because they
			// need to be used by GPOS_OFFSET macro for list construction

			// a pointer to entry's key
			K m_pKey;

			// link used to maintain entries in a hashtable
			SLink m_linkHash;

			// invalid key
			static
			const K m_pInvalidKey;

	}; // CCacheEntry

} // namespace gpos

#endif // GPOS_CCACHEENTRY_H_

// EOF
