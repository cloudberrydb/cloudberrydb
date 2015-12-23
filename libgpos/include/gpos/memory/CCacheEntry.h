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


// setting the cache quota to 0 means unlimited
#define UNLIMITED_CACHE_QUOTA 0

// no. of hashtable buckets
#define CACHE_HT_NUM_OF_BUCKETS 1000

using namespace gpos;

namespace gpos
{
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

			// a pointer to cached object's value
			T m_pVal;

			// true if this entry is marked for deletion
			BOOL m_fDeleted;

			// no. of active accessors of the entry
			ULONG m_ulRefCount;

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
				m_ulRefCount(0),
				m_ulGClockCounter(ulGClockCounter),
				m_pKey(pKey)
			{

			}

			// dtor
			virtual
			~CCacheEntry()
			{
				GPOS_ASSERT(0 == UlRefCount() &&
						"Destroying a cache entry with non-zero ref. count");
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

			// gets entry's ref-count
			ULONG UlRefCount() const
			{
				return m_ulRefCount;
			}

			// increments entry's ref-count
			void IncRefCount()
			{
				m_ulRefCount++;
			}

			//decrements entry's ref-count
			void DecRefCount()
			{
				m_ulRefCount--;
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
