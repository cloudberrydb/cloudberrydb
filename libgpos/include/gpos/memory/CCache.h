//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCache.h
//
//	@doc:
//		Definition of cache class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CCACHE_H_
#define GPOS_CCACHE_H_

#include "gpos/base.h"

#include "gpos/common/CList.h"
#include "gpos/common/CSyncHashtable.h"

#include "gpos/memory/CCacheFactory.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/IMemoryPool.h"

#include "gpos/sync/CSpinlock.h"

// setting the cache quota to 0 means unlimited
#define UNLIMITED_CACHE_QUOTA 0

namespace gpos
{
	//prototype
	class CCacheAccessorBase;

	//---------------------------------------------------------------------------
	//	@class:
	//		CCache
	//
	//	@doc:
	//		Definition of cache;
	//
	//		Cache stores key-value pairs of cached objects. Keys are hashed
	//		using a hashing function pointer pfuncHash. Key equality is determined
	//		using a function pfuncEqual. The value of a cached object contains
	//		object's key as member.
	//
	//		Cache API allows client to store, lookup, delete, and iterate over cached
	//		objects.
	//
	//		Cache can only be accessed through the CCacheAccessor friend class.
	//		The current implementation has a fixed gclock based eviction policy.
	//
	//---------------------------------------------------------------------------
	class CCache
	{
		friend class CCacheAccessorBase;

		private:

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
			class CCacheEntry
			{
				private:

					// allocated memory pool to the cached object
					IMemoryPool *m_pmp;

					// a pointer to cached object's value
					VOID_PTR m_pvVal;

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
						VOID_PTR pvKey,
						VOID_PTR pvVal,
						ULONG ulGClockCounter
						);

					// dtor
					virtual
					~CCacheEntry();

					// gets the key of cached object
					VOID_PTR PvKey() const
					{
						return m_pvKey;
					}

					// gets the value of cached object
					VOID_PTR PvVal() const
					{
						return m_pvVal;
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
					VOID_PTR m_pvKey;

					// link used to maintain entries in a hashtable
					SLink m_linkHash;

					// invalid key
					static
					const VOID_PTR m_pvInvalidKey;

			}; // CCacheEntry


			// type definition of hashtable, accessor and iterator
			typedef CSyncHashtable<CCacheEntry, VOID_PTR, CSpinlockCache>
				CCacheHashtable;
			typedef CSyncHashtableAccessByKey<CCacheEntry, VOID_PTR, CSpinlockCache>
				CCacheHashtableAccessor;
			typedef CSyncHashtableIter<CCacheEntry, VOID_PTR, CSpinlockCache>
				CCacheHashtableIter;
			typedef CSyncHashtableAccessByIter<CCacheEntry, VOID_PTR, CSpinlockCache>
					CCacheHashtableIterAccessor;

			// memory pool for allocating hashtable and cache entries
			IMemoryPool *m_pmp;

			// true if cache does not allow multiple objects with the same key
			BOOL m_fUnique;

			// total size of the cache in bytes
			ULLONG m_ullCacheSize;

			// quota of the cache in bytes; 0 means unlimited quota
			ULLONG m_ullCacheQuota;

			// initial value of gclock counter for new entries
			ULONG m_ulGClockInitCounter;

			// what percent of the cache size to evict
			float m_fEvictionFactor;

			// number of times cache entries were evicted
			ULLONG m_ullEvictionCounter;

			// atomic lock for eviction; only one thread can execute eviction process at a time
			volatile ULONG m_ulEvictionLock;

			// if the gclock hand was already advanced and therefore can serve the next entry
			BOOL m_fClockHandAdvanced;

			// a pointer to key hashing function
			CCacheFactory::HashFuncPtr m_pfuncHash;

			// a pointer to key equality function
			CCacheFactory::EqualFuncPtr m_pfuncEqual;

			// synchronized hash table; used to store and lookup entries
			CCacheHashtable m_sht;

			// the clock hand for gclock eviction policy
			CCacheHashtableIter *m_chtitClockHand;

			// inserts a new object
			CCacheEntry *PceInsert(CCacheEntry *pce);

			// returns the first object matching the given key
			CCacheEntry *PceLookup(const VOID_PTR pvKey);

			// releases entry's memory if deleted
			void ReleaseEntry(CCacheEntry *pce);

			// returns the next entry in the hash chain with a key matching the given object
			CCacheEntry *PceNext(CCacheEntry *pce);

			// Evict entries until the cache size is within the cache quota or until
			// the cache does not have any more evictable entries
			void EvictEntries();

			// cleans up when cache is destroyed
			void Cleanup();

			// destroy a cache entry
			static
			void DestroyCacheEntry(CCacheEntry *pce);

			// evict entries by making one pass through the hash table buckets
			ULLONG EvictEntriesOnePass(ULLONG ullTotalFreed, ULLONG ullToFree);

		public:

			// ctor
			CCache
				(
				IMemoryPool *pmp,
				BOOL fUnique,
				ULLONG ullCacheQuota,
				ULONG ulGClockInitCounter,
				CCacheFactory::HashFuncPtr pfuncHash,
				CCacheFactory::EqualFuncPtr pfuncEqual
				);

			// dtor
			~CCache()
			{
				Cleanup();
			}

			// does cache allow duplicate keys?
			BOOL FUnique() const
			{
				return m_fUnique;
			}

			// return number of cache entries
			ULONG_PTR UlpEntries() const
			{
				return m_sht.UlpEntries();
			}

			// return total allocated size in bytes
			ULLONG UllTotalAllocatedSize();

			// return memory quota of the cache
			ULLONG UllCacheQuota();

			// return number of times this cache underwent eviction
			ULLONG UllEvictionCounter();

			// sets the cache quota
			void SetCacheQuota(ULLONG ullNewQuota);

			// return eviction factor (what percentage of cache size to evict)
			float FGetEvictionFactor();

    }; //  CCache

} // namespace gpos

#endif // GPOS_CCACHE_H_

// EOF
