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

#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCacheEntry.h"

#include "gpos/sync/CSpinlock.h"

#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/common/CSyncHashtableIter.h"
#include "gpos/common/CSyncHashtableAccessByIter.h"

#include "gpos/sync/CAutoSpinlock.h"

#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CWorker.h"


// setting the cache quota to 0 means unlimited
#define UNLIMITED_CACHE_QUOTA 0

// no. of hashtable buckets
#define CACHE_HT_NUM_OF_BUCKETS 1000

// eligible to delete
#define EXPECTED_REF_COUNT_FOR_DELETE 1

using namespace gpos;

namespace gpos
{
	//prototype
	template <class T, class K>
	class CCacheAccessor;

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
	template <class T, class K>
	class CCache
	{
		friend class CCacheAccessor<T, K>;

		public:
			// type definition of key hashing and equality functions
			typedef ULONG (*HashFuncPtr)(const K&);
			typedef BOOL (*EqualFuncPtr)(const K&, const K&);

		private:

			typedef CCacheEntry<T, K> CCacheHashTableEntry;

			// type definition of hashtable, accessor and iterator
			typedef CSyncHashtable<CCacheHashTableEntry, K, CSpinlockCache>
				CCacheHashtable;
			typedef CSyncHashtableAccessByKey<CCacheHashTableEntry, K, CSpinlockCache>
				CCacheHashtableAccessor;
			typedef CSyncHashtableIter<CCacheHashTableEntry, K, CSpinlockCache>
				CCacheHashtableIter;
			typedef CSyncHashtableAccessByIter<CCacheHashTableEntry, K, CSpinlockCache>
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
			HashFuncPtr m_pfuncHash;

			// a pointer to key equality function
			EqualFuncPtr m_pfuncEqual;

			// synchronized hash table; used to store and lookup entries
			CCacheHashtable m_sht;

			// the clock hand for gclock eviction policy
			CCacheHashtableIter *m_chtitClockHand;

			// inserts a new object
			CCacheHashTableEntry *PceInsert(CCacheHashTableEntry *pce)
			{
				GPOS_ASSERT(NULL != pce);

				if (0 != m_ullCacheQuota && m_ullCacheSize > m_ullCacheQuota)
				{
					EvictEntries();
				}

				CCacheHashtableAccessor shtacc(m_sht, pce->PKey());

				// if we allow duplicates, insertion can be directly made;
				// if we do not allow duplicates, we need to check first
				CCacheHashTableEntry *pceReturn = pce;
				CCacheHashTableEntry *pceFound = NULL;
				if (!m_fUnique  ||
					(m_fUnique && NULL == (pceFound = shtacc.PtLookup())))
				{
					shtacc.Insert(pce);
					UllExchangeAdd((volatile ULLONG *)&m_ullCacheSize, pce->Pmp()->UllTotalAllocatedSize());
				}
				else
				{
					pceReturn = pceFound;
				}

				pceReturn->SetGClockCounter(m_ulGClockInitCounter);
				pceReturn->IncRefCount();

				return pceReturn;
			}

			// returns the first object matching the given key
			CCacheHashTableEntry *PceLookup(const K pKey)
			{
				CCacheHashtableAccessor shtacc(m_sht, pKey);

				// look for the first unmarked entry matching the given key
				CCacheHashTableEntry *pce = shtacc.PtLookup();
				while (NULL != pce && pce->FMarkedForDeletion())
				{
					pce = shtacc.PtNext(pce);
				}

				if (NULL != pce)
				{
					pce->SetGClockCounter(m_ulGClockInitCounter);
					// increase ref count, since CCacheHashtableAccessor points to the obj
					// ref count will be decreased when CCacheHashtableAccessor will be destroyed
					pce->IncRefCount();
				}

				return pce;
			}

			// releases entry's memory if deleted
			void ReleaseEntry(CCacheHashTableEntry *pce)
			{
				GPOS_ASSERT(NULL != pce);

				// CacheEntry's destructor is the only place where ref count go from 1(EXPECTED_REF_COUNT_FOR_DELETE) to 0
				GPOS_ASSERT(EXPECTED_REF_COUNT_FOR_DELETE < pce->UlRefCount() &&
						    "Releasing entry for which CCacheEntry has the ownership");

				BOOL fDeleted = false;

				// scope for hashtable accessor
				{
					CCacheHashtableAccessor shtacc(m_sht, pce->PKey());
					pce->DecRefCount();

					if (EXPECTED_REF_COUNT_FOR_DELETE == pce->UlRefCount() && pce->FMarkedForDeletion())
					{
						// remove entry from hash table
						shtacc.Remove(pce);
						fDeleted = true;
					}
				}

				if (fDeleted)
				{
					// delete cache entry
					DestroyCacheEntry(pce);
				}
			}

			// returns the next entry in the hash chain with a key matching the given object
			CCacheHashTableEntry *PceNext(CCacheHashTableEntry *pce)
			{
				GPOS_ASSERT(NULL != pce);

				CCacheHashTableEntry *pceCurrent = pce;
				K pvKey = pceCurrent->PKey();
				CCacheHashtableAccessor shtacc(m_sht, pvKey);

				// move forward until we find unmarked entry with the same key
				CCacheHashTableEntry *pceNext = shtacc.PtNext(pceCurrent);
				while (NULL != pceNext && pceNext->FMarkedForDeletion())
				{
					pceNext = shtacc.PtNext(pceNext);
				}

				if  (NULL != pceNext)
				{
					pceNext->IncRefCount();
				}
				GPOS_ASSERT_IMP(FUnique(), NULL == pceNext);

				return pceNext;
			}

			// Evict entries until the cache size is within the cache quota or until
			// the cache does not have any more evictable entries
			void EvictEntries()
			{
				GPOS_ASSERT(0 != m_ullCacheQuota || "Cannot evict from an unlimited sized cache");

				if (FCompareSwap(&m_ulEvictionLock, 0, 1))
				{
					if (m_ullCacheSize > m_ullCacheQuota)
					{
						double dToFree = static_cast<double>(static_cast<double>(m_ullCacheSize) -
								static_cast<double>(m_ullCacheQuota) * (1.0 - m_fEvictionFactor));
						GPOS_ASSERT(0 < dToFree);

						ULLONG ullToFree = static_cast<ULLONG>(dToFree);
						ULLONG ullTotalFreed = 0;

						// retryCount indicates the number of times we want to circle around the buckets.
						// depending on our previous cursor position (e.g., may be at the very last bucket)
						// we may end up circling 1 less time than the retry count
						for (ULONG ulRetryCount = 0; ulRetryCount < m_ulGClockInitCounter + 1; ulRetryCount++)
						{
							ullTotalFreed = EvictEntriesOnePass(ullTotalFreed, ullToFree);

							if (ullTotalFreed >= ullToFree)
							{
								// successfully freed up enough. The final action must have been a valid eviction
								GPOS_ASSERT(m_fClockHandAdvanced);
								// no need to retry
								break;
							}

							// exhausted the iterator, so rewind it
							m_chtitClockHand->RewindIterator();
						}

						if (0 < ullTotalFreed)
						{
							++m_ullEvictionCounter;
						}
					}

					// release the lock
					m_ulEvictionLock = 0;
				}
			}

			// cleans up when cache is destroyed
			void Cleanup()
			{
				m_sht.DestroyEntries(DestroyCacheEntryWithRefCountTest);
				GPOS_DELETE(m_chtitClockHand);
				m_chtitClockHand = NULL;
			}

			static
			void DestroyCacheEntryWithRefCountTest(CCacheHashTableEntry *pce)
			{
				GPOS_ASSERT(NULL != pce);

				// This assert is valid only when ccache get deleted. At that point nobody should hold a pointer to an object in CCache.
				// If ref count is not 1, then we possibly have a leak.
				GPOS_ASSERT(EXPECTED_REF_COUNT_FOR_DELETE == pce->UlRefCount() && "Expected CCacheEntry's refcount to be 1 before it get deleted");

				DestroyCacheEntry(pce);
			}

			// destroy a cache entry
			static
			void DestroyCacheEntry(CCacheHashTableEntry *pce)
			{
				GPOS_ASSERT(NULL != pce);

				// destroy the object before deleting memory pool. This cover the case where object & cacheentry use same memory pool
				IMemoryPool* pmp = pce->Pmp();
				GPOS_DELETE(pce);
				CMemoryPoolManager::Pmpm()->Destroy(pmp);
			}

			// evict entries by making one pass through the hash table buckets
			ULLONG EvictEntriesOnePass(ULLONG ullTotalFreed, ULLONG ullToFree)
			{
				while ((ullTotalFreed < ullToFree)
					&& (m_fClockHandAdvanced || m_chtitClockHand->FAdvance()))
				{
					m_fClockHandAdvanced = false;
					CCacheHashTableEntry *pt = NULL;
					BOOL fDeleted = false;
					// Scope for CCacheHashtableIterAccessor
					{
						CCacheHashtableIterAccessor shtitacc(*m_chtitClockHand);

						if (NULL != (pt = shtitacc.Pt()))
						{
							// can only remove when the clock hand points to a entry with 0 gclock counter
							if (0 == pt->ULGetGClockCounter())
							{
								// can only remove if no one else is using this entry.
								// for our self reference we are using CCacheHashtableIterAccessor
								// to directly access the entry. Therefore, we are not causing a
								// bump to ref counter
								if (EXPECTED_REF_COUNT_FOR_DELETE == pt->UlRefCount())
								{
									// remove advances iterator automatically
									shtitacc.Remove(pt);
									fDeleted = true;

									// successfully removing an entry automatically advances the iterator, so don't call FAdvance()
									m_fClockHandAdvanced = true;

									ULLONG ullFreed = pt->Pmp()->UllTotalAllocatedSize();
									UllExchangeAdd((volatile ULLONG *) &m_ullCacheSize,
											-ullFreed);
									ullTotalFreed += ullFreed;
								}
							}
							else
							{
								pt->DecrementGClockCounter();
							}
						}
					}

					// now free the memory of the evicted entry
					if (fDeleted)
					{
						GPOS_ASSERT(NULL != pt);
						DestroyCacheEntry(pt);
					}
				}
				return ullTotalFreed;
			}

		public:

			// ctor
			CCache
				(
				IMemoryPool *pmp,
				BOOL fUnique,
				ULLONG ullCacheQuota,
				ULONG ulGClockInitCounter,
				HashFuncPtr pfuncHash,
				EqualFuncPtr pfuncEqual
				)
			:
			m_pmp(pmp),
			m_fUnique(fUnique),
			m_ullCacheSize(0),
			m_ullCacheQuota(ullCacheQuota),
			m_ulGClockInitCounter(ulGClockInitCounter),
			m_fEvictionFactor((float)0.1),
			m_ullEvictionCounter(0),
			m_ulEvictionLock(0),
			m_fClockHandAdvanced(false),
			m_pfuncHash(pfuncHash),
			m_pfuncEqual(pfuncEqual)
			{
				GPOS_ASSERT(NULL != m_pmp &&
						    "Cache memory pool could not be initialized");

				GPOS_ASSERT(0 != ulGClockInitCounter);

				// initialize hashtable
				m_sht.Init
					(
					m_pmp,
					CACHE_HT_NUM_OF_BUCKETS,
					GPOS_OFFSET(CCacheHashTableEntry, m_linkHash),
					GPOS_OFFSET(CCacheHashTableEntry, m_pKey),
					(&CCacheHashTableEntry::m_pInvalidKey),
					m_pfuncHash,
					m_pfuncEqual
					);

				m_chtitClockHand = GPOS_NEW(pmp) CCacheHashtableIter(m_sht);
			}

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
			ULLONG UllTotalAllocatedSize()
			{
				return m_ullCacheSize;
			}

			// return memory quota of the cache
			ULLONG UllCacheQuota()
			{
				return m_ullCacheQuota;
			}

			// return number of times this cache underwent eviction
			ULLONG UllEvictionCounter()
			{
				return m_ullEvictionCounter;
			}

			// sets the cache quota
			void SetCacheQuota(ULLONG ullNewQuota)
			{
				m_ullCacheQuota = ullNewQuota;

				if (0 != m_ullCacheQuota && m_ullCacheSize > m_ullCacheQuota)
				{
					EvictEntries();
				}
			}

			// return eviction factor (what percentage of cache size to evict)
			float FGetEvictionFactor()
			{
				return m_fEvictionFactor;
			}

    }; //  CCache

	// invalid key
	template <class T, class K>
	const K CCacheEntry<T, K>::m_pInvalidKey = NULL;

} // namespace gpos

#endif // GPOS_CCACHE_H_

// EOF
