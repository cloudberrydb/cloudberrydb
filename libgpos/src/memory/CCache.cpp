//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCache.cpp
//
//	@doc:
//		 Function implementation of CCache
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/common/CSyncHashtableIter.h"
#include "gpos/common/CSyncHashtableAccessByIter.h"

#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCache.h"

#include "gpos/sync/CAutoSpinlock.h"

#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CWorker.h"

using namespace gpos;


// no. of hashtable buckets
#define CACHE_HT_NUM_OF_BUCKETS 1000

// invalid key
const VOID_PTR CCache::CCacheEntry::m_pvInvalidKey = NULL;


//---------------------------------------------------------------------------
//	@function:
//		CCache::CCacheEntry::CCacheEntry
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCache::CCacheEntry::CCacheEntry
	(
	IMemoryPool *pmp,
	VOID_PTR pvKey,
	VOID_PTR pvVal,
	ULONG ulGClockCounter
	)
	:
	m_pmp(pmp),
	m_pvVal(pvVal),
	m_fDeleted(false),
	m_ulRefCount(0),
	m_ulGClockCounter(ulGClockCounter),
	m_pvKey(pvKey)
{}


//---------------------------------------------------------------------------
//	@function:
//		CCache::CCacheEntry::~CCacheEntry
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCache::CCacheEntry::~CCacheEntry()
{
	GPOS_ASSERT(0 == UlRefCount() &&
			"Destroying a cache entry with non-zero ref. count");
}


//---------------------------------------------------------------------------
//	@function:
//		CCache::CCache
//
//	@doc:
//		Ctor; initializes cache memory pool and hash table
//
//---------------------------------------------------------------------------
CCache::CCache
	(
	IMemoryPool *pmp,
	BOOL fUnique,
	ULLONG ullCacheQuota,
	ULONG ulGClockInitCounter,
	CCacheFactory::HashFuncPtr pfuncHash,
	CCacheFactory::EqualFuncPtr pfuncEqual
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
		GPOS_OFFSET(CCacheEntry, m_linkHash),
		GPOS_OFFSET(CCacheEntry, m_pvKey),
		&(CCacheEntry::m_pvInvalidKey),
		m_pfuncHash,
		m_pfuncEqual
		);

	m_chtitClockHand = GPOS_NEW(pmp) CCacheHashtableIter(m_sht);
}


//---------------------------------------------------------------------------
//	@function:
//		CCache::DestroyCacheEntry
//
//	@doc:
//		Destroy a cache entry
//
//---------------------------------------------------------------------------
void CCache::DestroyCacheEntry
	(
	CCacheEntry *pce
	)
{
	GPOS_ASSERT(NULL != pce);

	CMemoryPoolManager::Pmpm()->Destroy(pce->Pmp());
	GPOS_DELETE(pce);
}


//---------------------------------------------------------------------------
//	@function:
//		CCache::Cleanup
//
//	@doc:
//		 Destroys left-over cache entries
//
//---------------------------------------------------------------------------
void CCache::Cleanup()
{
	m_sht.DestroyEntries(DestroyCacheEntry);
	GPOS_DELETE(m_chtitClockHand);
	m_chtitClockHand = NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CCache::PceInsert
//
//	@doc:
//		Inserts a new object into the cache; the function returns a pointer
//		to the inserted object; if cache does not allow duplicates, and an
//		object with the same key already exists, insertion fails and the
//		existing object is returned; the function pins either the newly
//		inserted object, or the already existing object.
//
//---------------------------------------------------------------------------
CCache::CCacheEntry *
CCache::PceInsert
	(
	CCache::CCacheEntry *pce
	)
{
	GPOS_ASSERT(NULL != pce);

	if (0 != m_ullCacheQuota && m_ullCacheSize > m_ullCacheQuota)
	{
		EvictEntries();
	}

	CCacheHashtableAccessor shtacc(m_sht, pce->PvKey());

	// if we allow duplicates, insertion can be directly made;
	// if we do not allow duplicates, we need to check first
	CCache::CCacheEntry *pceReturn = pce;
	CCache::CCacheEntry *pceFound = NULL;
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


//---------------------------------------------------------------------------
//	@function:
//		CCache::PceLookup
//
//	@doc:
//		Returns the first cached object matching the given key
//
//---------------------------------------------------------------------------
CCache::CCacheEntry *
CCache::PceLookup
	(
	VOID_PTR pvKey
	)
{
	CCacheHashtableAccessor shtacc(m_sht, pvKey);

	// look for the first unmarked entry matching the given key
	CCache::CCacheEntry *pce = shtacc.PtLookup();
	while (NULL != pce && pce->FMarkedForDeletion())
	{
		pce = shtacc.PtNext(pce);
	}

	if (NULL != pce)
	{
		pce->SetGClockCounter(m_ulGClockInitCounter);
		pce->IncRefCount();
	}

	return pce;
}


//---------------------------------------------------------------------------
//	@function:
//		CCache::ReleaseEntry
//
//	@doc:
//		Decrements entry's ref-count. If entry is marked for deletion and
//		has no active references, its memory pool is destroyed.
//
//---------------------------------------------------------------------------
void
CCache::ReleaseEntry
	(
	CCache::CCacheEntry *pce
	)
{
	GPOS_ASSERT(NULL != pce);

	GPOS_ASSERT(0 < pce->UlRefCount() &&
			    "Releasing entry with non-zero ref-count");

	BOOL fDeleted = false;

	// scope for hashtable accessor
	{
		CCacheHashtableAccessor shtacc(m_sht, pce->PvKey());
		pce->DecRefCount();
		if (0 == pce->UlRefCount() && pce->FMarkedForDeletion())
		{
			// remove entry from hash table
			shtacc.Remove(pce);
			fDeleted = true;
		}
	}

	if (fDeleted)
	{
		// release entry's memory
		CMemoryPoolManager::Pmpm()->Destroy(pce->Pmp());
		GPOS_DELETE(pce);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCache::EvictEntriesOnePass
//
//	@doc:
//		Evict entries by making one pass through the hash table buckets
//
//---------------------------------------------------------------------------
ULLONG
CCache::EvictEntriesOnePass(ULLONG ullTotalFreed, ULLONG ullToFree)
{
	while ((ullTotalFreed < ullToFree)
		&& (m_fClockHandAdvanced || m_chtitClockHand->FAdvance()))
	{
		m_fClockHandAdvanced = false;
		CCacheEntry *pt = NULL;
		BOOL fDeleted = false;

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
					if (0 == pt->UlRefCount())
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
			// release entry's memory
			CMemoryPoolManager::Pmpm()->Destroy(pt->Pmp());
			GPOS_DELETE(pt);
		}
	}
	return ullTotalFreed;
}

//---------------------------------------------------------------------------
//	@function:
//		CCache::EvictEntries
//
//	@doc:
//		Evicts entries from cache to enforce cache quota. Only one evictor can evict at
//		a given time
//
//---------------------------------------------------------------------------
void
CCache::EvictEntries()
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

//---------------------------------------------------------------------------
//	@function:
//		CCache::PceNext
//
//	@doc:
//		Returns the value of the next unmarked entry in the hash chain
//		with the same key as the given entry.
//
//---------------------------------------------------------------------------
CCache::CCacheEntry *
CCache::PceNext
	(
	CCache::CCacheEntry *pce
	)
{
	GPOS_ASSERT(NULL != pce);

	CCache::CCacheEntry *pceCurrent = pce;
	VOID_PTR pvKey = pceCurrent->PvKey();
	CCacheHashtableAccessor shtacc(m_sht, pvKey);

	// move forward until we find unmarked entry with the same key
	CCache::CCacheEntry *pceNext = shtacc.PtNext(pceCurrent);
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


//---------------------------------------------------------------------------
//	@function:
//		CCache::UllTotalAllocatedSize
//
//	@doc:
//		Return total allocated size in bytes
//
//---------------------------------------------------------------------------
ULLONG
CCache::UllTotalAllocatedSize()
{
	return m_ullCacheSize;
}


//---------------------------------------------------------------------------
//	@function:
//		CCache::UllCacheQuota
//
//	@doc:
//		Return memory quota of the cache in bytes
//
//---------------------------------------------------------------------------
ULLONG
CCache::UllCacheQuota()
{
	return m_ullCacheQuota;
}


//---------------------------------------------------------------------------
//	@function:
//		CCache::SetCacheQuota
//
//	@doc:
//		Sets the cache quota
//
//---------------------------------------------------------------------------
void
CCache::SetCacheQuota(ULLONG ullNewQuota)
{
	m_ullCacheQuota = ullNewQuota;

	if (0 != m_ullCacheQuota && m_ullCacheSize > m_ullCacheQuota)
	{
		EvictEntries();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCache::UllEvictionCounter
//
//	@doc:
//		Return number of times this cache underwent eviction
//
//---------------------------------------------------------------------------
ULLONG
CCache::UllEvictionCounter()
{
	return m_ullEvictionCounter;
}

//---------------------------------------------------------------------------
//	@function:
//		CCache::FGetEvictionFactor
//
//	@doc:
//		Return percentage of cache (in terms of current cache size) to empty during eviction
//
//---------------------------------------------------------------------------
float
CCache::FGetEvictionFactor()
{
	return m_fEvictionFactor;
}

// EOF
