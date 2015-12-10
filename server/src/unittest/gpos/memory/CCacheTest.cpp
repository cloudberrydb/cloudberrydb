//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC CORP.
//
//	@filename:
//		CCacheTest.cpp
//
//	@doc:
//		Tests for CCache
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/error/CAutoTrace.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CRandom.h"

#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCacheFactory.h"

#include "gpos/task/CAutoTaskProxy.h"

#include "gpos/test/CUnittest.h"

#include "unittest/gpos/memory/CCacheTest.h"


using namespace gpos;

#define GPOS_CACHE_THREADS	10
#define GPOS_CACHE_ELEMENTS	20
#define GPOS_CACHE_DUPLICATES	5
#define GPOS_CACHE_DUPLICATES_TO_DELETE		3

// static variable
static BOOL fUnique = true;

//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresUnittest
//
//	@doc:
//		Driver of cache unittests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CCacheTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CCacheTest::EresUnittest_Eviction),
		GPOS_UNITTEST_FUNC(CCacheTest::EresUnittest_Iteration),
		GPOS_UNITTEST_FUNC(CCacheTest::EresUnittest_DeepObject),
		GPOS_UNITTEST_FUNC(CCacheTest::EresUnittest_IterativeDeletion),
		GPOS_UNITTEST_FUNC(CCacheTest::EresUnittest_ConcurrentAccess)
		};

	fUnique = true;
	GPOS_RESULT eres =  CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

	if (GPOS_OK == eres)
	{
		fUnique = false;
		eres =  CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
	}

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::SSimpleObject::FMyEqual
//
//	@doc:
//		Key equality function
//
//---------------------------------------------------------------------------
BOOL
CCacheTest::SSimpleObject::FMyEqual
	(
	const VOID_PTR &pvKey,
	const VOID_PTR &pvKeySecond
	)
{
	BOOL fReturn = false;
	if (NULL == pvKey && NULL == pvKeySecond)
	{
		fReturn = true;
	}
	else if (NULL == pvKey || NULL == pvKeySecond)
	{
		fReturn = false;
	}
	else
	{
		ULONG *pul = static_cast<ULONG *> (pvKey);
		ULONG *pulSecond = static_cast<ULONG *> (pvKeySecond);
		fReturn = (*pul) == (*pulSecond);
	}

	return fReturn;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::CDeepObject::UlMyHash
//
//	@doc:
//		The hash code of linked list is the summation of entries' keys
//
//---------------------------------------------------------------------------
ULONG
CCacheTest::CDeepObject::UlMyHash
	(
	const VOID_PTR &pvKey
	)
{
	CList<SDeepObjectEntry> *plist = static_cast<CList<SDeepObjectEntry> *> (pvKey);
	ULONG ulKey = 0;
	SDeepObjectEntry *pdoe = plist->PtFirst();
	while (pdoe != NULL)
	{
		ulKey += pdoe->m_ulKey;
		pdoe = plist->PtNext(pdoe);
	}

	return ulKey;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::CDeepObject::FMyEqual
//
//	@doc:
//		Two objects are equal if the keys in their corresponding list
//		entries are equal
//
//---------------------------------------------------------------------------
BOOL
CCacheTest::CDeepObject::FMyEqual
	(
	const VOID_PTR &pvKey,
	const VOID_PTR &pvKeySecond
	)
{
	BOOL fReturn = false;
	if (NULL == pvKey && NULL == pvKeySecond)
	{
		fReturn = true;
	}
	else if (NULL == pvKey || NULL == pvKeySecond)
	{
		fReturn = false;
	}
	else
	{
		CList<SDeepObjectEntry> *plist =
				static_cast<CList<SDeepObjectEntry> *> (pvKey);
		CList<SDeepObjectEntry> *plistSecond =
				static_cast<CList<SDeepObjectEntry> *> (pvKeySecond);
		if (plist->UlSize() != plistSecond->UlSize())
		{
			fReturn = false;
		}
		else
		{
			fReturn = true;
			SDeepObjectEntry *pdoe = plist->PtFirst();
			SDeepObjectEntry *pdoeSecond = plistSecond->PtFirst();
			while (NULL != pdoe)
			{
				GPOS_ASSERT(NULL != pdoeSecond &&
				    		"Reached a NULL entry in the second list");

				if (pdoe->m_ulKey != pdoeSecond->m_ulKey)
				{
					fReturn = false;
					break;
				}
				pdoe = plist->PtNext(pdoe);
				pdoeSecond = plistSecond->PtNext(pdoeSecond);
			}
		}
	}

	return fReturn;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::CDeepObject::AddEntry
//
//	@doc:
//		Adds a new entry to the linked list
//
//---------------------------------------------------------------------------
void
CCacheTest::CDeepObject::AddEntry
	(
	IMemoryPool *pmp,
	ULONG ulKey,
	ULONG ulVal
	)
{
	m_list.Prepend(GPOS_NEW(pmp) SDeepObjectEntry (ulKey, ulVal));
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresUnittest_Basic
//
//	@doc:
//		Basic cache test (insert, lookup, delete, lookup again)
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresUnittest_Basic()
{
	CAutoP<CCache> apcache;
	apcache = CCacheFactory::PCacheCreate
				(
				fUnique,
				UNLIMITED_CACHE_QUOTA,
				SSimpleObject::UlMyHash,
				SSimpleObject::FMyEqual
				);

	CCache *pcache = apcache.Pt();

	//insertion - scope for accessor
	{
		CSimpleObjectCacheAccessor ca(pcache);

		SSimpleObject *pso = GPOS_NEW(ca.Pmp()) SSimpleObject(1, 2);

#ifdef GPOS_DEBUG
		SSimpleObject *psoReturned =
#endif // GPOS_DEBUG
			ca.PtInsert(&(pso->m_ulKey), pso);

		GPOS_ASSERT(psoReturned == pso &&
				    "Incorrect cache entry was inserted");
		GPOS_ASSERT(1 == pcache->UlpEntries());

		// insert duplicate while not allowed
		if (pcache->FUnique())
		{
			CSimpleObjectCacheAccessor ca(pcache);
			SSimpleObject *psoDuplicate = GPOS_NEW(ca.Pmp()) SSimpleObject(1, 5);

#ifdef GPOS_DEBUG
			SSimpleObject *psoReturned =
#endif // GPOS_DEBUG
				ca.PtInsert(&(psoDuplicate->m_ulKey), psoDuplicate);

			GPOS_ASSERT(psoReturned == pso &&
						"Duplicate insertion must fail");
			GPOS_ASSERT(1 == pcache->UlpEntries());

			// delete original cache object
			ca.MarkForDeletion();
		}

		// lookup - scope for accessor
		{
			CSimpleObjectCacheAccessor ca(pcache);
			ULONG ulkey = 1;
			ca.Lookup(&ulkey);
			pso = ca.PtVal();

			GPOS_ASSERT_IMP(!pcache->FUnique(), NULL != pso && 2 == pso->m_ulValue);
			GPOS_ASSERT_IMP(pcache->FUnique(), NULL == pso);
		}

		// delete - scope for accessor
		{
			CSimpleObjectCacheAccessor ca(pcache);
			ULONG ulkey = 1;
			ca.Lookup(&ulkey);
			pso = ca.PtVal();

			GPOS_ASSERT_IMP(!pcache->FUnique(), NULL != pso);

			if (NULL != pso)
			{
				ca.MarkForDeletion();
			}
		}

		// lookup again - scope for accessor
		{
			CSimpleObjectCacheAccessor ca(pcache);
			ULONG ulkey = 1;
			ca.Lookup(&ulkey);
			pso = ca.PtVal();

			GPOS_ASSERT(NULL == pso);

		}

		// at this point, we still maintain a valid cached object held by the
		// outmost accessor

		GPOS_ASSERT(NULL != psoReturned && 2 == psoReturned->m_ulValue);
	}

	GPOS_ASSERT(0 == pcache->UlpEntries());

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::InsertOneElement
//
//	@doc:
//		Inserts one SSimpleObject with both key and value set to ulKey.
//		Returns the size of a single instance of SSimpleObject
//
//---------------------------------------------------------------------------
ULLONG
CCacheTest::InsertOneElement(CCache *pCache, ULONG ulKey)
{
	{
		CSimpleObjectCacheAccessor ca(pCache);
		IMemoryPool *pmp = ca.Pmp();
		SSimpleObject *pso = GPOS_NEW(pmp) SSimpleObject(ulKey, ulKey);
		ca.PtInsert(&(pso->m_ulKey), pso);
		return pmp->UllTotalAllocatedSize();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::ULFillCacheWithoutEviction
//
//	@doc:
//		Inserts as many SSimpleObjects as needed (starting with the key ulKeyStart and
//		sequentially generating the successive keys) to consume cache quota.
//		Returns the key of the last inserted element
//---------------------------------------------------------------------------
ULONG
CCacheTest::ULFillCacheWithoutEviction(CCache *pCache, ULONG ulKeyStart)
{
#ifdef GPOS_DEBUG
	// initial size of the cache
	ULLONG ullInitialCacheSize = pCache->UllTotalAllocatedSize();
	ULLONG ullOldEvictionCounter = pCache->UllEvictionCounter();
#endif

	ULLONG ullOneElemSize = InsertOneElement(pCache, ulKeyStart);

#ifdef GPOS_DEBUG
	ULLONG ullOneElemCacheSize = pCache->UllTotalAllocatedSize();
	ULLONG ullNewEvictionCounter = pCache->UllEvictionCounter();
#endif

	GPOS_ASSERT((ullOneElemCacheSize > ullInitialCacheSize || ullOldEvictionCounter < ullNewEvictionCounter)
			&& "Cache size didn't change upon insertion");

	ULLONG ullCacheCapacity = pCache->UllCacheQuota() / ullOneElemSize;

	// We already have an element in the cache and the eviction happens after we violate.
	// So, we should not trigger eviction inserting cacheCapacity + 1
	for (ULONG ulElemCount = 1; ulElemCount <= ullCacheCapacity; ulElemCount++)
	{
		InsertOneElement(pCache, ulKeyStart + ulElemCount);
		GPOS_CHECK_ABORT;
	}

#ifdef GPOS_DEBUG
	ULLONG ullSizeBeforeEviction =
#endif // GPOS_DEBUG
			pCache->UllTotalAllocatedSize();

	// Check the size of the cache. Nothing should be evicted if the cache was initially empty
#ifdef GPOS_DEBUG
	ULLONG ullExpectedCacheSize = (ullCacheCapacity + 1) * ullOneElemSize;
#endif // GPOS_DEBUG

	GPOS_ASSERT_IMP(0 == ullInitialCacheSize, ullSizeBeforeEviction == ullExpectedCacheSize &&
			ullSizeBeforeEviction + ullOneElemSize > ullInitialCacheSize);

	return (ULONG) (ullCacheCapacity + ulKeyStart);
}

//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::CheckGenerationSanityAfterEviction
//
//	@doc:
//		Checks if after eviction we have more entries from newer generation than the older generation
//---------------------------------------------------------------------------
void
CCacheTest::CheckGenerationSanityAfterEviction(CCache* pCache, ULLONG
#ifdef GPOS_DEBUG
		ullOneElemSize
#endif
		, ULONG ulOldGenBeginKey,
		ULONG ulOldGenEndKey, ULONG ulNewGenEndKey)
{
	ULONG uloldGenEntryCount = 0;
	ULONG ulNewGenEntryCount = 0;

	for (ULONG ulKey = ulOldGenBeginKey; ulKey <= ulNewGenEndKey; ulKey++)
	{
		CSimpleObjectCacheAccessor ca(pCache);
		ca.Lookup(&ulKey);
		SSimpleObject* pso = ca.PtVal();
		if (NULL != pso)
		{
			if (ulKey <= ulOldGenEndKey)
			{
				uloldGenEntryCount++;
			}
			else
			{
				ulNewGenEntryCount++;
			}
		}
	}

#ifdef GPOS_DEBUG
	ULLONG ullCacheCapacity = pCache->UllCacheQuota() / ullOneElemSize;
#endif

	// total in-cache entries must be at least as many as the minimum number of in-cache entries after an eviction
	GPOS_ASSERT(uloldGenEntryCount + ulNewGenEntryCount >= (ULONG)((double)ullCacheCapacity * (1 - pCache->FGetEvictionFactor())));
	// there should be at least as many new gen entries as the old gen entries as they get to live longer
	GPOS_ASSERT(ulNewGenEntryCount >= uloldGenEntryCount);
}

//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::TestEvictionForOneCacheSize
//
//	@doc:
//		Tests if cache eviction works for a single cache size
//---------------------------------------------------------------------------
void
CCacheTest::TestEvictionForOneCacheSize(ULLONG ullCacheQuota)
{
	CAutoP<CCache> apCache;
	apCache = CCacheFactory::PCacheCreate(false, /* not an unique cache */
			ullCacheQuota, SSimpleObject::UlMyHash, SSimpleObject::FMyEqual);

	CCache* pCache = apCache.Pt();
	ULONG ulLastKeyFirstGen = ULFillCacheWithoutEviction(pCache, 0);

#ifdef GPOS_DEBUG
	ULLONG ullSizeBeforeEviction = pCache->UllTotalAllocatedSize();
#endif

	ULLONG ullOneElemSize = InsertOneElement(pCache, ulLastKeyFirstGen + 1);

#ifdef GPOS_DEBUG
	ULLONG ullPostEvictionSize = pCache->UllTotalAllocatedSize();
#endif

	// Make sure cache is now smaller, due to eviction
	GPOS_ASSERT(ullPostEvictionSize < ullSizeBeforeEviction);
	// Now insert another batch of elements to fill the cache
	ULONG ulLastKeySecondGen = ULFillCacheWithoutEviction(pCache, ulLastKeyFirstGen + 2);
	// Another batch of insert to the cache's filling should evict all the second generation keys
	ULONG ulLastKeyThirdGen = ULFillCacheWithoutEviction(pCache, ulLastKeySecondGen + 1);

	CSimpleObjectCacheAccessor caBeforeEviction(pCache);
	// this is now pinned as the accessor is not going out of scope; pinned entry is used later for checking non-eviction
	caBeforeEviction.Lookup(&ulLastKeyThirdGen);

	// Now verify everything from the first generation insertion is evicted
	for (ULONG ulKey = 0; ulKey <= ulLastKeyFirstGen; ulKey++)
	{
		CSimpleObjectCacheAccessor ca(pCache);
		ca.Lookup(&ulKey);

#ifdef GPOS_DEBUG
		SSimpleObject* pso =
#endif
		ca.PtVal();
		GPOS_ASSERT(NULL == pso);
	}


	// now ensure that newer gen items are outliving older gen during cache eviction
	CheckGenerationSanityAfterEviction(pCache, ullOneElemSize, ulLastKeyFirstGen + 2,
			ulLastKeySecondGen, ulLastKeyThirdGen);

	ULLONG ullNewQuota = static_cast<ULLONG>(static_cast<double>(ullCacheQuota) * 0.5);
	// drastically reduce the size of the cache
	pCache->SetCacheQuota(ullNewQuota);
	GPOS_ASSERT(pCache->UllCacheQuota() == ullNewQuota);
	// now ensure that newer gen items are outliving older gen during cache eviction
	CheckGenerationSanityAfterEviction(pCache, ullOneElemSize, ulLastKeyFirstGen + 2,
			ulLastKeySecondGen, ulLastKeyThirdGen);

	// now check pinning would retain the entry, no matter how many eviction is triggered

	// Another batch of insert to the cache's filling should evict all the third generation keys
	ULONG ulLastKeyFourthGen = ULFillCacheWithoutEviction(pCache, ulLastKeyThirdGen + 1);

	// Another batch of insert to the cache's filling should evict all the forth generation keys
	ULONG ulLastKeyFifthGen = ULFillCacheWithoutEviction(pCache, ulLastKeyFourthGen + 1);

	// Another batch of insert to the cache's filling should evict all the fifth generation keys
	ULFillCacheWithoutEviction(pCache, ulLastKeyFifthGen + 1);
	for (ULONG ulKey = ulLastKeySecondGen + 1; ulKey <= ulLastKeyFourthGen; ulKey++)
	{
		CSimpleObjectCacheAccessor ca(pCache);
		ca.Lookup(&ulKey);
#ifdef GPOS_DEBUG
		SSimpleObject* pso =
#endif
		ca.PtVal();

		// everything is evicted from third and fourth gen, except the pinned entry
		GPOS_ASSERT_IMP(ulKey != ulLastKeyThirdGen, NULL == pso);
		GPOS_ASSERT_IMP(ulKey == ulLastKeyThirdGen, NULL != pso);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresUnittest_Eviction
//
//	@doc:
//		Test if cache eviction works
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresUnittest_Eviction()
{
	TestEvictionForOneCacheSize(10240);
	TestEvictionForOneCacheSize(20480);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresInsertDuplicates
//
//	@doc:
//		Helper function: insert multiple elements with duplicate keys
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresInsertDuplicates
	(
	CCache *pcache
	)
{
	ULONG ulDuplicates = 1;
	if (!pcache->FUnique())
	{
		ulDuplicates = GPOS_CACHE_DUPLICATES;
	}

	for (ULONG i = 0; i < GPOS_CACHE_ELEMENTS; i++)
	{
		for (ULONG j = 0; j < ulDuplicates; j++)
		{
			CSimpleObjectCacheAccessor ca(pcache);
			SSimpleObject *pso = GPOS_NEW(ca.Pmp()) SSimpleObject(i, j);

#ifdef GPOS_DEBUG
			SSimpleObject *psoReturned =
#endif // GPOS_DEBUG
					ca.PtInsert(&(pso->m_ulKey), pso);

			GPOS_ASSERT(NULL != psoReturned);

		}
		GPOS_CHECK_ABORT;
	}

	{
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();
		CAutoTrace at(pmp);
		at.Os() << std::endl << "Total memory consumption by cache: " << pcache->UllTotalAllocatedSize() << " bytes";
		at.Os() << std::endl << "Total memory consumption by memory manager: " << CMemoryPoolManager::Pmpm()->UllTotalAllocatedSize() << " bytes";
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresRemoveDuplicates
//
//	@doc:
//		Helper function: remove multiple elements with the same key
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresRemoveDuplicates
	(
	CCache *pcache
	)
{
	for (ULONG i = 0; i < GPOS_CACHE_ELEMENTS; i++)
	{
		GPOS_CHECK_ABORT;

		CSimpleObjectCacheAccessor ca(pcache);
		ca.Lookup(&i);
		ULONG ulCount = 0;
		SSimpleObject* pso = ca.PtVal();
		GPOS_ASSERT(NULL != pso);


		while (NULL != pso)
		{
			GPOS_CHECK_ABORT;

			GPOS_ASSERT(pso->m_ulValue < GPOS_CACHE_DUPLICATES &&
					    "Incorrect entry was found");

			if (pso->m_ulValue < GPOS_CACHE_DUPLICATES_TO_DELETE)
			{
				ca.MarkForDeletion();
				ulCount++;
			}

			pso = ca.PtNext();
		}
		GPOS_ASSERT(ulCount == GPOS_CACHE_DUPLICATES_TO_DELETE &&
				    "Incorrect number of deleted entries");

	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresUnittest_DeepObject
//
//	@doc:
//		Cache test with deep objects
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresUnittest_DeepObject()
{
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);

	// construct a key
	CDeepObject *pdoDummy = GPOS_NEW(amp.Pmp()) CDeepObject();
	pdoDummy->AddEntry(amp.Pmp(), 1, 1);
	pdoDummy->AddEntry(amp.Pmp(), 2, 2);

	CAutoP<CCache> apcache;
	apcache = CCacheFactory::PCacheCreate
			(
			fUnique,
			UNLIMITED_CACHE_QUOTA,
			CDeepObject::UlMyHash,
			CDeepObject::FMyEqual
			);

	CCache *pcache = apcache.Pt();

	// insertion - scope for accessor
	{
		CDeepObjectCacheAccessor ca(pcache);
		IMemoryPool *pmp = ca.Pmp();
		CDeepObject *pdo = GPOS_NEW(pmp) CDeepObject();
		pdo->AddEntry(pmp, 1, 1);
		pdo->AddEntry(pmp, 2, 2);

#ifdef GPOS_DEBUG
		CDeepObject *pdoReturned =
#endif // GPOS_DEBUG
			ca.PtInsert(pdo->PKey(), pdo);

		GPOS_ASSERT(NULL != pdoReturned &&
				    "Incorrect cache entry was inserted");

		// insert duplicate while not allowed
		if (pcache->FUnique())
		{
			CDeepObjectCacheAccessor ca(pcache);
			IMemoryPool *pmp = ca.Pmp();
			CDeepObject *pdoDuplicate = GPOS_NEW(pmp) CDeepObject();
			pdoDuplicate->AddEntry(pmp, 1, 5);
			pdoDuplicate->AddEntry(pmp, 2, 5);

#ifdef GPOS_DEBUG
			CDeepObject *pdoReturned  =
#endif // GPOS_DEBUG
				ca.PtInsert(pdoDuplicate->PKey(), pdoDuplicate);

			GPOS_ASSERT(pdoReturned == pdo &&
						"Duplicate insertion must fail");

			// delete original cache object
			ca.MarkForDeletion();
		}


		// lookup - scope for accessor
		{
			CDeepObjectCacheAccessor ca(pcache);
			ca.Lookup(pdoDummy->PKey());
			pdo = ca.PtVal();

			GPOS_ASSERT_IMP(pcache->FUnique(), NULL == pdo);
			GPOS_ASSERT_IMP(!pcache->FUnique(), NULL != pdo);
			GPOS_ASSERT_IMP(!pcache->FUnique(),
							3 == CDeepObject::UlMyHash(pdo->PKey()) &&
							"Incorrect cache entry");

		}

		// delete - scope for accessor
		{
			CDeepObjectCacheAccessor ca(pcache);
			ca.Lookup(pdoDummy->PKey());
			pdo = ca.PtVal();

			GPOS_ASSERT_IMP(!pcache->FUnique(), NULL != pdo);

			if (NULL != pdo)
			{
				ca.MarkForDeletion();
			}
		}

		// lookup again - scope for accessor
		{
			CDeepObjectCacheAccessor ca(pcache);
			ca.Lookup(pdoDummy->PKey());
			pdo = ca.PtVal();
			GPOS_ASSERT(NULL == pdo);

		}

		// at this point, we still maintain a valid cached object held by the
		// outmost accessor

		GPOS_ASSERT(NULL != pdoReturned &&
					3 == CDeepObject::UlMyHash(pdoReturned->PKey()));
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresUnittest_Iteration
//
//	@doc:
//		Cache iteration test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresUnittest_Iteration()
{
	CAutoP<CCache> apcache;
	apcache = CCacheFactory::PCacheCreate
				(
				fUnique,
				UNLIMITED_CACHE_QUOTA,
				SSimpleObject::UlMyHash,
				SSimpleObject::FMyEqual
				);

	CCache *pcache = apcache.Pt();

	CCacheTest::EresInsertDuplicates(pcache);

#ifdef GPOS_DEBUG
	ULONG ulDuplicates = 1;
	if (!pcache->FUnique())
	{
		ulDuplicates = GPOS_CACHE_DUPLICATES;
	}
#endif // GPOS_DEBUG

	for (ULONG i = 0; i < GPOS_CACHE_ELEMENTS; i++)
	{
		GPOS_CHECK_ABORT;

		CSimpleObjectCacheAccessor ca(pcache);
		ca.Lookup(&i);
		ULONG ulCount = 0;
		SSimpleObject* pso = ca.PtVal();
		GPOS_ASSERT(NULL != pso);

		while (NULL != pso)
		{
			GPOS_CHECK_ABORT;

			GPOS_ASSERT(ulDuplicates > pso->m_ulValue &&
					    "Incorrect entry was found");

			ulCount++;
			pso = ca.PtNext();
		}
		GPOS_ASSERT(ulCount == ulDuplicates &&
				    "Incorrect number of duplicates");

	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresUnittest_IterativeDeletion
//
//	@doc:
//		Cache iterative deletion test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresUnittest_IterativeDeletion()
{
	GPOS_ASSERT(GPOS_CACHE_DUPLICATES >= GPOS_CACHE_DUPLICATES_TO_DELETE);

	CAutoP<CCache> apcache;
	apcache = CCacheFactory::PCacheCreate
				(
				fUnique,
				UNLIMITED_CACHE_QUOTA,
				SSimpleObject::UlMyHash,
				SSimpleObject::FMyEqual
				);

	CCache *pcache = apcache.Pt();

	CCacheTest::EresInsertDuplicates(pcache);

	if (!pcache->FUnique())
	{
		CCacheTest::EresRemoveDuplicates(pcache);
	}

#ifdef GPOS_DEBUG
	ULONG ulDuplicates = 1;
	ULONG ulDuplicatesToDelete = 0;
	if (!pcache->FUnique())
	{
		ulDuplicates = GPOS_CACHE_DUPLICATES;
		ulDuplicatesToDelete = GPOS_CACHE_DUPLICATES_TO_DELETE;
	}

	ULONG ulRemaining = ulDuplicates - ulDuplicatesToDelete;
#endif // GPOS_DEBUG

	// count remaining duplicate entries
	for (ULONG i = 0; i < GPOS_CACHE_ELEMENTS; i++)
	{
		GPOS_CHECK_ABORT;

		CSimpleObjectCacheAccessor ca(pcache);
		ca.Lookup(&i);
		ULONG ulCount = 0;
		SSimpleObject *pso = ca.PtVal();
		GPOS_ASSERT_IMP(0 < ulRemaining, NULL != pso);

		while (NULL != pso)
		{
			GPOS_CHECK_ABORT;

			GPOS_ASSERT(pso->m_ulValue >= ulDuplicatesToDelete &&
					    "Incorrect entry value was found");
			ulCount++;
			pso = ca.PtNext();
		}

		GPOS_ASSERT(ulCount == ulRemaining &&
				    "Incorrect number of remaining duplicates");

	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::PvInsertTask
//
//	@doc:
//		A task that inserts entries in the cache
//
//---------------------------------------------------------------------------
void *
CCacheTest::PvInsertTask
	(
	 void * pv
	)
{
	GPOS_CHECK_ABORT;

	CCache *pcache = (CCache *) pv;
	CCacheTest::EresInsertDuplicates(pcache);

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::PvLookupTask
//
//	@doc:
//		A task that iterates over entries in the cache
//
//---------------------------------------------------------------------------
void *
CCacheTest::PvLookupTask
	(
	 void * pv
	)
{
	CCache *pcache = (CCache *) pv;
	CRandom rand;
	for (ULONG i = 0; i<10; i++)
	{
		GPOS_CHECK_ABORT;

		CSimpleObjectCacheAccessor ca(pcache);
		ULONG ulkey =  rand.ULNext() % (10);
		ca.Lookup(&ulkey);
		SSimpleObject *pso = ca.PtVal();

		while (NULL != pso)
		{
			GPOS_CHECK_ABORT;

			pso = ca.PtNext();
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::PvDeleteTask
//
//	@doc:
//		A task that iterates over entries in the cache and removes them
//
//---------------------------------------------------------------------------
void *
CCacheTest::PvDeleteTask
	(
	 void * pv
	)
{
	CCache *pcache = (CCache *) pv;
	CRandom rand;
	for (ULONG i = 0; i< 10; i++)
	{
		GPOS_CHECK_ABORT;

		CSimpleObjectCacheAccessor ca(pcache);
		ULONG ulkey =  rand.ULNext() % (10);
		ca.Lookup(&ulkey);

		SSimpleObject *pso = ca.PtVal();

		while (NULL != pso)
		{
			ca.MarkForDeletion();
			pso = ca.PtNext();

			GPOS_CHECK_ABORT;
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheTest::EresUnittest_ConcurrentAccess
//
//	@doc:
//		A test for multiple tasks accessing the same cache.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheTest::EresUnittest_ConcurrentAccess()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for cache auto pointer
	{
		GPOS_CHECK_ABORT;

		CAutoP<CCache> apcache;
		apcache = CCacheFactory::PCacheCreate
					(
					fUnique,
					UNLIMITED_CACHE_QUOTA,
					SSimpleObject::UlMyHash,
					SSimpleObject::FMyEqual
					);

		CCache *pcache = apcache.Pt();

		// scope for ATP
		{
			GPOS_CHECK_ABORT;

			CAutoTaskProxy atp(pmp, pwpm);

			CTask *rgPtsk[GPOS_CACHE_THREADS];

			CCacheTest::TaskFuncPtr rgPfuncTask[] =
				{
				CCacheTest::PvInsertTask,
				CCacheTest::PvLookupTask,
				CCacheTest::PvDeleteTask
				};

			const ULONG ulNumberOfTaskTypes = GPOS_ARRAY_SIZE(rgPfuncTask);

			// create tasks
			for (ULONG i = 0; i < GPOS_CACHE_THREADS; i++)
			{
				rgPtsk[i] = atp.PtskCreate
							(
							rgPfuncTask[i % ulNumberOfTaskTypes],
							pcache
							);

				GPOS_CHECK_ABORT;

				atp.Schedule(rgPtsk[i]);
			}
			GPOS_CHECK_ABORT;

			// wait for completion
			for (ULONG i = 0; i < GPOS_CACHE_THREADS; i++)
			{
				atp.Wait(rgPtsk[i]);
				GPOS_CHECK_ABORT;

			}
		}
		GPOS_CHECK_ABORT;

	}
	GPOS_CHECK_ABORT;

	return GPOS_OK;
}

// EOF
