//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CHashMapTest.cpp
//
//	@doc:
//		Test for CHashMap
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CHashMapTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CHashMapTest::EresUnittest
//
//	@doc:
//		Unittest for basic hash map
//
//---------------------------------------------------------------------------
GPOS_RESULT
CHashMapTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CHashMapTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CHashMapTest::EresUnittest_Ownership),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CHashMapTest::EresUnittest_Basic
//
//	@doc:
//		Basic insertion/lookup for hash table
//
//---------------------------------------------------------------------------
GPOS_RESULT
CHashMapTest::EresUnittest_Basic()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// test with CHAR array
	ULONG_PTR rgul[] = {1,2,3,4,5,6,7,8,9};
	CHAR rgsz[][5] = {"abc", "def", "ghi", "qwe", "wer", "wert", "dfg", "xcv", "zxc"};
	
	GPOS_ASSERT(GPOS_ARRAY_SIZE(rgul) == GPOS_ARRAY_SIZE(rgsz));
	const ULONG ulCnt = GPOS_ARRAY_SIZE(rgul);
	
	typedef CHashMap<ULONG_PTR, CHAR, UlHashPtr<ULONG_PTR>, gpos::FEqual<ULONG_PTR>,
		CleanupNULL<ULONG_PTR>, CleanupNULL<CHAR> > HMUlChar;

	HMUlChar *phm = GPOS_NEW(pmp) HMUlChar(pmp, 128);
	for (ULONG i = 0; i < ulCnt; ++i)
	{
#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif // GPOS_DEBUG
			phm->FInsert(&rgul[i], (CHAR*)rgsz[i]);
		GPOS_ASSERT(fSuccess);
		
		for (ULONG j = 0; j <= i; ++j)
		{
			GPOS_ASSERT(rgsz[j] == phm->PtLookup(&rgul[j]));
		}
	}
	GPOS_ASSERT(ulCnt == phm->UlEntries());
	
	// test replacing entry values of existing keys
	CHAR rgszNew[][10] = {"abc_", "def_", "ghi_", "qwe_", "wer_", "wert_", "dfg_", "xcv_", "zxc_"};
	for (ULONG i = 0; i < ulCnt; ++i)
	{
#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif // GPOS_DEBUG
			phm->FReplace(&rgul[i], rgszNew[i]);
		GPOS_ASSERT(fSuccess);

#ifdef GPOS_DEBUG
		fSuccess =
#endif // GPOS_DEBUG
			phm->FReplace(&rgul[i], rgsz[i]);
		GPOS_ASSERT(fSuccess);
	}
	GPOS_ASSERT(ulCnt == phm->UlEntries());

	// test replacing entry value of a non-existing key
	ULONG_PTR ulp = 0;
#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif // GPOS_DEBUG
		phm->FReplace(&ulp, rgsz[0]);
	GPOS_ASSERT(!fSuccess);

	phm->Release();

	// test replacing values and triggering their release
	typedef CHashMap<ULONG, ULONG, UlHash<ULONG>, gpos::FEqual<ULONG>,
			CleanupDelete<ULONG>, CleanupDelete<ULONG> > HMUlUl;
	HMUlUl *phm2 = GPOS_NEW(pmp) HMUlUl(pmp, 128);

	ULONG *pulKey = GPOS_NEW(pmp) ULONG(1);
	ULONG *pulVal1 = GPOS_NEW(pmp) ULONG(2);
	ULONG *pulVal2 = GPOS_NEW(pmp) ULONG(3);

#ifdef GPOS_DEBUG
	fSuccess =
#endif // GPOS_DEBUG
		phm2->FInsert(pulKey, pulVal1);
	GPOS_ASSERT(fSuccess);

#ifdef GPOS_DEBUG
	ULONG *pulVal = phm2->PtLookup(pulKey);
	GPOS_ASSERT(*pulVal == 2);

	fSuccess =
#endif // GPOS_DEBUG
		phm2->FReplace(pulKey, pulVal2);
	GPOS_ASSERT(fSuccess);

#ifdef GPOS_DEBUG
	pulVal = phm2->PtLookup(pulKey);
	GPOS_ASSERT(*pulVal == 3);
#endif // GPOS_DEBUG

	phm2->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CHashMapTest::EresUnittest_Ownership
//
//	@doc:
//		Basic hash map test with ownership
//
//---------------------------------------------------------------------------
GPOS_RESULT
CHashMapTest::EresUnittest_Ownership()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG ulCnt = 256;

	typedef CHashMap<ULONG_PTR, CHAR, UlHashPtr<ULONG_PTR>, gpos::FEqual<ULONG_PTR>,
		CleanupDelete<ULONG_PTR>, CleanupDeleteRg<CHAR> > HMUlChar;
	
	HMUlChar *phm = GPOS_NEW(pmp) HMUlChar(pmp, 32);
	for (ULONG i = 0; i < ulCnt; ++i)
	{
		ULONG_PTR *pulp = GPOS_NEW(pmp) ULONG_PTR(i);
		CHAR *sz = GPOS_NEW_ARRAY(pmp, CHAR, 3);
	
#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif // GPOS_DEBUG
			phm->FInsert(pulp, sz);

		GPOS_ASSERT(fSuccess);
		GPOS_ASSERT(sz == phm->PtLookup(pulp));
		
		// can't insert existing keys
		GPOS_ASSERT(!phm->FInsert(pulp, sz));
	}

	phm->Release();
	
	return GPOS_OK;
}

// EOF

