//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CHashMapIterTest.cpp
//
//	@doc:
//		Test for CHashMapIter
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CHashMapIter.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CHashMapIterTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CHashMapIterTest::EresUnittest
//
//	@doc:
//		Unittest for basic hash map iterator
//
//---------------------------------------------------------------------------
GPOS_RESULT
CHashMapIterTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CHashMapIterTest::EresUnittest_Basic),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CHashMapIterTest::EresUnittest_Basic
//
//	@doc:
//		Basic iterator test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CHashMapIterTest::EresUnittest_Basic()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// test data
	ULONG_PTR rgul[] = {1,2,3,4,5,6,7,8,9};
	const ULONG ulCnt = GPOS_ARRAY_SIZE(rgul);
	
	typedef CHashMap<ULONG_PTR, ULONG_PTR, 
		UlHashPtr<ULONG_PTR>, gpos::FEqual<ULONG_PTR>,
		CleanupNULL<ULONG_PTR>, CleanupNULL<ULONG_PTR> > Map;

	typedef CHashMapIter<ULONG_PTR, ULONG_PTR, 
		UlHashPtr<ULONG_PTR>, gpos::FEqual<ULONG_PTR>,
		CleanupNULL<ULONG_PTR>, CleanupNULL<ULONG_PTR> > MapIter;


	// using N - 2 slots guarantees collisions
	Map *pm = GPOS_NEW(pmp) Map(pmp, ulCnt - 2);

#ifdef GPOS_DEBUG

	// iteration over empty map
	MapIter miEmpty(pm);
	GPOS_ASSERT(!miEmpty.FAdvance());
	
#endif // GPOS_DEBUG
	
	// load map and iterate over it after each step
	for (ULONG ul = 0; ul < ulCnt; ++ul)
	{
		(void) pm->FInsert(&rgul[ul], &rgul[ul]);
	
		// checksum over keys
		ULONG_PTR ulpChkSumKey = 0;

		// checksum over values
		ULONG_PTR ulpChkSumValue = 0;
			
		// iterate over full map
		MapIter mi(pm);
		while (mi.FAdvance())
		{
			ulpChkSumKey += *(mi.Pk());
			ulpChkSumValue += *(mi.Pt());
		}
		
		// use Gauss's formula for checksum-ing
		GPOS_ASSERT(ulpChkSumKey == ulpChkSumValue);
		GPOS_ASSERT(ulpChkSumKey == ((ul + 2) * (ul + 1)) / 2);
	}
	
	pm->Release();

	return GPOS_OK;
}


// EOF

