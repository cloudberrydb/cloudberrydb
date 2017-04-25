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
#include "gpos/common/CAutoRef.h"
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
	ULONG rgul[] = {1,2,3,4,5,6,7,8,9};
	const ULONG ulCnt = GPOS_ARRAY_SIZE(rgul);
	
	typedef CHashMap<ULONG, ULONG, 
		UlHashPtr<ULONG>, gpos::FEqual<ULONG>,
		CleanupNULL<ULONG>, CleanupNULL<ULONG> > Map;

	typedef CHashMapIter<ULONG, ULONG, 
		UlHashPtr<ULONG>, gpos::FEqual<ULONG>,
		CleanupNULL<ULONG>, CleanupNULL<ULONG> > MapIter;


	// using N - 2 slots guarantees collisions
	Map *pm = GPOS_NEW(pmp) Map(pmp, ulCnt - 2);

#ifdef GPOS_DEBUG

	// iteration over empty map
	MapIter miEmpty(pm);
	GPOS_ASSERT(!miEmpty.FAdvance());
	
#endif // GPOS_DEBUG

	typedef CDynamicPtrArray<const ULONG, CleanupNULL> DrgPul;
	CAutoRef<DrgPul> pdrgpulKeys(GPOS_NEW(pmp) DrgPul(pmp)), pdrgpulValues(GPOS_NEW(pmp) DrgPul(pmp));
	// load map and iterate over it after each step
	for (ULONG ul = 0; ul < ulCnt; ++ul)
	{
		(void) pm->FInsert(&rgul[ul], &rgul[ul]);
		pdrgpulKeys->Append(&rgul[ul]);
		pdrgpulValues->Append(&rgul[ul]);

		CAutoRef<DrgPul> pdrgpulIterKeys(GPOS_NEW(pmp) DrgPul(pmp)), pdrgpulIterValues(GPOS_NEW(pmp) DrgPul(pmp));

		// iterate over full map
		MapIter mi(pm);
		while (mi.FAdvance())
		{
			pdrgpulIterKeys->Append(mi.Pk());
			pdrgpulIterValues->Append(mi.Pt());
		}

		pdrgpulIterKeys->Sort();
		pdrgpulIterValues->Sort();

		GPOS_ASSERT(pdrgpulKeys->FEqual(pdrgpulIterKeys.Pt()));
		GPOS_ASSERT(pdrgpulValues->FEqual(pdrgpulIterValues.Pt()));
	}
	
	pm->Release();

	return GPOS_OK;
}


// EOF

