//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CHashSetTest.cpp
//
//	@doc:
//		Test for CHashSet
//
//	@owner:
//		solimm1
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CHashSet.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CHashSetTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CHashSetTest::EresUnittest
//
//	@doc:
//		Unittest for basic hash set
//
//---------------------------------------------------------------------------
GPOS_RESULT
CHashSetTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CHashSetTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CHashSetTest::EresUnittest_Ownership),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CHashSetTest::EresUnittest_Basic
//
//	@doc:
//		Basic insertion/lookup test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CHashSetTest::EresUnittest_Basic()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// test with ULONG array
	ULONG_PTR rgul[] = {0,1,2,3,4,5,6,7,8,9};

	const ULONG ulCnt = GPOS_ARRAY_SIZE(rgul);

	typedef CHashSet< ULONG_PTR, UlHashPtr<ULONG_PTR>, FEqual<ULONG_PTR>, CleanupNULL<ULONG_PTR> > HSUl;

	HSUl *phs = GPOS_NEW(pmp) HSUl(pmp, 128);
	for (ULONG ul = 0; ul < ulCnt; ul++)
	{
#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif // GPOS_DEBUG
			phs->FInsert(&rgul[ul]);
		GPOS_ASSERT(fSuccess);
	}
	GPOS_ASSERT(ulCnt == phs->UlEntries());

	for (ULONG ul = 0; ul < ulCnt; ul++)
	{
		GPOS_ASSERT(phs->FExists(&rgul[ul]));
	}

	phs->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CHashSetTest::EresUnittest_Ownership
//
//	@doc:
//		Basic hash set test with ownership
//
//---------------------------------------------------------------------------
GPOS_RESULT
CHashSetTest::EresUnittest_Ownership()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG ulCnt = 256;

	typedef CHashSet< ULONG_PTR, UlHashPtr<ULONG_PTR>, FEqual<ULONG_PTR>, CleanupDelete<ULONG_PTR> > HSUl;

	HSUl *phs = GPOS_NEW(pmp) HSUl(pmp, 32);
	for (ULONG ul = 0; ul < ulCnt; ul++)
	{
		ULONG_PTR *pulp = GPOS_NEW(pmp) ULONG_PTR(ul);

#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif // GPOS_DEBUG
			phs->FInsert(pulp);

		GPOS_ASSERT(fSuccess);
		GPOS_ASSERT(phs->FExists(pulp));

		// can't insert existing keys
		GPOS_ASSERT(!phs->FInsert(pulp));
	}

	phs->Release();

	return GPOS_OK;
}

// EOF

