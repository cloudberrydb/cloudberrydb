//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc

#include "gpos/base.h"
#include "gpos/common/CHashSetIter.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CHashSetIterTest.h"

using namespace gpos;

// Unittest for basic hash set iterator
GPOS_RESULT
CHashSetIterTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CHashSetIterTest::EresUnittest_Basic),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


// Basic iterator test
GPOS_RESULT
CHashSetIterTest::EresUnittest_Basic()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// test data
	ULONG_PTR rgul[] = {1,2,3,4,5,6,7,8,9};
	const ULONG ulCnt = GPOS_ARRAY_SIZE(rgul);
	
	typedef CHashSet<ULONG_PTR, UlHashPtr<ULONG_PTR>, gpos::FEqual<ULONG_PTR>, CleanupNULL<ULONG_PTR> > Set;

	typedef CHashSetIter<ULONG_PTR, UlHashPtr<ULONG_PTR>, gpos::FEqual<ULONG_PTR>, CleanupNULL<ULONG_PTR> > SetIter;


	// using N - 2 slots guarantees collisions
	Set *ps = GPOS_NEW(pmp) Set(pmp, ulCnt - 2);

#ifdef GPOS_DEBUG

	// iteration over empty set
	SetIter siEmpty(ps);
	GPOS_ASSERT(!siEmpty.FAdvance());
	
#endif // GPOS_DEBUG
	
	// load set and iterate over it after each step
	for (ULONG ul = 0; ul < ulCnt; ++ul)
	{
		(void) ps->FInsert(&rgul[ul]);
	
		// checksum over elements
		ULONG_PTR ulpChkSumElement = 0;
		
		// iterate over full set
		SetIter si(ps);
		while (si.FAdvance())
		{
			ulpChkSumElement += *(si.Pt());
		}
		
		// use Gauss's formula for checksum-ing
		GPOS_ASSERT(ulpChkSumElement == ((ul + 2) * (ul + 1)) / 2);
	}
	
	ps->Release();

	return GPOS_OK;
}


// EOF

