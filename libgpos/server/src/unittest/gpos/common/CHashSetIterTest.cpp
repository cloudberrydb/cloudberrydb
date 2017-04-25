//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc

#include "gpos/base.h"
#include "gpos/common/CAutoRef.h"
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
	ULONG rgul[] = {1,2,3,4,5,6,7,8,9};
	const ULONG ulCnt = GPOS_ARRAY_SIZE(rgul);

	typedef CHashSet<ULONG, UlHash<ULONG>, gpos::FEqual<ULONG>, CleanupNULL<ULONG> > Set;

	typedef CHashSetIter<ULONG, UlHash<ULONG>, gpos::FEqual<ULONG>, CleanupNULL<ULONG> > SetIter;

	// using N - 2 slots guarantees collisions
	Set *ps = GPOS_NEW(pmp) Set(pmp, ulCnt - 2);

#ifdef GPOS_DEBUG

	// iteration over empty map
	SetIter siEmpty(ps);
	GPOS_ASSERT(!siEmpty.FAdvance());

#endif // GPOS_DEBUG

	typedef CDynamicPtrArray<const ULONG, CleanupNULL> DrgPul;
	CAutoRef<DrgPul> pdrgpulValues(GPOS_NEW(pmp) DrgPul(pmp));
	// load map and iterate over it after each step
	for (ULONG ul = 0; ul < ulCnt; ++ul)
	{
		(void) ps->FInsert(&rgul[ul]);
		pdrgpulValues->Append(&rgul[ul]);

		CAutoRef<DrgPul> pdrgpulIterValues(GPOS_NEW(pmp) DrgPul(pmp));

		// iterate over full set
		SetIter si(ps);
		while (si.FAdvance())
		{
			pdrgpulIterValues->Append(si.Pt());
		}

		pdrgpulIterValues->Sort();

		GPOS_ASSERT(pdrgpulValues->FEqual(pdrgpulIterValues.Pt()));
	}

	ps->Release();

	return GPOS_OK;
}


// EOF

