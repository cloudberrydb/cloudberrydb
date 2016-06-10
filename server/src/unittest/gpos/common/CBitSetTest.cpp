//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CBitSetTest.cpp
//
//	@doc:
//      Test for CBitSet
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpos/common/CBitSet.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CBitSetTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CBitSet::EresUnittest
//
//	@doc:
//		Unittest for bit vectors
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitSetTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CBitSetTest::EresUnittest_Basics),
		GPOS_UNITTEST_FUNC(CBitSetTest::EresUnittest_Removal),
		GPOS_UNITTEST_FUNC(CBitSetTest::EresUnittest_SetOps),
		GPOS_UNITTEST_FUNC(CBitSetTest::EresUnittest_Performance)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CBitSetTest::EresUnittest_Basics
//
//	@doc:
//		Testing ctors/dtor
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitSetTest::EresUnittest_Basics()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG cSizeBits = 32;
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, cSizeBits);

	ULONG cInserts = 10;
	for (ULONG i = 0; i < cInserts; i += 2)
	{
		// forces addition of new link
		pbs->FExchangeSet(i * cSizeBits);
	}
	GPOS_ASSERT(cInserts / 2 == pbs->CElements());

	for (ULONG i = 1; i < cInserts; i += 2)
	{
		// new link between existing links
		pbs->FExchangeSet(i * cSizeBits);
	}
	GPOS_ASSERT(cInserts == pbs->CElements());

	CBitSet *pbsCopy = GPOS_NEW(pmp) CBitSet(pmp, *pbs);
	GPOS_ASSERT(pbsCopy->FEqual(pbs));

	// delete old bitset to make sure we're not accidentally
	// using any of its memory
	pbs->Release();

	for (ULONG i = 0; i < cInserts; i++)
	{
		GPOS_ASSERT(pbsCopy->FBit(i * cSizeBits));
	}

	CWStringDynamic str(pmp);
	COstreamString os(&str);
	
	os << *pbsCopy << std::endl;
	GPOS_TRACE(str.Wsz());
	
	pbsCopy->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSetTest::EresUnittest_Removal
//
//	@doc:
//		Cleanup test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitSetTest::EresUnittest_Removal()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG cSizeBits = 32;
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, cSizeBits);
	CBitSet *pbsEmpty = GPOS_NEW(pmp) CBitSet(pmp, cSizeBits);

	GPOS_ASSERT(pbs->FEqual(pbsEmpty));
	GPOS_ASSERT(pbsEmpty->FEqual(pbs));

	ULONG cInserts = 10;
	for (ULONG i = 0; i < cInserts; i++)
	{
		pbs->FExchangeSet(i * cSizeBits);

		GPOS_ASSERT(i + 1 == pbs->CElements());
	}

	for (ULONG i = 0; i < cInserts; i++)
	{
		// cleans up empty links
		pbs->FExchangeClear(i * cSizeBits);

		GPOS_ASSERT(cInserts - i - 1 == pbs->CElements());
	}

	GPOS_ASSERT(pbs->FEqual(pbsEmpty));
	GPOS_ASSERT(pbsEmpty->FEqual(pbs));

	pbs->Release();
	pbsEmpty->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSetTest::EresUnittest_SetOps
//
//	@doc:
//		Test for set operations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitSetTest::EresUnittest_SetOps()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG cSizeBits = 32;
	ULONG cInserts = 10;

	CBitSet *pbs1 = GPOS_NEW(pmp) CBitSet(pmp, cSizeBits);
	for (ULONG i = 0; i < cInserts; i += 2)
	{
		pbs1->FExchangeSet(i * cSizeBits);
	}

	CBitSet *pbs2 = GPOS_NEW(pmp) CBitSet(pmp, cSizeBits);
	for (ULONG i = 1; i < cInserts; i += 2)
	{
		pbs2->FExchangeSet(i * cSizeBits);
	}
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, cSizeBits);

	pbs->Union(pbs1);
	GPOS_ASSERT(pbs->FEqual(pbs1));

	pbs->Intersection(pbs1);
	GPOS_ASSERT(pbs->FEqual(pbs1));
	GPOS_ASSERT(pbs->FEqual(pbs));
	GPOS_ASSERT(pbs1->FEqual(pbs1));

	pbs->Union(pbs2);
	GPOS_ASSERT(!pbs->FEqual(pbs1) && !pbs->FEqual(pbs2));
	GPOS_ASSERT(pbs->FSubset(pbs1) && pbs->FSubset(pbs2));
	
	pbs->Difference(pbs2);
	GPOS_ASSERT(pbs->FEqual(pbs1));

	pbs1->Release();

	pbs->Union(pbs2);
	pbs->Intersection(pbs2);
	GPOS_ASSERT(pbs->FEqual(pbs2));
	GPOS_ASSERT(pbs->FSubset(pbs2));

	GPOS_ASSERT(pbs->CElements() == pbs2->CElements());

	pbs2->Release();

	pbs->Release();

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSetTest::EresUnittest_Performance
//
//	@doc:
//		Simple perf test -- simulates xform candidate sets
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitSetTest::EresUnittest_Performance()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	ULONG cSizeBits = 512;
	CBitSet *pbsBase = GPOS_NEW(pmp) CBitSet(pmp, cSizeBits);
	for (ULONG i = 0; i < cSizeBits; i++)
		{
			(void) pbsBase->FExchangeSet(i);
		}

	CBitSet *pbsTest = GPOS_NEW(pmp) CBitSet(pmp, cSizeBits);
	for (ULONG j = 0; j < 100000; j++)
	{
		ULONG cRandomBits = 16;
		for (ULONG i = 0; i < cRandomBits; i += ((cSizeBits - 1) / cRandomBits))
		{
			(void) pbsTest->FExchangeSet(i);
		}
			
		pbsTest->Intersection(pbsBase);		
	}	
	
	pbsTest->Release();
	pbsBase->Release();
	
	return GPOS_OK;
}	

// EOF

