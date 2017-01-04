//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CBitVectorTest.cpp
//
//	@doc:
//		Tests for CBitVector
//---------------------------------------------------------------------------


#include "gpos/base.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CBitVector.h"
#include "gpos/common/CAutoRg.h"
#include "gpos/common/CRandom.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CBitVectorTest.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::EresUnittest
//
//	@doc:
//		Unittest for bit vectors
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitVectorTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CBitVectorTest::EresUnittest_Basics),
		GPOS_UNITTEST_FUNC(CBitVectorTest::EresUnittest_SetOps),
		GPOS_UNITTEST_FUNC(CBitVectorTest::EresUnittest_Cursor),
		GPOS_UNITTEST_FUNC(CBitVectorTest::EresUnittest_Random)
#ifdef GPOS_DEBUG
		,
		GPOS_UNITTEST_FUNC_ASSERT(CBitVectorTest::EresUnittest_OutOfBounds)
#endif // GPOS_DEBUG
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVectorTest::EresUnittest_Basics
//
//	@doc:
//		Various basic operations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitVectorTest::EresUnittest_Basics()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG cSize = 129;

	CBitVector bv(pmp, cSize);
	GPOS_ASSERT(bv.FEmpty());

	for(ULONG i = 0; i < cSize; i++)
	{
		BOOL fSet = bv.FExchangeSet(i);
		if(fSet)
		{
			return GPOS_FAILED;
		}
		GPOS_ASSERT(bv.FBit(i));

		CBitVector bvCopy(pmp, bv);
		for(ULONG j = 0; j <= i; j++)
		{
			BOOL fSetAlt = bvCopy.FBit(j);
			GPOS_ASSERT(fSetAlt);

			if (true != fSetAlt)
			{
				return GPOS_FAILED;
			}

			// clear and check
			bvCopy.FExchangeClear(j);
			fSetAlt = bvCopy.FBit(j);
			GPOS_ASSERT(!fSetAlt);
		}

		GPOS_ASSERT(bvCopy.CElements() == 0);
	}

	GPOS_ASSERT(bv.CElements() == cSize);

	return GPOS_OK;
}



//---------------------------------------------------------------------------
//	@function:
//		CBitVectorTest::EresUnittest_SetOps
//
//	@doc:
//		Set operation tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitVectorTest::EresUnittest_SetOps()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	ULONG cSize = 129;
	CBitVector bvEmpty(pmp, cSize);

	CBitVector bvEven(pmp, cSize);
	for(ULONG i = 0; i < cSize; i += 2)
	{
		bvEven.FExchangeSet(i);
	}
	GPOS_ASSERT(bvEven.FSubset(&bvEmpty));

	CBitVector bvOdd(pmp, cSize);
	for(ULONG i = 1; i < cSize; i += 2)
	{
		bvOdd.FExchangeSet(i);
	}
	GPOS_ASSERT(bvOdd.FSubset(&bvEmpty));
	GPOS_ASSERT(bvOdd.FDisjoint(&bvEven));

	GPOS_ASSERT(!bvEven.FSubset(&bvOdd));
	GPOS_ASSERT(!bvOdd.FSubset(&bvEven));

	CBitVector bv(pmp, bvOdd);

	bv.Union(&bvEven);
	bv.Intersection(&bvOdd);
	GPOS_ASSERT(bv.FEqual(&bvOdd));

	bv.Union(&bvEven);
	bv.Intersection(&bvEven);
	GPOS_ASSERT(bv.FEqual(&bvEven));

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVectorTest::EresUnittest_Cursor
//
//	@doc:
//		Unittest for cursoring
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitVectorTest::EresUnittest_Cursor()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CBitVector bv(pmp, 129);
	for(ULONG i = 1; i < 20; i ++)
	{
		bv.FExchangeSet(i * 3);
	}

	ULONG ulCursor = 0;
	bv.FNextBit(0, ulCursor);
	while(bv.FNextBit(ulCursor + 1, ulCursor))
	{
		GPOS_ASSERT(ulCursor == ((ulCursor / 3) * 3));
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVectorTest::EresUnittest_Random
//
//	@doc:
//		Test with random bit vectors to avoid patterns
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitVectorTest::EresUnittest_Random()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// set up control vector
	ULONG cTotal = 10000;
	CHAR *rg = GPOS_NEW_ARRAY(pmp, CHAR, cTotal);
	
	CRandom rand;
	
	clib::PvMemSet(rg, 0 , cTotal);

	// set random chars in the control vector
	for (ULONG i = 0; i < cTotal * 0.2; i++)
	{
		ULONG index = rand.ULNext() % (cTotal - 1);
		GPOS_ASSERT(index < cTotal);
		rg[index] = 1;
	}

	ULONG cElements = 0;
	CBitVector bv(pmp, cTotal);
	for (ULONG i = 0; i < cTotal; i++)
	{
		if (1 == rg[i])
		{
			bv.FExchangeSet(i);
			cElements++;
		}
	}

	GPOS_ASSERT(cElements == bv.CElements());

	ULONG ulCursor = 0;
	while(bv.FNextBit(ulCursor + 1, ulCursor))
	{
		GPOS_ASSERT(1 == rg[ulCursor]);
		cElements--;
	}

	GPOS_ASSERT(0 == cElements);
	GPOS_DELETE_ARRAY(rg);

	return GPOS_OK;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CBitVectorTest::EresUnittest_OutOfBounds
//
//	@doc:
//		Unittest for OOB access
//
//---------------------------------------------------------------------------
GPOS_RESULT
CBitVectorTest::EresUnittest_OutOfBounds()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CBitVector bv(pmp, 129);

	// this must assert
	bv.FExchangeSet(130);

	return GPOS_FAILED;
}

#endif // GPOS_DEBUG

// EOF

