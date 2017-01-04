//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CEnumSetTest.cpp
//
//	@doc:
//      Test for CEnumSet/CEnumSetIter
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/types.h"

#include "gpos/common/CEnumSet.h"
#include "gpos/common/CEnumSetIter.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CEnumSetTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CEnumSetTest::EresUnittest
//
//	@doc:
//		Unittest for enum sets
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEnumSetTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CEnumSetTest::EresUnittest_Basics)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CEnumSetTest::EresUnittest_Basics
//
//	@doc:
//		Testing ctors/dtor, accessors, iterator
//
//---------------------------------------------------------------------------
GPOS_RESULT
CEnumSetTest::EresUnittest_Basics()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	typedef CEnumSet<eTest, eTestSentinel> CETestSet;
	typedef CEnumSetIter<eTest, eTestSentinel> CETestIter;

	CETestSet *pes = GPOS_NEW(pmp) CETestSet(pmp);
	
	(void) pes->FExchangeSet(eTestOne);
	(void) pes->FExchangeSet(eTestTwo);
	
	GPOS_ASSERT(pes->FExchangeClear(eTestTwo));
	GPOS_ASSERT(!pes->FExchangeSet(eTestTwo));

	CETestIter eti(*pes);
	while(eti.FAdvance())
	{
		GPOS_ASSERT((BOOL)eti);
		GPOS_ASSERT(eTestSentinel > eti.TBit());
		GPOS_ASSERT(pes->FBit(eti.TBit()));
	}
	
	pes->Release();

	return GPOS_OK;
}

// EOF

