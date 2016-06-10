//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CNameTest.cpp
//
//	@doc:
//      Test for CName
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/metadata/CName.h"

#include "unittest/gpopt/metadata/CNameTest.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CNameTest::EresUnittest
//
//	@doc:
//		Unittest for metadata names
//
//---------------------------------------------------------------------------
GPOS_RESULT
CNameTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CNameTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CNameTest::EresUnittest_Ownership)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CNameTest::EresUnittest_Basic
//
//	@doc:
//		basic naming, assignment thru copy constructors
//
//---------------------------------------------------------------------------
GPOS_RESULT
CNameTest::EresUnittest_Basic()
{
	CWStringConst strName(GPOS_WSZ_LIT("nametest"));
	CName name1(&strName);
	CName name2(name1);
	CName name3 = name2;

	GPOS_ASSERT(name1.FEquals(name2));
	GPOS_ASSERT(name1.FEquals(name3));
	GPOS_ASSERT(name2.FEquals(name3));

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CNameTest::EresUnittest_Ownership
//
//	@doc:
//		basic name with deep copies
//
//---------------------------------------------------------------------------
GPOS_RESULT
CNameTest::EresUnittest_Ownership()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringConst strName(GPOS_WSZ_LIT("nametest"));
	CName name1(&strName);

	CName name2(pmp, name1);
	CName name3(pmp, name2);

	GPOS_ASSERT(name1.FEquals(name2));
	GPOS_ASSERT(name1.FEquals(name3));
	GPOS_ASSERT(name2.FEquals(name3));

	return GPOS_OK;
}


// EOF

