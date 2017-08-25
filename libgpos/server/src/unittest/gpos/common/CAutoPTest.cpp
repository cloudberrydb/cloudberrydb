//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CAutoPTest.cpp
//
//	@doc:
//		Tests for CAutoP
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpos/common/CAutoP.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CAutoPTest.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CAutoP::EresUnittest
//
//	@doc:
//		Unittest for auto pointers
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoPTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CAutoPTest::EresUnittest_Basics)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoPTest::EresUnittest_Basics
//
//	@doc:
//		Various basic operations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CAutoPTest::EresUnittest_Basics()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// assignment
	CAutoP<CHAR> asz;
	CHAR *sz = GPOS_NEW(pmp) CHAR;
	asz = sz;

	CAutoP<CHAR> asz2;
	CAutoP<CHAR> asz3;
	CHAR *sz2 = GPOS_NEW(pmp) CHAR;

	*sz2 = '\0';
	asz2 = sz2;

	// default assignment
	asz3 = asz2;

	// accessor
#ifdef GPOS_DEBUG
	CHAR *szBack = asz3.Pt();
	GPOS_ASSERT(szBack == sz2);
#endif // GPOS_DEBUG

	// deref
	GPOS_ASSERT(*sz2 == *asz3);

	// wipe out asz2 to prevent double free
	asz2 = NULL;

	// unhooking of object
	GPOS_DELETE(asz3.PtReset());

	CElem *pelem = GPOS_NEW(pmp) CElem;
	pelem->m_ul = 3;

	CAutoP<CElem> aelem;
	aelem = pelem;

	// deref
	GPOS_ASSERT(pelem->m_ul == aelem->m_ul);

	// c'tor
	CAutoP<CHAR> asz4(GPOS_NEW(pmp) CHAR);
	*(asz4.Pt()) = 'a';

	return GPOS_OK;
}

// EOF

