//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CListTest.cpp
//
//	@doc:
//		Tests for CList
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CList.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/common/CListTest.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CListTest::EresUnittest
//
//	@doc:
//		Unittest for lists
//
//---------------------------------------------------------------------------
GPOS_RESULT
CListTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CListTest::EresUnittest_Basics),
		GPOS_UNITTEST_FUNC(CListTest::EresUnittest_Navigate),
		GPOS_UNITTEST_FUNC(CListTest::EresUnittest_Cursor),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CListTest::EresUnittest_Basics
//
//	@doc:
//		Various list operations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CListTest::EresUnittest_Basics()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CList<SElem> listFwd;
	listFwd.Init(GPOS_OFFSET(SElem, m_linkFwd));

	CList<SElem> listBwd;
	listBwd.Init(GPOS_OFFSET(SElem, m_linkBwd));

	ULONG cSize = 10;
	SElem *rgelem = GPOS_NEW_ARRAY(pmp, SElem, cSize);

	GPOS_ASSERT(0 == listFwd.UlSize());
	GPOS_ASSERT(0 == listBwd.UlSize());

	// insert all elements
	for(ULONG i = 0; i < cSize; i++)
	{
		GPOS_ASSERT(i == listFwd.UlSize());
		GPOS_ASSERT(i == listBwd.UlSize());

		listFwd.Prepend(&rgelem[i]);
		listBwd.Append(&rgelem[i]);
	}

	GPOS_ASSERT(cSize == listFwd.UlSize());
	GPOS_ASSERT(cSize == listBwd.UlSize());

	// remove first/last element until empty
	for(ULONG i = 0; i < cSize; i++)
	{
		GPOS_ASSERT(cSize - i == listFwd.UlSize());
		GPOS_ASSERT(&rgelem[i] == listFwd.PtLast());
		listFwd.Remove(listFwd.PtLast());

		// make sure it's still in the other list
		GPOS_ASSERT(GPOS_OK == listBwd.EresFind(&rgelem[i]));
	}
	GPOS_ASSERT(NULL == listFwd.PtFirst());
	GPOS_ASSERT(0 == listFwd.UlSize());

	// insert all elements in reverse order,
	// i.e. list is in same order as array
	for(ULONG i = cSize; i > 0; i--)
	{
		GPOS_ASSERT(cSize - i == listFwd.UlSize());
		listFwd.Prepend(&rgelem[i - 1]);
	}
	GPOS_ASSERT(cSize == listFwd.UlSize());

	for(ULONG i = 0; i < cSize; i++)
	{
		listFwd.Remove(&rgelem[(cSize/2 + i) % cSize]);
	}
	GPOS_ASSERT(NULL == listFwd.PtFirst());
	GPOS_ASSERT(NULL == listFwd.PtLast());
	GPOS_ASSERT(0 == listFwd.UlSize());

	GPOS_DELETE_ARRAY(rgelem);
	return GPOS_OK;
}




//---------------------------------------------------------------------------
//	@function:
//		CListTest::EresUnittest_Navigate
//
//	@doc:
//		Various navigation operations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CListTest::EresUnittest_Navigate()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CList<SElem> listFwd;
	listFwd.Init(GPOS_OFFSET(SElem, m_linkFwd));

	CList<SElem> listBwd;
	listBwd.Init(GPOS_OFFSET(SElem, m_linkBwd));

	ULONG cSize = 10;
	SElem *rgelem = GPOS_NEW_ARRAY(pmp, SElem, cSize);

	// insert all elements in reverse order,
	// i.e. list is in same order as array
	for(ULONG i = 0; i < cSize; i++)
	{
		listBwd.Prepend(&rgelem[i]);
		listFwd.Append(&rgelem[i]);
	}

	// use getnext to walk list
	SElem *pelem = listFwd.PtFirst();
	for(ULONG i = 0; i < cSize; i++)
	{
		GPOS_ASSERT(pelem == &rgelem[i]);
		pelem = listFwd.PtNext(pelem);
	}
	GPOS_ASSERT(NULL == pelem);

	// go to end of list -- then traverse backward
	pelem = listFwd.PtFirst();
	while(pelem && listFwd.PtNext(pelem))
	{
		pelem = listFwd.PtNext(pelem);
	}
	GPOS_ASSERT(listFwd.PtLast() == pelem);

	for(ULONG i = cSize; i > 0; i--)
	{
		GPOS_ASSERT(pelem == &rgelem[i - 1]);
		pelem = listFwd.PtPrev(pelem);
	}
	GPOS_ASSERT(NULL == pelem);

	GPOS_DELETE_ARRAY(rgelem);
	return GPOS_OK;
}



//---------------------------------------------------------------------------
//	@function:
//		CListTest::EresUnittest_Cursor
//
//	@doc:
//		Various cursor-based inserts
//
//---------------------------------------------------------------------------
GPOS_RESULT
CListTest::EresUnittest_Cursor()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CList<SElem> list;
	list.Init(GPOS_OFFSET(SElem, m_linkFwd));

	ULONG cSize = 5;
	SElem *rgelem = GPOS_NEW_ARRAY(pmp, SElem, cSize);

	list.Append(&rgelem[0]);

	list.Prepend(&rgelem[1], list.PtFirst());
	list.Append(&rgelem[2], list.PtLast());

	GPOS_ASSERT(&rgelem[1] == list.PtFirst());
	GPOS_ASSERT(&rgelem[2] == list.PtLast());

	list.Prepend(&rgelem[3], list.PtLast());
	list.Append(&rgelem[4], list.PtFirst());

	GPOS_ASSERT(&rgelem[1] == list.PtFirst());
	GPOS_ASSERT(&rgelem[2] == list.PtLast());

	GPOS_DELETE_ARRAY(rgelem);
	return GPOS_OK;
}

// EOF


