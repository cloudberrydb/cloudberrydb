//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLMemoryManagerTest.cpp
//
//	@doc:
//		Tests the memory manager to be plugged in Xerces.
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/dxl/CDXLMemoryManagerTest.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLMemoryManagerTest::EresUnittest
//
//	@doc:
//		
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDXLMemoryManagerTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CDXLMemoryManagerTest::EresUnittest_Basic)
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLMemoryManagerTest::EresUnittest_Basic
//
//	@doc:
//		Test for allocating and deallocating memory, as required by the Xerces parser
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDXLMemoryManagerTest::EresUnittest_Basic()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	CDXLMemoryManager *pmm = GPOS_NEW(pmp) CDXLMemoryManager(pmp);
	void *pvMemory = pmm->allocate(5);
	
	GPOS_ASSERT(NULL != pvMemory);
	
	pmm->deallocate(pvMemory);
	
	// cleanup
	GPOS_DELETE(pmm);
	// pvMemory is deallocated through the memory manager, otherwise the test will throw
	// with a memory leak
	
	return GPOS_OK;
}





// EOF
