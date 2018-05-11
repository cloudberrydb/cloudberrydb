//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//
//	@filename:
//		CMemoryPoolAllocTest.cpp
//
//	@doc:
//		Test for CMemoryPoolAllocTest
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CMainArgs.h"

#include "gpos/assert.h"
#include "gpos/memory/CMemoryPoolAlloc.h"
#include "gpos/test/CUnittest.h"
#include "unittest/gpos/memory/CMemoryPoolAllocTest.h"

using namespace gpos;

SIZE_T CCustomAllocator::m_ulTotalMemoryAllocation = 0;
SIZE_T CCustomAllocator::m_ulLastMemoryAllocation = 0;

void* 
CCustomAllocator::fnAlloc
  (
    SIZE_T ulAlloc
  )
{
  m_ulTotalMemoryAllocation += ulAlloc;
  m_ulLastMemoryAllocation = ulAlloc;

  return clib::Malloc(ulAlloc);
}

void
CCustomAllocator::fnFree
  (
    void* pMem
  )
{
  m_ulTotalMemoryAllocation -= m_ulLastMemoryAllocation;

  return clib::Free(pMem);
}

BOOL
gpos::FUseCustomAllocator
  (
    CMainArgs* pma
  )
{
  CHAR ch = '\0';
  bool fUseCustom  = false;
  while (pma->Getopt(&ch))
  {
    switch(ch)
    {
      case 'c':
        fUseCustom = true;
        break;
      default:
        // ignore other arguments
        break;
    }
  }
  // Reset the optargs
  optind = 1;
  return fUseCustom;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolAllocTest::EresUnittest
//
//	@doc:
//		unit test driver
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolAllocTest::EresUnittest() {
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMemoryPoolAllocTest::EresUnittest_AllocFree)
		};

	// execute unit tests
	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolAllocTest::EresUnittest_AllocFree
//
//	@doc:
//	  Unit test to test a simple allocation and free through custom allocator
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolAllocTest::EresUnittest_AllocFree() {
  CMemoryPoolAlloc customAlloc(CCustomAllocator::fnAlloc, CCustomAllocator::fnFree);

  SIZE_T ulTotalMemoryAllocated = CCustomAllocator::UlGetTotalMemoryAllocation();
  void * pMem = customAlloc.Allocate(100, __FILE__, __LINE__);
  GPOS_RTL_ASSERT
    (
      (ulTotalMemoryAllocated + 100) ==
      CCustomAllocator::UlGetTotalMemoryAllocation()
    );
  customAlloc.Free(pMem);
  GPOS_RTL_ASSERT
    (
     ulTotalMemoryAllocated ==
     CCustomAllocator::UlGetTotalMemoryAllocation()
     );
  return GPOS_OK;
}

// EOF

