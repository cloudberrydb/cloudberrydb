//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//	@filename:
//		CMemoryPoolAllocTest.h
//
//	@doc:
//		Test for CMemoryPoolAllocTest
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolAllocTest_H
#define GPOS_CMemoryPoolAllocTest_H

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CMainArgs.h"

#define GPOS_INIT(pma)  struct gpos_init_params init_params = { \
    (FUseCustomAllocator(pma) ? CCustomAllocator::fnAlloc : NULL), \
    (FUseCustomAllocator(pma) ? CCustomAllocator::fnFree : NULL), NULL}; \
    gpos_init(&init_params);

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CCustomAllocator
	//
	//	@doc:
  //	  Helper class to perform and track memory allocations
	//
	//---------------------------------------------------------------------------
  class CCustomAllocator
  {
    private:
      static SIZE_T m_ulTotalMemoryAllocation;
      static SIZE_T m_ulLastMemoryAllocation;

    public:
      static void* fnAlloc(SIZE_T ulAlloc);

      static void fnFree(void* pMem);

      static SIZE_T UlGetTotalMemoryAllocation()
      {
        return m_ulTotalMemoryAllocation;
      }

      static SIZE_T UlGetLastMemoryAllocation()
      {
        return m_ulLastMemoryAllocation;
      }

  };

  BOOL FUseCustomAllocator(CMainArgs* pma);


	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPoolAllocTest
	//
	//	@doc:
	//		Unittest for CMemoryPoolAllocTest
	//
	//---------------------------------------------------------------------------
	class CMemoryPoolAllocTest
	{
    public:
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_AllocFree();
	};
}

#endif // !GPOS_CMemoryPoolAllocTest_H

// EOF

