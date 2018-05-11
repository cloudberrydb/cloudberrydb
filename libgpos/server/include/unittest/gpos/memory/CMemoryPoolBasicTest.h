//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPoolBasicTest.h
//
//	@doc:
//      Test for CMemoryPoolBasic
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolBasicTest_H
#define GPOS_CMemoryPoolBasicTest_H

#include "gpos/memory/IMemoryPool.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"

namespace gpos
{
	class CMemoryPoolBasicTest
	{
		private:

			static GPOS_RESULT EresTestType(CMemoryPoolManager::EAllocType eat);
			static GPOS_RESULT EresTestExpectedError
				(
				GPOS_RESULT (*pfunc)(CMemoryPoolManager::EAllocType),
				CMemoryPoolManager::EAllocType eat,
				ULONG ulMinor
				);

			static GPOS_RESULT EresNewDelete(CMemoryPoolManager::EAllocType eat);
			static GPOS_RESULT EresOOM(CMemoryPoolManager::EAllocType eat);
			static GPOS_RESULT EresThrowingCtor(CMemoryPoolManager::EAllocType eat);
#ifdef GPOS_DEBUG
			static GPOS_RESULT EresLeak(CMemoryPoolManager::EAllocType eat);
			static GPOS_RESULT EresLeakByException(CMemoryPoolManager::EAllocType eat);
#endif // GPOS_DEBUG
			static GPOS_RESULT EresConcurrency(CMemoryPoolManager::EAllocType eat);
			static GPOS_RESULT EresStress(CMemoryPoolManager::EAllocType eat);

			static void *AllocateSerial(void *pv);
			static void *AllocateRepeated(void *pv);
			static void *AllocateStress(void *pv);
			static void Allocate(IMemoryPool *pmp, ULONG ulCount);
			static void AllocateRandom(IMemoryPool *pmp);
			static ULONG UlSize(ULONG ulOffset);

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
#ifdef GPOS_DEBUG
			static GPOS_RESULT EresUnittest_Print();
#endif // GPOS_DEBUG
			static GPOS_RESULT EresUnittest_TestTracker();
			static GPOS_RESULT EresUnittest_TestSlab();
			static GPOS_RESULT EresUnittest_TestStack();

	}; // class CMemoryPoolBasicTest
}

#endif // !GPOS_CMemoryPoolBasicTest_H

// EOF

