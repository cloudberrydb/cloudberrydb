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

#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"

namespace gpos
{
	class CMemoryPoolBasicTest
	{
		private:

			static GPOS_RESULT EresTestType(CMemoryPoolManager::AllocType eat);
			static GPOS_RESULT EresTestExpectedError
				(
				GPOS_RESULT (*pfunc)(CMemoryPoolManager::AllocType),
				CMemoryPoolManager::AllocType eat,
				ULONG minor
				);

			static GPOS_RESULT EresNewDelete(CMemoryPoolManager::AllocType eat);
			static GPOS_RESULT EresOOM(CMemoryPoolManager::AllocType eat);
			static GPOS_RESULT EresThrowingCtor(CMemoryPoolManager::AllocType eat);
#ifdef GPOS_DEBUG
			static GPOS_RESULT EresLeak(CMemoryPoolManager::AllocType eat);
			static GPOS_RESULT EresLeakByException(CMemoryPoolManager::AllocType eat);
#endif // GPOS_DEBUG
			static GPOS_RESULT EresConcurrency(CMemoryPoolManager::AllocType eat);
			static GPOS_RESULT EresStress(CMemoryPoolManager::AllocType eat);

			static void *AllocateSerial(void *pv);
			static void *AllocateRepeated(void *pv);
			static void *AllocateStress(void *pv);
			static void Allocate(CMemoryPool *mp, ULONG count);
			static void AllocateRandom(CMemoryPool *mp);
			static ULONG Size(ULONG offset);

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

