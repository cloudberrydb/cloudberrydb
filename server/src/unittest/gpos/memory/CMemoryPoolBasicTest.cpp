//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CMemoryPoolBasicTest.cpp
//
//	@doc:
//		Tests for CMemoryPoolBasicTest
//---------------------------------------------------------------------------

#include "gpos/assert.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/error/CException.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CMemoryVisitorPrint.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CWorkerPoolManager.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/memory/CMemoryPoolBasicTest.h"

#define GPOS_MEM_TEST_STRESS_TASKS	(8)
#define GPOS_MEM_TEST_ALLOC_SMALL	(8)
#define GPOS_MEM_TEST_ALLOC_LARGE	(256)
#define GPOS_MEM_TEST_ALLOC_MAX     (1024 * 1024)

#ifdef GPOS_DEBUG
#define GPOS_MEM_TEST_CFA           (10)
#define GPOS_MEM_TEST_LOOP_SHORT	(10)
#define GPOS_MEM_TEST_LOOP_STRESS	(100)
#define GPOS_MEM_TEST_LOOP_LONG 	(1000)
#else
#define GPOS_MEM_TEST_CFA           (100)
#define GPOS_MEM_TEST_LOOP_SHORT	(100)
#define GPOS_MEM_TEST_LOOP_STRESS	(1000)
#define GPOS_MEM_TEST_LOOP_LONG 	(100000)
#endif // GPOS_DEBUG

#define GPOS_MEM_TEST_REPEAT_SHORT	(GPOS_MEM_TEST_LOOP_LONG / GPOS_MEM_TEST_LOOP_SHORT)

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresUnittest
//
//	@doc:
//		Basic tests for memory management abstraction
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresUnittest()
{
	CUnittest rgut[] =
		{
#ifdef GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CMemoryPoolBasicTest::EresUnittest_Print),
#endif // GPOS_DEBUG
		GPOS_UNITTEST_FUNC(CMemoryPoolBasicTest::EresUnittest_TestTracker),
		GPOS_UNITTEST_FUNC(CMemoryPoolBasicTest::EresUnittest_TestSlab),
		GPOS_UNITTEST_FUNC(CMemoryPoolBasicTest::EresUnittest_TestStack),
		};

	CAutoTraceFlag atf(EtraceTestMemoryPools, true /*fVal*/);

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresUnittest_Print
//
//	@doc:
//		Print memory pools
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresUnittest_Print()
{
	CAutoTraceFlag atfStackTrace(EtracePrintMemoryLeakStackTrace, true);

	const ULONG ulBufferSize = 4096;
	WCHAR wsz[ulBufferSize];
	CWStringStatic str(wsz, GPOS_ARRAY_SIZE(wsz));
	COstreamString os(&str);

	(void) CMemoryPoolManager::Pmpm()->OsPrint(os);
	GPOS_TRACE(str.Wsz());

	return GPOS_OK;
}

#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresUnittest_TestTracker
//
//	@doc:
//		Run tests for pool tracking allocations
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresUnittest_TestTracker()
{
	return EresTestType(CMemoryPoolManager::EatTracker);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresUnittest_TestSlab
//
//	@doc:
//		Run tests for pool using slab allocation
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresUnittest_TestSlab()
{
	return EresTestType(CMemoryPoolManager::EatSlab);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresUnittest_TestStack
//
//	@doc:
//		Run tests for pool using stack allocation
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresUnittest_TestStack()
{
	return EresTestType(CMemoryPoolManager::EatStack);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresTestType
//
//	@doc:
//		Run tests per pool type
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresTestType
	(
	CMemoryPoolManager::EAllocType eat
	)
{
	if (GPOS_OK != EresNewDelete(eat) ||
	    GPOS_OK != EresTestExpectedError(EresOOM, eat, CException::ExmiOOM) ||
	    GPOS_OK != EresTestExpectedError(EresThrowingCtor, eat, CException::ExmiOOM) ||

#ifdef GPOS_DEBUG
	    GPOS_OK != EresTestExpectedError(EresLeak, eat, CException::ExmiAssert) ||
	    GPOS_OK != EresTestExpectedError(EresLeakByException, eat, CException::ExmiAssert) ||
#endif // GPOS_DEBUG
	    GPOS_OK != EresConcurrency(eat)
#if defined(GPOS_64BIT) && defined(GPOS_DEBUG)
	    ||
	    GPOS_OK != EresStress(eat)
#endif
	    )
	{
		return GPOS_FAILED;
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresTestExpectedError
//
//	@doc:
//		Run test that is expected to raise an exception
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresTestExpectedError
	(
	GPOS_RESULT (*pfunc)(CMemoryPoolManager::EAllocType),
	CMemoryPoolManager::EAllocType eat,
	ULONG ulMinor
	)
{
	CErrorHandlerStandard errhdl;
	GPOS_TRY_HDL(&errhdl)
	{
		pfunc(eat);
	}
	GPOS_CATCH_EX(ex)
	{
		if (CException::ExmaSystem == ex.UlMajor() &&
			ulMinor == ex.UlMinor())
		{
			GPOS_RESET_EX;

			return GPOS_OK;
		}

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	return GPOS_FAILED;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresNewDelete
//
//	@doc:
//		Basic tests for allocation and free-ing
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresNewDelete
	(
	CMemoryPoolManager::EAllocType eat
	)
{
	// create memory pool
	CAutoTimer at("NewDelete test", true /*fPrint*/);
	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc, eat, false /*fThreadSafe*/);
	IMemoryPool *pmp = amp.Pmp();

	WCHAR rgwszText[] = GPOS_WSZ_LIT(
							"This is a lengthy test string. "
							"Nothing serious just to demonstrate that "
							"you can allocate using New and free using "
							"delete. That's all. Special characters anybody? "
							"Sure thing: \x07 \xAB \xFF!. End of string."
						);

	// use overloaded New operator
	WCHAR *wsz = GPOS_NEW_ARRAY(pmp, WCHAR, GPOS_ARRAY_SIZE(rgwszText));
	(void) clib::WszWMemCpy(wsz, rgwszText, GPOS_ARRAY_SIZE(rgwszText));

#ifdef GPOS_DEBUG

	WCHAR rgBuffer[8 * 1024];
	CWStringStatic str(rgBuffer, GPOS_ARRAY_SIZE(rgBuffer));
	COstreamString os(&str);

	// dump allocations
	if (pmp->FSupportsLiveObjectWalk())
	{
		CMemoryVisitorPrint movp(os);
		pmp->WalkLiveObjects(&movp);
	}
	else
	{
		os << "Memory dump unavailable";
	}
	os << std::endl;

	GPOS_TRACE(str.Wsz());

#endif // GPOS_DEBUG

	GPOS_DELETE_ARRAY(wsz);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresOOM
//
//	@doc:
//		Basic tests for OOM situation
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresOOM
	(
	CMemoryPoolManager::EAllocType eat
	)
{
	CAutoTimer at("OOM test", true /*fPrint*/);

	// create memory pool
	CAutoMemoryPool amp
		(
		CAutoMemoryPool::ElcExc,
		eat,
		false /*fThreadSafe*/,
		4 * 1024 * 1024 /*ullMaxSize*/
		);
	IMemoryPool *pmp = amp.Pmp();

	// OOM
	GPOS_NEW_ARRAY(pmp, BYTE, 128 * 1024 * 1024);

	return GPOS_FAILED;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresThrowingCtor
//
//	@doc:
//		Basic tests for exception in constructor
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresThrowingCtor
	(
	CMemoryPoolManager::EAllocType eat
	)
{
	CAutoTimer at("ThrowingCtor test", true /*fPrint*/);

	// create memory pool
	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc, eat, false /*fThreadSafe*/);
	IMemoryPool *pmp = amp.Pmp();

	// malicious test class
	class CMyTestClass
	{
		public:

			CMyTestClass()
			{
				// throw in ctor
				GPOS_RAISE(CException::ExmaSystem, CException::ExmiOOM);
			}
	};

	// try instantiating the class
	GPOS_NEW(pmp) CMyTestClass();

	// doesn't reach this line
	return GPOS_FAILED;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresLeak
//
//	@doc:
//		Basic tests for leak checking
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresLeak
	(
	CMemoryPoolManager::EAllocType eat
	)
{
	CAutoTraceFlag atfDump(EtracePrintMemoryLeakDump, true);
	CAutoTraceFlag atfStackTrace(EtracePrintMemoryLeakStackTrace, true);
	CAutoTimer at("Leak test", true /*fPrint*/);

	// scope for pool
	{
		CAutoMemoryPool amp
			(
			CAutoMemoryPool::ElcStrict,
			eat,
			false /*fThreadSafe*/,
			4 * 1024 * 1024 /*ullMaxSize*/
			);
		IMemoryPool *pmp = amp.Pmp();

		for (ULONG i = 0; i < 10; i++)
		{
			// use overloaded New operator
			ULONG *rgul = GPOS_NEW_ARRAY(pmp, ULONG, 10);
			rgul[2] = 1;

			if (i < 8)
			{
				GPOS_DELETE_ARRAY(rgul);
			}
		}
	}

	return GPOS_FAILED;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresLeakByException
//
//	@doc:
//		Basic test for ignored leaks under exception
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresLeakByException
	(
	CMemoryPoolManager::EAllocType eat
	)
{
	CAutoTraceFlag atfDump(EtracePrintMemoryLeakDump, true);
	CAutoTraceFlag atfStackTrace(EtracePrintMemoryLeakStackTrace, true);
	CAutoTimer at("LeakByException test", true /*fPrint*/);

	// scope for pool
	{
		// create memory pool
		CAutoMemoryPool amp
			(
			CAutoMemoryPool::ElcExc,
			eat,
			false /*fThreadSafe*/,
			4 * 1024 * 1024 /*ullMaxSize*/
			);
		IMemoryPool *pmp = amp.Pmp();

		for (ULONG i = 0; i < 10; i++)
		{
			// use overloaded New operator
			ULONG *rgul = GPOS_NEW_ARRAY(pmp, ULONG, 3);
			rgul[2] = 1;
		}

		GPOS_ASSERT(!"Trigger leak with exception");
	}

	return GPOS_FAILED;
}

#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresConcurrency
//
//	@doc:
//		Concurrency test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresConcurrency
	(
	CMemoryPoolManager::EAllocType eat
	)
{
	CAutoTimer at("Concurrency test", true /*fPrint*/);

	// create memory pool
	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc, eat, true /*fThreadSafe*/);
	IMemoryPool *pmp = amp.Pmp();
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgptsk[GPOS_MEM_TEST_STRESS_TASKS];

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk) / 2; i++)
		{
			rgptsk[i] = atp.PtskCreate(AllocateSerial, pmp);
		}

		for (ULONG i = GPOS_ARRAY_SIZE(rgptsk) / 2; i < GPOS_ARRAY_SIZE(rgptsk); i++)
		{
			rgptsk[i] = atp.PtskCreate(AllocateRepeated, pmp);
		}

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk); i++)
		{
			atp.Schedule(rgptsk[i]);
		}

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk); i++)
		{
			atp.Wait(rgptsk[i]);
		}
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::AllocateSerial
//
//	@doc:
//		Allocate and release memory serially
//
//---------------------------------------------------------------------------
void *
CMemoryPoolBasicTest::AllocateSerial
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	IMemoryPool *pmp = static_cast<IMemoryPool*>(pv);

	Allocate(pmp, GPOS_MEM_TEST_LOOP_LONG);

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::AllocateRepeated
//
//	@doc:
//		Repeat loops of memory allocation and release
//
//---------------------------------------------------------------------------
void *
CMemoryPoolBasicTest::AllocateRepeated
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	IMemoryPool *pmp = static_cast<IMemoryPool*>(pv);

	for (ULONG i = 0; i < GPOS_MEM_TEST_REPEAT_SHORT; i++)
	{
		Allocate(pmp, GPOS_MEM_TEST_LOOP_SHORT);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::Allocate
//
//	@doc:
//		Allocate and release memory in batches
//
//---------------------------------------------------------------------------
void
CMemoryPoolBasicTest::Allocate
	(
	IMemoryPool *pmp,
	ULONG ulCount
	)
{
	BYTE **rgpb = GPOS_NEW_ARRAY(pmp, BYTE*, ulCount);

	for (ULONG i = 0; i < ulCount; i++)
	{
		const ULONG ulSize = UlSize(i);
		rgpb[i] = GPOS_NEW_ARRAY(pmp, BYTE, ulSize);
#ifdef GPOS_DEBUG
		(void) clib::PvMemSet(rgpb[i], 1, ulSize);
#endif // GPOS_DEBUG

		if (0 == i % GPOS_MEM_TEST_CFA)
		{
			GPOS_CHECK_ABORT;
		}
	}

	GPOS_CHECK_ABORT;

	for (ULONG i = 0; i < ulCount; i++)
	{
#ifdef GPOS_DEBUG
		GPOS_ASSERT(NULL != rgpb[i]);

		const ULONG ulSize = UlSize(i);
		for (ULONG j = 0; j < ulSize; j++)
		{
			GPOS_ASSERT(1 == rgpb[i][j]);
		}
#endif // GPOS_DEBUG

		GPOS_DELETE_ARRAY(rgpb[i]);

		if (0 == i % GPOS_MEM_TEST_CFA)
		{
			GPOS_CHECK_ABORT;
		}
	}

	GPOS_CHECK_ABORT;

	GPOS_DELETE_ARRAY(rgpb);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::UlSize
//
//	@doc:
//		Pick allocation size based on offset
//
//---------------------------------------------------------------------------
ULONG
CMemoryPoolBasicTest::UlSize
	(
	ULONG ulOffset
	)
{
	if (0 == (ulOffset & 1))
	{
		return GPOS_MEM_TEST_ALLOC_SMALL;
	}
	return GPOS_MEM_TEST_ALLOC_LARGE;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::EresStress
//
//	@doc:
//		Stress test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolBasicTest::EresStress
	(
	CMemoryPoolManager::EAllocType eat
	)
{
	CAutoTimer at("Stress test", true /*fPrint*/);

	// create memory pool
	CAutoMemoryPool amp(CAutoMemoryPool::ElcExc, eat, true /*fThreadSafe*/);
	IMemoryPool *pmp = amp.Pmp();
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	// scope for ATP
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgptsk[GPOS_MEM_TEST_STRESS_TASKS];

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk); i++)
		{
			rgptsk[i] = atp.PtskCreate(AllocateStress, pmp);
		}

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk); i++)
		{
			atp.Schedule(rgptsk[i]);
		}

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgptsk); i++)
		{
			atp.Wait(rgptsk[i]);
		}
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::AllocateStress
//
//	@doc:
//		Repeat loops of random memory allocation and release
//
//---------------------------------------------------------------------------
void *
CMemoryPoolBasicTest::AllocateStress
	(
	void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	IMemoryPool *pmp = static_cast<IMemoryPool*>(pv);

	for (ULONG i = 0; i < GPOS_MEM_TEST_LOOP_SHORT; i++)
	{
		AllocateRandom(pmp);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolBasicTest::AllocateRandom
//
//	@doc:
//		Allocate and release memory randomly
//
//---------------------------------------------------------------------------
void
CMemoryPoolBasicTest::AllocateRandom
	(
	IMemoryPool *pmp
	)
{
	BYTE *rgpb[GPOS_MEM_TEST_LOOP_STRESS];

	ULONG ulSeed = 0;
	const ULONG ulMaxPower = 6;
	const ULONG ulMask = 128;

	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgpb); i++)
	{
		ULONG ulRand = clib::UlRandR(&ulSeed);
		ULONG ulExp = (1 << (ulRand % ulMaxPower));
		ULONG ulFactor = ulRand % ulMask + 1;
		ULONG ulSize = ulFactor * ulExp * ulExp;
		rgpb[i] = GPOS_NEW_ARRAY(pmp, BYTE, ulSize);

		if (0 == i % GPOS_MEM_TEST_CFA)
		{
			GPOS_CHECK_ABORT;
		}
	}

	GPOS_CHECK_ABORT;

	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgpb); i++)
	{
		GPOS_ASSERT(NULL != rgpb[i]);
		GPOS_DELETE_ARRAY(rgpb[i]);

		if (0 == i % GPOS_MEM_TEST_CFA)
		{
			GPOS_CHECK_ABORT;
		}
	}

	GPOS_CHECK_ABORT;
}


// EOF
