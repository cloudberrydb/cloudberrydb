//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2004-2015 Pivotal Software, Inc.
//
//	@filename:
//		CWorkerPoolManager.cpp
//
//	@doc:
//		Central scheduler; 
//		* maintains global worker-local-storage
//		* keeps track of all worker pools
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/common/CAutoP.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/error/CFSimulator.h" // for GPOS_FPSIMULATOR
#include "gpos/error/CAutoTrace.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/memory/CMemoryPoolAlloc.h"
#include "gpos/memory/CMemoryPoolInjectFault.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/CMemoryPoolStack.h"
#include "gpos/memory/CMemoryPoolTracker.h"
#include "gpos/memory/CMemoryVisitorPrint.h"
#include "gpos/sync/CAutoSpinlock.h"
#include "gpos/sync/CAutoMutex.h"
#include "gpos/task/CAutoSuspendAbort.h"


using namespace gpos;
using namespace gpos::clib;


// global instance of memory pool manager
CMemoryPoolManager *CMemoryPoolManager::m_pmpm = NULL;

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::CMemoryPoolManager
//
//	@doc:
//		Ctor.
//
//---------------------------------------------------------------------------
CMemoryPoolManager::CMemoryPoolManager
	(
	IMemoryPool *pmpInternal,
	IMemoryPool *pmpBase
	)
	:
	m_pmpBase(pmpBase),
	m_pmpInternal(pmpInternal),
	m_pmpGlobal(NULL),
	m_fAllowGlobalNew(true)
{
	GPOS_ASSERT(NULL != pmpInternal);
	GPOS_ASSERT(NULL != pmpBase);
	GPOS_ASSERT(GPOS_OFFSET(CMemoryPool, m_link) == GPOS_OFFSET(CMemoryPoolAlloc, m_link));
	GPOS_ASSERT(GPOS_OFFSET(CMemoryPool, m_link) == GPOS_OFFSET(CMemoryPoolTracker, m_link));

	m_sht.Init
		(
		m_pmpInternal,
		GPOS_MEMORY_POOL_HT_SIZE,
		GPOS_OFFSET(CMemoryPool, m_link),
		GPOS_OFFSET(CMemoryPool, m_ulpKey),
		&(CMemoryPool::m_ulpInvalid),
		UlHashUlp,
		FEqualUlp
		);

	// create pool used in allocations made using global new operator
	m_pmpGlobal = PmpCreate(EatTracker, true, gpos::ullong_max);
}

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Init
//
//	@doc:
//		Initializer for global memory pool manager
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolManager::EresInit
	(
		void* (*pfnAlloc) (SIZE_T),
		void (*pfnFree) (void*)
	)
{
	GPOS_ASSERT(NULL == CMemoryPoolManager::m_pmpm);

	// raw allocation of memory for internal memory pools
	void *pvAllocBase = PvMalloc(sizeof(CMemoryPoolAlloc));
	void *pvAllocInternal = PvMalloc(sizeof(CMemoryPoolTracker));

	// check if any allocation failed
	if (NULL == pvAllocInternal ||
		NULL == pvAllocBase)
	{
		Free(pvAllocBase);
		Free(pvAllocInternal);

		return GPOS_OOM;
	}

	// create base memory pool
	IMemoryPool *pmpBase = new(pvAllocBase) CMemoryPoolAlloc(pfnAlloc, pfnFree);

	// create internal memory pool
	IMemoryPool *pmpInternal = new(pvAllocInternal) CMemoryPoolTracker
			(
			pmpBase,
			gpos::ullong_max, // ullMaxMemory
			true, // FThreadSafe
			false //fOwnsUnderlyingPmp
			);

	// instantiate manager
	GPOS_TRY
	{
		CMemoryPoolManager::m_pmpm = GPOS_NEW(pmpInternal) CMemoryPoolManager
				(
				pmpInternal,
				pmpBase
				);
	}
	GPOS_CATCH_EX(ex)
	{
		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			Free(pvAllocBase);
			Free(pvAllocInternal);

			return GPOS_OOM;
		}

		return GPOS_FAILED;
	}
	GPOS_CATCH_END;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::PmpCreate		
//
//	@doc:
//		Create new memory pool
//
//---------------------------------------------------------------------------
IMemoryPool *
CMemoryPoolManager::PmpCreate
	(
	EAllocType eat,
	BOOL fThreadSafe,
	ULLONG ullCapacity
	)
{
	IMemoryPool *pmp =
#ifdef GPOS_DEBUG
			PmpCreatePoolStack(eat, ullCapacity, fThreadSafe);
#else
			PmpNew(eat, m_pmpBase, ullCapacity, fThreadSafe, false /*fOwnsUnderlyingPmp*/);
#endif // GPOS_DEBUG

	// accessor scope
	{
		MemoryPoolKeyAccessor shtacc(m_sht, pmp->UlpKey());
		shtacc.Insert(PmpConvert(pmp));
	}

	return pmp;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::PmpNew
//
//	@doc:
//		Create new pool of given type
//
//---------------------------------------------------------------------------
IMemoryPool *
CMemoryPoolManager::PmpNew
	(
	EAllocType eat,
	IMemoryPool *pmpUnderlying,
	ULLONG ullCapacity,
	BOOL fThreadSafe,
	BOOL fOwnsUnderlying
	)
{
	switch (eat)
	{
		case CMemoryPoolManager::EatTracker:
			return GPOS_NEW(m_pmpInternal) CMemoryPoolTracker
						(
						pmpUnderlying,
						ullCapacity,
						fThreadSafe,
						fOwnsUnderlying
						);

		case CMemoryPoolManager::EatStack:
			return GPOS_NEW(m_pmpInternal) CMemoryPoolStack
						(
						pmpUnderlying,
						ullCapacity,
						fThreadSafe,
						fOwnsUnderlying
						);
	}

	GPOS_ASSERT(!"No matching pool type found");
	return NULL;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::PmpCreatePoolStack
//
//	@doc:
//		Surround new pool with tracker pools
//
//---------------------------------------------------------------------------
IMemoryPool *
CMemoryPoolManager::PmpCreatePoolStack
	(
	EAllocType eat,
	ULLONG ullCapacity,
	BOOL fThreadSafe
	)
{
	IMemoryPool *pmpBase = m_pmpBase;
	BOOL fMallocType = (EatTracker == eat);

	// check if tracking and fault injection on internal allocations
	// of memory pools is enabled
	if (NULL != ITask::PtskSelf() && !fMallocType  && GPOS_FTRACE(EtraceTestMemoryPools))
	{
		// put fault injector on top of base pool
		IMemoryPool *pmpFPSimLow = GPOS_NEW(m_pmpInternal) CMemoryPoolInjectFault
				(
				pmpBase,
				false /*fOwnsUnderlying*/
				);

		// put tracker on top of fault injector
		pmpBase = PmpNew
				(
				EatTracker,
				pmpFPSimLow,
				ullCapacity,
				fThreadSafe,
				true /*fOwnsUnderlying*/
				);
	}

	// tracker pool goes on top
	IMemoryPool *pmpRequested = pmpBase;
	if (!fMallocType)
	{
		// put requested pool on top of underlying pool
		pmpRequested = PmpNew
				(
				eat,
				pmpBase,
				ullCapacity,
				fThreadSafe,
				pmpBase != m_pmpBase
				);
	}

	// put fault injector on top of requested pool
	IMemoryPool *pmpFPSim = GPOS_NEW(m_pmpInternal) CMemoryPoolInjectFault
				(
				pmpRequested,
				!fMallocType
				);

	// put tracker on top of the stack
	return PmpNew(EatTracker, pmpFPSim, ullCapacity, fThreadSafe, true /*fOwnsUnderlying*/);
}

#endif // GPOS_DEBUG



//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::DeleteUnregistered
//
//	@doc:
//		Release returned pool
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::DeleteUnregistered
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(pmp != NULL);

#ifdef GPOS_DEBUG
	// accessor's scope
	{
		MemoryPoolKeyAccessor shtacc(m_sht, pmp->UlpKey());

		// make sure that this pool is not in the hash table
		IMemoryPool *pmpFound = shtacc.PtLookup();
		while (NULL != pmpFound)
		{
			GPOS_ASSERT(pmpFound != pmp && "Attempt to delete a registered memory pool");

			pmpFound = shtacc.PtNext(PmpConvert(pmpFound));
		}
	}
#endif // GPOS_DEBUG

	GPOS_DELETE(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Destroy
//
//	@doc:
//		Release returned pool
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::Destroy
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != pmp);

	// accessor scope
	{
		MemoryPoolKeyAccessor shtacc(m_sht, pmp->UlpKey());
		shtacc.Remove(PmpConvert(pmp));
	}

	pmp->TearDown();

	GPOS_DELETE(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::UllTotalAllocatedSize
//
//	@doc:
//		Return total allocated size in bytes
//
//---------------------------------------------------------------------------
ULLONG
CMemoryPoolManager::UllTotalAllocatedSize()
{
	ULLONG ullTotalSize = 0;
	MemoryPoolIter mpiter(m_sht);
	while (mpiter.FAdvance())
	{
		MemoryPoolIterAccessor shtacc(mpiter);
		IMemoryPool *pmp = shtacc.Pt();
		if (NULL != pmp)
		{
			ullTotalSize = ullTotalSize + pmp->UllTotalAllocatedSize();
		}
	}

	return ullTotalSize;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::OsPrint
//
//	@doc:
//		Print contents of all allocated memory pools
//
//---------------------------------------------------------------------------
IOstream &
CMemoryPoolManager::OsPrint
	(
	IOstream &os
	)
{
	os << "Print memory pools: " << std::endl;

	MemoryPoolIter mpiter(m_sht);
	while (mpiter.FAdvance())
	{
		IMemoryPool *pmp = NULL;
		{
			MemoryPoolIterAccessor shtacc(mpiter);
			pmp = shtacc.Pt();
		}

		if (NULL != pmp)
		{
			os << *pmp << std::endl;
		}
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::PrintOverSizedPools
//
//	@doc:
//		Print memory pools with total allocated size above given threshold
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::PrintOverSizedPools
	(
	IMemoryPool *pmpTrace,
	ULLONG ullSizeThreshold // size threshold in bytes
	)
{
	CAutoTraceFlag atfAbort(EtraceSimulateAbort, false);
	CAutoTraceFlag atfOOM(EtraceSimulateOOM, false);
	CAutoTraceFlag atfNet(EtraceSimulateNetError, false);
	CAutoTraceFlag atfIO(EtraceSimulateIOError, false);

	MemoryPoolIter mpiter(m_sht);
	while (mpiter.FAdvance())
	{
		MemoryPoolIterAccessor shtacc(mpiter);
		IMemoryPool *pmp = shtacc.Pt();

		if (NULL != pmp)
		{
			ULLONG ullSize = pmp->UllTotalAllocatedSize();
			if (ullSize > ullSizeThreshold)
			{
				CAutoTrace at(pmpTrace);
				at.Os() << std::endl << "OVERSIZED MEMORY POOL: " << ullSize << " bytes " << std::endl;
			}
		}
	}
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::DestroyMemoryPoolAtShutdown
//
//	@doc:
//		Destroy a memory pool at shutdown
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::DestroyMemoryPoolAtShutdown
	(
	CMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != pmp);

#ifdef GPOS_DEBUG
	gpos::oswcerr << "Leaked " << *pmp << std::endl;
#endif // GPOS_DEBUG

	pmp->TearDown();
	GPOS_DELETE(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Cleanup
//
//	@doc:
//		Clean-up memory pools
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::Cleanup()
{
#ifdef GPOS_DEBUG
	if (0 < m_pmpGlobal->UllTotalAllocatedSize())
	{
		// allocations made by calling global new operator are not deleted
		gpos::oswcerr << "Memory leaks detected"<< std::endl << *m_pmpGlobal << std::endl;
	}
#endif // GPOS_DEBUG

	GPOS_ASSERT(NULL != m_pmpGlobal);
	Destroy(m_pmpGlobal);

	// cleanup left-over memory pools;
	// any such pool means that we have a leak
	m_sht.DestroyEntries(DestroyMemoryPoolAtShutdown);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolManager::Shutdown
//
//	@doc:
//		Delete memory pools and release manager
//
//---------------------------------------------------------------------------
void
CMemoryPoolManager::Shutdown()
{
	// cleanup remaining memory pools
	Cleanup();

	// save off pointers for explicit deletion
	IMemoryPool *pmpInternal = m_pmpInternal;
	IMemoryPool *pmpBase = m_pmpBase;

	GPOS_DELETE(CMemoryPoolManager::m_pmpm);
	CMemoryPoolManager::m_pmpm = NULL;

#ifdef GPOS_DEBUG
	pmpInternal->AssertEmpty(oswcerr);
	pmpBase->AssertEmpty(oswcerr);
#endif // GPOS_DEBUG

	Free(pmpInternal);
	Free(pmpBase);
}

// EOF

