//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryPool.cpp
//
//	@doc:
//		Implementation of abstract interface; 
//		implements helper functions for extraction of allocation
//		header from memory block;
//---------------------------------------------------------------------------

#ifdef GPOS_DEBUG
#include "gpos/error/CFSimulator.h"
#endif // GPOS_DEBUG
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/memory/CMemoryVisitorPrint.h"
#include "gpos/task/ITask.h"


using namespace gpos;


// invalid exception
const ULONG_PTR CMemoryPool::m_ulpInvalid = ULONG_PTR_MAX;


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::CMemoryPool
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMemoryPool::CMemoryPool
	(
	IMemoryPool *pmpUnderlying,
	BOOL fOwnsUnderlying,
	BOOL fThreadSafe
	)
	:
	m_ulRef(0),
	m_ulpKey(0),
	m_pmpUnderlying(pmpUnderlying),
	m_fOwnsUnderlying(fOwnsUnderlying),
	m_fThreadSafe(fThreadSafe)
{
	GPOS_ASSERT_IMP(fOwnsUnderlying, NULL != pmpUnderlying);

	m_ulpKey = reinterpret_cast<ULONG_PTR>(this);
#ifdef GPOS_DEBUG
	m_sd.BackTrace();
#endif // GPOS_DEBUG
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::~CMemoryPool
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMemoryPool::~CMemoryPool()
{
	if (m_fOwnsUnderlying)
	{
		CMemoryPoolManager::Pmpm()->DeleteUnregistered(m_pmpUnderlying);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::PvFinalizeAlloc
//
//	@doc:
//		Set allocation header and footer, return pointer to user data
//
//---------------------------------------------------------------------------
void *
CMemoryPool::PvFinalizeAlloc
	(
	void *pv,
	ULONG ulAlloc,
	EAllocationType eat
	)
{
	GPOS_ASSERT(NULL != pv);

	SAllocHeader *pah = static_cast<SAllocHeader*>(pv);
	pah->m_pmp = this;
	pah->m_ulAlloc = ulAlloc;

	BYTE *pbAllocType = reinterpret_cast<BYTE*>(pah + 1) + ulAlloc;
	*pbAllocType = eat;

	return pah + 1;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::FreeAlloc
//
//	@doc:
//		Return allocation to owning memory pool
//
//---------------------------------------------------------------------------
void
CMemoryPool::FreeAlloc
	(
	void *pv,
	EAllocationType eat
	)
{
	GPOS_ASSERT(pv != NULL);

	SAllocHeader *pah = static_cast<SAllocHeader*>(pv) - 1;
	BYTE *pbAllocType = static_cast<BYTE*>(pv) + pah->m_ulAlloc;
	GPOS_RTL_ASSERT(*pbAllocType == eat);
	pah->m_pmp->Free(pah);

}


ULONG
CMemoryPool::UlSizeOfAlloc
	(
	const void *pv
	)
{
	GPOS_ASSERT(NULL != pv);

	const SAllocHeader *pah = static_cast<const SAllocHeader*>(pv) - 1;
	return pah->m_ulAlloc;
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::Print
//
//	@doc:
//		Walk all objects and print identification
//
//---------------------------------------------------------------------------
IOstream &
CMemoryPool::OsPrint
	(
	IOstream &os
	)
{
	os << "Memory pool: " << this;

	ITask *ptsk = ITask::PtskSelf();
	if (NULL != ptsk && ptsk->FTrace(EtracePrintMemoryLeakStackTrace))
	{
		os << ", stack trace: " << std::endl;
		m_sd.AppendTrace(os, 8 /*ulDepth*/);
	}
	else
	{
		os << std::endl;
	}

	if (FSupportsLiveObjectWalk())
	{
		CMemoryVisitorPrint movpi(os);
		WalkLiveObjects(&movpi);
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::AssertEmpty
//
//	@doc:
//		Static helper function to print out if the given pool is empty;
//		if the pool is not empty and we are compiled with debugging on then
//		an assertion will be thrown;
//
//---------------------------------------------------------------------------
void
CMemoryPool::AssertEmpty
	(
	IOstream &os
	)
{
	if (FSupportsLiveObjectWalk() && NULL != ITask::PtskSelf() &&
	    !GPOS_FTRACE(EtraceDisablePrintMemoryLeak))
	{
		CMemoryVisitorPrint movpi(os);
		WalkLiveObjects(&movpi);

		if (0 != movpi.UllVisits())
		{
			os
				<< "Unfreed memory in memory pool "
				<< (void*)this
				<< ": "
				<< movpi.UllVisits()
				<< " objects leaked"
				<< std::endl;

			GPOS_ASSERT(!"leak detected");
		}
	}
}

#endif // GPOS_DEBUG


// EOF

