//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMemoryPool.cpp
//
//	@doc:
//		Implements functions that are used by the operators "new" and "delete"
//
//	@owner: 
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/error/CAutoTrace.h"
#include "gpos/error/CException.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"


using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		NewImpl
//
//	@doc:
//		Implementation of New that can be used by "operator new" functions
////
//---------------------------------------------------------------------------
void*
NewImpl
	(
	IMemoryPool *pmp,
	SIZE_T cSize,
	const CHAR *szFilename,
	ULONG ulLine,
	IMemoryPool::EAllocationType eat
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(ULONG_MAX >= cSize);
	GPOS_ASSERT_IMP
		(
		(NULL != CMemoryPoolManager::Pmpm()) && (pmp == CMemoryPoolManager::Pmpm()->PmpGlobal()),
		CMemoryPoolManager::Pmpm()->FAllowGlobalNew() &&
		"Use of new operator without target memory pool is prohibited, use New(...) instead"
		);

	ULONG ulAlloc = CMemoryPool::UlAllocSize((ULONG) cSize);
	void *pv = pmp->PvAllocate(ulAlloc, szFilename, ulLine);

	GPOS_OOM_CHECK(pv);

	return dynamic_cast<CMemoryPool*>(pmp)->PvFinalizeAlloc(pv, (ULONG) cSize, eat);
}


//---------------------------------------------------------------------------
//	@function:
//		NewImplNoThrow
//
//	@doc:
//		Implementation of New that returns NULL if an exception is thrown
//
//---------------------------------------------------------------------------
void*
NewImplNoThrow
	(
	IMemoryPool *pmp,
	SIZE_T cSize,
	const CHAR *szFilename,
	ULONG ulLine,
	IMemoryPool::EAllocationType eat
	)
{
	try
	{
		void *pv = NewImpl(pmp, cSize, szFilename, ulLine, eat);
		return pv;
	}
	catch(...)
	{
		return NULL;
	}
}



//---------------------------------------------------------------------------
//	@function:
//		DeleteImpl
//
//	@doc:
//		implementation of Delete that can be used by operator new functions
//
//---------------------------------------------------------------------------
void
DeleteImpl
	(
	void *pv,
	IMemoryPool::EAllocationType eat
	)
{
	// deletion of NULL pointers is legal
	if (NULL == pv)
	{
		return;
	}

	// release allocation
	CMemoryPool::FreeAlloc(pv, eat);
}


//---------------------------------------------------------------------------
//	@function:
//		DeleteImplNoThrow
//
//	@doc:
//		implementation of Delete that does not throw
//
//---------------------------------------------------------------------------
void
DeleteImplNoThrow
	(
	void *pv,
	IMemoryPool::EAllocationType eat
	)
{
	try
	{
		DeleteImpl(pv, eat);
	}
	catch(...)
	{}
}


// EOF

