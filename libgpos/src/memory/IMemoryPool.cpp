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
//---------------------------------------------------------------------------

#include "gpos/error/CAutoTrace.h"
#include "gpos/error/CException.h"
#include "gpos/memory/CMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"


namespace gpos
{

ULONG
IMemoryPool::UlSizeOfAlloc(const void *pv) {
	return CMemoryPool::UlSizeOfAlloc(pv);
}

//---------------------------------------------------------------------------
//	@function:
//		IMemoryPool::NewImpl
//
//	@doc:
//		Implementation of New that can be used by "operator new" functions
////
//---------------------------------------------------------------------------
void*
IMemoryPool::NewImpl
	(
	SIZE_T cSize,
	const CHAR *szFilename,
	ULONG ulLine,
	IMemoryPool::EAllocationType eat
	)
{
	GPOS_ASSERT(gpos::ulong_max >= cSize);
	GPOS_ASSERT_IMP
		(
		(NULL != CMemoryPoolManager::Pmpm()) && (this == CMemoryPoolManager::Pmpm()->PmpGlobal()),
		CMemoryPoolManager::Pmpm()->FAllowGlobalNew() &&
		"Use of new operator without target memory pool is prohibited, use New(...) instead"
		);

	ULONG ulAlloc = CMemoryPool::UlAllocSize((ULONG) cSize);
	void *pv = PvAllocate(ulAlloc, szFilename, ulLine);

	GPOS_OOM_CHECK(pv);

	return dynamic_cast<CMemoryPool*>(this)->PvFinalizeAlloc(pv, (ULONG) cSize, eat);
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
IMemoryPool::DeleteImpl
	(
	void *pv,
	EAllocationType eat
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

}  // namespace gpos

// EOF

