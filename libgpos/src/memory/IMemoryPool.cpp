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
IMemoryPool::SizeOfAlloc(const void *ptr) {
	return CMemoryPool::SizeOfAlloc(ptr);
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
	SIZE_T size,
	const CHAR *filename,
	ULONG line,
	IMemoryPool::EAllocationType eat
	)
{
	GPOS_ASSERT(gpos::ulong_max >= size);
	GPOS_ASSERT_IMP
		(
		(NULL != CMemoryPoolManager::GetMemoryPoolMgr()) && (this == CMemoryPoolManager::GetMemoryPoolMgr()->GetGlobalMemoryPool()),
		CMemoryPoolManager::GetMemoryPoolMgr()->IsGlobalNewAllowed() &&
		"Use of new operator without target memory pool is prohibited, use New(...) instead"
		);

	ULONG alloc_size = CMemoryPool::GetAllocSize((ULONG) size);
	void *ptr = Allocate(alloc_size, filename, line);

	GPOS_OOM_CHECK(ptr);

	return dynamic_cast<CMemoryPool*>(this)->FinalizeAlloc(ptr, (ULONG) size, eat);
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
	void *ptr,
	EAllocationType eat
	)
{
	// deletion of NULL pointers is legal
	if (NULL == ptr)
	{
		return;
	}

	// release allocation
	CMemoryPool::FreeAlloc(ptr, eat);
}

}  // namespace gpos

// EOF

