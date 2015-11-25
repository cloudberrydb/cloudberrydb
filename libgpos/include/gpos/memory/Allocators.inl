//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		Allocators.inl
//
//	@doc:
//		Implementation of GPOS memory allocators
//
//	@owner:
//		solimm1
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/memory/IMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		new
//
//	@doc:
//		overloaded placement new operator
//
//---------------------------------------------------------------------------
void *
operator new
	(
	SIZE_T cSize,
	IMemoryPool *pmp,
	const CHAR *szFilename,
	ULONG ulLine
	)
{
	return NewImpl(pmp, cSize, szFilename, ulLine, IMemoryPool::EatSingleton);
}


//---------------------------------------------------------------------------
//	@function:
//		new[]
//
//	@doc:
//		Overload for array allocation; raises OOM exception if
//		unable to allocate
//
//---------------------------------------------------------------------------
void *
operator new []
	(
	SIZE_T cSize,
	IMemoryPool *pmp,
	const CHAR *szFilename,
	ULONG ulLine
	)
{
	return NewImpl(pmp, cSize, szFilename, ulLine, IMemoryPool::EatArray);
}


//---------------------------------------------------------------------------
//	@function:
//		delete
//
//	@doc:
//		Overload for singleton deletion
//
//---------------------------------------------------------------------------
void
operator delete
	(
	void *pv
	)
	throw()
{
	DeleteImpl(pv, IMemoryPool::EatSingleton);
}


//---------------------------------------------------------------------------
//	@function:
//		delete
//
//	@doc:
//		Placement delete; only used if constructor throws
//
//---------------------------------------------------------------------------
void
operator delete
	(
	void *pv,
	IMemoryPool *, // pmp,
	const CHAR *, // szFilename,
	ULONG // ulLine
	)
{
	DeleteImpl(pv, IMemoryPool::EatSingleton);
}


//---------------------------------------------------------------------------
//	@function:
//		delete []
//
//	@doc:
//		Overload for array deletion
//
//---------------------------------------------------------------------------
void
operator delete []
	(
	void *pv
	)
	throw()
{
	DeleteImpl(pv, IMemoryPool::EatArray);
}


//---------------------------------------------------------------------------
//	@function:
//		delete []
//
//	@doc:
//		Placement delete []; only used if constructor throws
//
//---------------------------------------------------------------------------
void
operator delete []
	(
	void *pv,
	IMemoryPool *, // pmp,
	const CHAR *, // szFilename,
	ULONG // ulLine
	)
{
	DeleteImpl(pv, IMemoryPool::EatArray);
}


//---------------------------------------------------------------------------
//	@function:
//		delete
//
//	@doc:
//		Non-throwing singleton delete operator
//
//---------------------------------------------------------------------------
void
operator delete
	(
	void* pv,
	const gpos::NO_THROW&
	)
	throw()
{
	DeleteImplNoThrow(pv, IMemoryPool::EatSingleton);
}


//---------------------------------------------------------------------------
//	@function:
//		delete
//
//	@doc:
//		Non-throwing array delete operator
//
//---------------------------------------------------------------------------
void
operator delete []
	(
	void* pv,
	const gpos::NO_THROW&
	)
	throw()
{
	DeleteImplNoThrow(pv, IMemoryPool::EatArray);
}

