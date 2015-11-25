//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		Allocators.h
//
//	@doc:
//		Memory allocation/deallocation operators;
//		these operators are for internal use in gpos only;
//		library linking system must un-export these operators to avoid
//		being wrongly picked-up by other libraries during linking;
//		the functions that do actual memory allocation are NewImplXXX() and
//		DeleteImplXXX(), these functions should be exposed to allow gpos
//		clients to use gpos memory manager
//
//
//	@owner:
//		solimm1
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_Allocators_H
#define GPOS_Allocators_H

#include "gpos/memory/IMemoryPool.h"

using namespace gpos;

//---------------------------------------------------------------------------
// Overloading placement variants of new/delete operators
//---------------------------------------------------------------------------

// placement new operator
void *operator new
	(
	gpos::SIZE_T cSize,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);

// placement array new operator
void *operator new []
	(
	gpos::SIZE_T cSize,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);

//---------------------------------------------------------------------------
// 	Provide two variants for delete:
//		1. Placement delete: triggered when constructors throw
//		2. Explicit delete: to be used for delete calls
//
//	Internally both map to the same delete function;
//---------------------------------------------------------------------------

// throwing explicit singleton delete operator
void operator delete (void *pv) throw();

// placement delete
void operator delete
	(
	void *pv,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);

// throwing explicit array delete operator
void operator delete [] (void *pv) throw();

// placement array delete
void operator delete []
	(
	void *pv,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);

// non-throwing singleton delete operator
void operator delete
	(
	void* pv,
	const gpos::NO_THROW&
	)
	throw();

// non-throwing array delete operator
void operator delete[]
	(
	void* pv,
	const gpos::NO_THROW&
	)
	throw();

#endif // !GPOS_Allocators_H
