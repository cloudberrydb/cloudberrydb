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
// Overloading placement variants of new operators. Used to allocate
// arbitrary objects from an IMemoryPool. This does not affect the ordinary
// built-in 'new', and is used only when placement-new is invoked with the
// specific type signature defined below.
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
// Corresponding placement variants of delete operators. Note that, for delete
// statements in general, the compiler can not determine which overloaded
// version of new was used to allocate memory originally, and the global
// non-placement version is used. These placement versions of 'delete' are used
// only when a constructor throws an exception, and the version of 'new' is
// known to be one of the two declared above.
//---------------------------------------------------------------------------

// placement delete
void operator delete
	(
	void *pv,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);

// placement array delete
void operator delete []
	(
	void *pv,
	gpos::IMemoryPool *pmp,
	const gpos::CHAR *szFilename,
	gpos::ULONG cLine
	);

#endif // !GPOS_Allocators_H
