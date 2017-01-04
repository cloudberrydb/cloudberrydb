//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		atomic.h
//
//	@doc:
//		Atomic operation primitives
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_atomic_H
#define GPOS_atomic_H

#include "gpos/types.h"

// number of spins before acquisition before backing off
#define GPOS_SPIN_ATTEMPTS	10000U

// backoff wait period in microseconds
#define GPOS_SPIN_BACKOFF	1000U

namespace gpos
{
	// inter-locked add function; returns original value;
	ULONG_PTR UlpExchangeAdd(volatile ULONG_PTR *pul, INT i);

	// inter-locked add function; returns original value;
	ULLONG UllExchangeAdd(volatile ULLONG *pullValue, ULLONG ullInc);

	// compare-and-swap function for integers
	BOOL FCompareSwap(volatile ULONG *pulDest, ULONG ulOld, ULONG ulNew);

	// compare-and-swap function for long integers
	BOOL FCompareSwap(volatile ULLONG *pullDest, ULLONG ullOld, ULLONG ullNew);

#ifdef GPOS_Darwin
	// compare-and-swap function for integers holding addresses
	BOOL FCompareSwap(volatile ULONG_PTR *pulpDest, ULONG_PTR ulpOld, ULONG_PTR ulpNew);
#endif // GPOS_Darwin

	// compare-and-swap function for generic pointers
	template<class T>
	BOOL FCompareSwap
		(
		volatile T **pptDest,
		T *ptOld,
		T *ptNew
		)
	{
		return FCompareSwap((ULONG_PTR*) pptDest, (ULONG_PTR) ptOld, (ULONG_PTR) ptNew);
	}
}

#endif // !GPOS_atomic_H


// EOF
