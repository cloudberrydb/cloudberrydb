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
	ULONG_PTR ExchangeAddUlongPtrWithInt(volatile ULONG_PTR *ul, INT i);

	// inter-locked add function; returns original value;
	ULLONG ExchangeAddUllongWithUllong(volatile ULLONG *value, ULLONG inc);

	// compare-and-swap function for integers
	BOOL CompareSwap(volatile ULONG *dest_val, ULONG old_val, ULONG new_val);

	// compare-and-swap function for long integers
	BOOL CompareSwap(volatile ULLONG *dest_val, ULLONG old_val, ULLONG new_val);

#ifdef GPOS_Darwin
	// compare-and-swap function for integers holding addresses
	BOOL CompareSwap(volatile ULONG_PTR *dest_val, ULONG_PTR old_val, ULONG_PTR new_val);
#endif // GPOS_Darwin

	// compare-and-swap function for generic pointers
	template<class T>
	BOOL CompareSwap
		(
		volatile T **dest_val,
		T *old_val,
		T *new_val
		)
	{
		return CompareSwap((ULONG_PTR*) dest_val, (ULONG_PTR) old_val, (ULONG_PTR) new_val);
	}
}

#endif // !GPOS_atomic_H


// EOF
