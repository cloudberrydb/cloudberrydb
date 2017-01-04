//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		atomic.cpp
//
//	@doc:
//		Implementation of atomic operation primitives
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#ifdef GPOS_SunOS
#include <atomic.h>
#endif

#include "gpos/assert.h"
#include "gpos/utils.h"
#include "gpos/sync/atomic.h"
#include "gpos/sync/CSpinlock.h"

using namespace gpos;

// make sure we can cast the target address in UlpExchangeAdd
GPOS_CPL_ASSERT(sizeof(ULONG_PTR) == sizeof(void*));


//---------------------------------------------------------------------------
//	@function:
//		UlpExchangeAdd
//
//	@doc:
//		Atomic add function; returns original value;
//
//		Note: it is the caller's responsibility to handle overflows;
//
//		Highly system dependent -- add support for individual platforms as
//		necessary
//
//---------------------------------------------------------------------------
ULONG_PTR
gpos::UlpExchangeAdd
	(
	volatile ULONG_PTR *pul,
	INT i
	)
{
	// check for alignment of the address
	GPOS_ASSERT(ALIGNED_32(pul));

#ifdef GPOS_GCC_FETCH_ADD_32
	return __sync_fetch_and_add(pul, i);
#else
	// unknown primitive
	GPOS_CPL_ASSERT(!"no atomic add primitive defined");
#endif
}

//---------------------------------------------------------------------------
//	@function:
//		UlpExchangeAdd
//
//	@doc:
//		Atomic add function; returns original value;
//
//		Note: it is the caller's responsibility to handle overflows;
//
//		Highly system dependent -- add support for individual platforms as
//		necessary
//
//---------------------------------------------------------------------------
ULLONG
gpos::UllExchangeAdd
	(
	volatile ULLONG *pullValue,
	ULLONG ullInc
	)
{
	// check for alignment of the address
	GPOS_ASSERT(ALIGNED_64(pullValue));

#ifdef GPOS_GCC_FETCH_ADD_64
	return __sync_fetch_and_add(pullValue, ullInc);
#else
	// for 32 bit platform we make this function single threaded using a static spin lock
	static CSpinlockOS sLock;

	sLock.Lock();
	ULLONG ullOriginal = *pullValue;
	*pullValue += ullInc;
	sLock.Unlock();
	return ullOriginal;
#endif

	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		FCompareSwap
//
//	@doc:
//		Atomic exchange function for integers;
//		if the stored value is the same as the expected old value, it is
//		replaced with the new one and the function returns true;
//		else, it returns false;
//
//		Highly system dependent -- add support for individual platforms as
//		necessary
//
//---------------------------------------------------------------------------
BOOL
gpos::FCompareSwap
	(
	volatile ULONG *pulDest,
	ULONG ulOld,
	ULONG ulNew
	)
{
	GPOS_ASSERT(NULL != pulDest);
	GPOS_ASSERT(ulOld != ulNew);
	GPOS_ASSERT(ALIGNED_32(pulDest));
	GPOS_ASSERT(ALIGNED_32(&ulOld));
	GPOS_ASSERT(ALIGNED_32(&ulNew));

#ifdef GPOS_GCC_CAS_32
	return __sync_bool_compare_and_swap(pulDest, ulOld, ulNew);
#elif defined(GPOS_SunOS)
	return (ulOld == atomic_cas_32(pulDest, ulOld, ulNew));
#else
	GPOS_CPL_ASSERT(!"no atomic compare-and-swap defined");
#endif

}

//---------------------------------------------------------------------------
//	@function:
//		FCompareSwap
//
//	@doc:
//		Atomic exchange function for long integers;
//		if the stored value is the same as the expected old value, it is
//		replaced with the new one and the function returns true;
//		else, it returns false;
//
//		Highly system dependent -- add support for individual platforms as
//		necessary
//
//---------------------------------------------------------------------------
BOOL
gpos::FCompareSwap
	(
	volatile ULLONG *pullDest,
	ULLONG ullOld,
	ULLONG ullNew
	)
{
	GPOS_ASSERT(NULL != pullDest);
	GPOS_ASSERT(ullOld != ullNew);
	GPOS_ASSERT(ALIGNED_64(pullDest));
	GPOS_ASSERT(ALIGNED_64(&ullOld));
	GPOS_ASSERT(ALIGNED_64(&ullNew));

#ifdef GPOS_GCC_CAS_64
	return __sync_bool_compare_and_swap(pullDest, ullOld, ullNew);
#elif defined(GPOS_SunOS)
	return (ullOld == atomic_cas_64(pullDest, ullOld, ullNew));
#else
	// unknown primitive
	GPOS_CPL_ASSERT(!"no atomic add primitive defined");
#endif

}

#ifdef GPOS_Darwin

//---------------------------------------------------------------------------
//	@function:
//		FCompareSwap
//
//	@doc:
//		Atomic exchange function for integers holding addresses;
//		depending on the architecture, either the 32-bit or the 64-bit
//		version of this function is called;
//
//---------------------------------------------------------------------------
BOOL
gpos::FCompareSwap
	(
	volatile ULONG_PTR *pulpDest,
	ULONG_PTR ulpOld,
	ULONG_PTR ulpNew
	)
{
#ifdef GPOS_32BIT
	return FCompareSwap((ULONG*) pulpDest, (ULONG) ulpOld, (ULONG) ulpNew);
#else
	return FCompareSwap((ULLONG*) pulpDest, (ULLONG) ulpOld, (ULLONG) ulpNew);
#endif // GPOS_32BIT
}

#endif // GPOS_Darwin

//EOF
