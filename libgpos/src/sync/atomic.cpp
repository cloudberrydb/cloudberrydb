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

// make sure we can cast the target address in ExchangeAddUlongPtrWithInt
GPOS_CPL_ASSERT(sizeof(ULONG_PTR) == sizeof(void*));


//---------------------------------------------------------------------------
//	@function:
//		ExchangeAddUlongPtrWithInt
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
gpos::ExchangeAddUlongPtrWithInt
	(
	volatile ULONG_PTR *ul,
	INT i
	)
{
	// check for alignment of the address
	GPOS_ASSERT(ALIGNED_32(ul));

#ifdef GPOS_GCC_FETCH_ADD_32
	return __sync_fetch_and_add(ul, i);
#else
	// unknown primitive
	GPOS_CPL_ASSERT(!"no atomic add primitive defined");
#endif
}

//---------------------------------------------------------------------------
//	@function:
//		ExchangeAddUllongWithUllong
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
gpos::ExchangeAddUllongWithUllong
	(
	volatile ULLONG *value,
	ULLONG inc
	)
{
	// check for alignment of the address
	GPOS_ASSERT(ALIGNED_64(value));

#ifdef GPOS_GCC_FETCH_ADD_64
	return __sync_fetch_and_add(value, inc);
#else
	// for 32 bit platform we make this function single threaded using a static spin lock
	static CSpinlockOS sLock;

	sLock.Lock();
	ULLONG original = *value;
	*value += inc;
	sLock.Unlock();
	return original;
#endif

	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CompareSwap
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
gpos::CompareSwap
	(
	volatile ULONG *dest_val,
	ULONG old_val,
	ULONG new_val
	)
{
	GPOS_ASSERT(NULL != dest_val);
	GPOS_ASSERT(old_val != new_val);
	GPOS_ASSERT(ALIGNED_32(dest_val));
	GPOS_ASSERT(ALIGNED_32(&old_val));
	GPOS_ASSERT(ALIGNED_32(&new_val));

#ifdef GPOS_GCC_CAS_32
	return __sync_bool_compare_and_swap(dest_val, old_val, new_val);
#elif defined(GPOS_SunOS)
	return (old_val == atomic_cas_32(dest_val, old_val, new_val));
#else
	GPOS_CPL_ASSERT(!"no atomic compare-and-swap defined");
#endif

}

//---------------------------------------------------------------------------
//	@function:
//		CompareSwap
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
gpos::CompareSwap
	(
	volatile ULLONG *dest_val,
	ULLONG old_val,
	ULLONG new_val
	)
{
	GPOS_ASSERT(NULL != dest_val);
	GPOS_ASSERT(old_val != new_val);
	GPOS_ASSERT(ALIGNED_64(dest_val));
	GPOS_ASSERT(ALIGNED_64(&old_val));
	GPOS_ASSERT(ALIGNED_64(&new_val));

#ifdef GPOS_GCC_CAS_64
	return __sync_bool_compare_and_swap(dest_val, old_val, new_val);
#elif defined(GPOS_SunOS)
	return (old_val == atomic_cas_64(dest_val, old_val, new_val));
#else
	// unknown primitive
	GPOS_CPL_ASSERT(!"no atomic add primitive defined");
#endif

}

#ifdef GPOS_Darwin

//---------------------------------------------------------------------------
//	@function:
//		CompareSwap
//
//	@doc:
//		Atomic exchange function for integers holding addresses;
//		depending on the architecture, either the 32-bit or the 64-bit
//		version of this function is called;
//
//---------------------------------------------------------------------------
BOOL
gpos::CompareSwap
	(
	volatile ULONG_PTR *dest_val,
	ULONG_PTR old_val,
	ULONG_PTR new_val
	)
{
#ifdef GPOS_32BIT
	return CompareSwap((ULONG*) dest_val, (ULONG) old_val, (ULONG) new_val);
#else
	return CompareSwap((ULLONG*) dest_val, (ULLONG) old_val, (ULLONG) new_val);
#endif // GPOS_32BIT
}

#endif // GPOS_Darwin

//EOF
