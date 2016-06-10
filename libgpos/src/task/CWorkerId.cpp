//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CWorkerId.cpp
//
//	@doc:
//		Basic worker identification
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/utils.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/common/pthreadwrapper.h"
#include "gpos/task/CWorkerId.h"

using namespace gpos;


// invalid worker id
const CWorkerId CWorkerId::m_widInvalid (false /*fvalid*/);

//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::CWorkerId
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CWorkerId::CWorkerId
	(
	BOOL fValid
	)
{
	if (fValid)
	{
		Current();
	}
	else
	{
		Invalid();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::FEqual
//
//	@doc:
//		comparison of worker ids
//
//---------------------------------------------------------------------------
BOOL
CWorkerId::FEqual
	(
	const CWorkerId &wid
	)
	const
{
	// compare on thread id first; if equal then there cannot be a race on the
	// valid flags

	return pthread::FPthreadEqual(wid.m_pthread, m_pthread);
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::Set
//
//	@doc:
//		Set worker id to current thread
//
//---------------------------------------------------------------------------
void
CWorkerId::Current()
{
	m_pthread = pthread::PthrdtPthreadSelf();
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::Reset
//
//	@doc:
//		Reset to invalid id
//
//---------------------------------------------------------------------------
void
CWorkerId::Invalid()
{
	// reset bytes to 0
	clib::PvMemSet(this, 0, sizeof(CWorkerId));
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::UlHash
//
//	@doc:
//		Primitive hash function
//
//---------------------------------------------------------------------------
ULONG
CWorkerId::UlHash
	(
	const CWorkerId &wid
	)
{
	// don't compute hash value for invalid id
	GPOS_ASSERT(wid.FValid() && "Invalid worker id.");

	return gpos::UlHash<PTHREAD_T>(&wid.m_pthread);
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::FValid
//
//	@doc:
//		Check if worker id is valid
//
//---------------------------------------------------------------------------
BOOL
CWorkerId::FValid() const
{
	return !FEqual(m_widInvalid);
}

#endif // GPOS_DEBUG

// EOF

