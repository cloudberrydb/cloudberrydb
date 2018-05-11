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
const CWorkerId CWorkerId::m_wid_invalid (false /*fvalid*/);

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
	BOOL valid
	)
{
	if (valid)
	{
		SetThreadToCurrent();
	}
	else
	{
		SetThreadToInvalid();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::Equals
//
//	@doc:
//		comparison of worker ids
//
//---------------------------------------------------------------------------
BOOL
CWorkerId::Equals
	(
	const CWorkerId &wid
	)
	const
{
	// compare on thread id first; if equal then there cannot be a race on the
	// valid flags

	return pthread::Equal(wid.m_pthread, m_pthread);
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
CWorkerId::SetThreadToCurrent()
{
	m_pthread = pthread::Self();
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
CWorkerId::SetThreadToInvalid()
{
	// reset bytes to 0
	clib::Memset(this, 0, sizeof(CWorkerId));
}


//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::HashValue
//
//	@doc:
//		Primitive hash function
//
//---------------------------------------------------------------------------
ULONG
CWorkerId::HashValue
	(
	const CWorkerId &wid
	)
{
	// don't compute hash value for invalid id
	GPOS_ASSERT(wid.IsValid() && "Invalid worker id.");

	return gpos::HashValue<PTHREAD_T>(&wid.m_pthread);
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CWorkerId::IsValid
//
//	@doc:
//		Check if worker id is valid
//
//---------------------------------------------------------------------------
BOOL
CWorkerId::IsValid() const
{
	return !Equals(m_wid_invalid);
}

#endif // GPOS_DEBUG

// EOF

