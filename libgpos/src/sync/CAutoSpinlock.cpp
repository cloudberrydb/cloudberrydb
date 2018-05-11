//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CAutoSpinlock.cpp
//
//	@doc:
//		Wrapper around spinlock base class; releases spinlock in destructor
//		if acquired previously;
//---------------------------------------------------------------------------


#include "gpos/sync/CAutoSpinlock.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CAutoSpinlock::~CAutoSpinlock
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CAutoSpinlock::~CAutoSpinlock()
{
	if (m_locked)
	{
		m_lock.Unlock();
	}
}

// EOF

