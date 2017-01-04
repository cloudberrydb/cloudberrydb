//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CAutoSuspendAbort.cpp
//
//	@doc:
//		Auto suspend abort object
//---------------------------------------------------------------------------

#include <stddef.h>

#include "gpos/base.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CTask.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoSuspendAbort::CAutoSuspendAbort
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CAutoSuspendAbort::CAutoSuspendAbort()
{
	m_ptsk = CTask::PtskSelf();
	
	if (NULL != m_ptsk)
	{
		m_ptsk->SuspendAbort();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoSuspendAbort::~CAutoSuspendAbort
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CAutoSuspendAbort::~CAutoSuspendAbort()
{
	if (NULL != m_ptsk)
	{
		m_ptsk->ResumeAbort();
	}
}


// EOF
