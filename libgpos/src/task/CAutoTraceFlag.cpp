//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CAutoTraceFlag.cpp
//
//	@doc:
//		Auto object to toggle TF in scope
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/task/CAutoTraceFlag.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CAutoTraceFlag::CAutoTraceFlag
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CAutoTraceFlag::CAutoTraceFlag
	(
	ULONG ulTrace,
	BOOL fVal
	)
	:
	m_ulTrace(ulTrace),
	m_fOrig(false)
{
	GPOS_ASSERT(NULL != ITask::PtskSelf());
	m_fOrig = ITask::PtskSelf()->FTrace(m_ulTrace, fVal);
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoTraceFlag::~CAutoTraceFlag
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CAutoTraceFlag::~CAutoTraceFlag()
{
	GPOS_ASSERT(NULL != ITask::PtskSelf());
		
	// reset original value
	ITask::PtskSelf()->FTrace(m_ulTrace, m_fOrig);
}


// EOF
