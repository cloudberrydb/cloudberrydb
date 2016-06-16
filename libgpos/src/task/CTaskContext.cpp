//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CTaskContext.cpp
//
//	@doc:
//		Task context implementation
//---------------------------------------------------------------------------

#include "gpos/common/CAutoRef.h"

#include "gpos/error/CLoggerStream.h"
#include "gpos/task/CTaskContext.h"


using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::CTaskContext
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CTaskContext::CTaskContext
	(
	IMemoryPool *pmp
	)
	:
	m_pbs(NULL),
	m_plogOut(&CLoggerStream::m_plogStdOut),
	m_plogErr(&CLoggerStream::m_plogStdErr),
	m_eloc(ElocEnUS_Utf8)
{
	m_pbs = GPOS_NEW(pmp) CBitSet(pmp, EtraceSentinel);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::CTaskContext
//
//	@doc:
//		used to inherit parent task's context
//
//---------------------------------------------------------------------------
CTaskContext::CTaskContext
	(
	IMemoryPool *pmp,
	const CTaskContext &tskctxt
	)
	:
	m_pbs(NULL),
	m_plogOut(tskctxt.PlogOut()),
	m_plogErr(tskctxt.PlogErr()),
	m_eloc(tskctxt.Eloc())
{
	// allocate bitset and union separately to guard against leaks under OOM
	CAutoRef<CBitSet> a_pbs;
	
	a_pbs = GPOS_NEW(pmp) CBitSet(pmp);
	a_pbs->Union(tskctxt.m_pbs);
	
	m_pbs = a_pbs.PtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::~CTaskContext
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CTaskContext::~CTaskContext()
{
    CRefCount::SafeRelease(m_pbs);
}


//---------------------------------------------------------------------------
//	@function:
//		CTaskContext::FTrace
//
//	@doc:
//		Set trace flag; return original setting
//
//---------------------------------------------------------------------------
BOOL
CTaskContext::FTrace
	(
	ULONG ulTrace,
	BOOL fVal
	)
{
	if(fVal)
	{
		return m_pbs->FExchangeSet(ulTrace);
	}
	else
	{
		return m_pbs->FExchangeClear(ulTrace);
	}
}

// EOF

