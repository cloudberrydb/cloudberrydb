//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CAutoExceptionStack.cpp
//
//	@doc:
//		Implementation of auto object for saving and restoring exception stack
//
//	@owner:
//		elhela
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/error/CAutoExceptionStack.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CAutoExceptionStack::CAutoExceptionStack
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CAutoExceptionStack::CAutoExceptionStack
	(
	void **ppvExceptionStack,
	void **ppvErrorContextStack
	)
	:
	m_ppvExceptionStack(ppvExceptionStack),
	m_pvExceptionStack(*ppvExceptionStack),
	m_ppvErrorContextStack(ppvErrorContextStack),
	m_pvErrorContextStack(*ppvErrorContextStack)
{}

//---------------------------------------------------------------------------
//	@function:
//		CAutoExceptionStack::~CAutoExceptionStack
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CAutoExceptionStack::~CAutoExceptionStack()
{
	*m_ppvExceptionStack = m_pvExceptionStack;
	*m_ppvErrorContextStack = m_pvErrorContextStack;
}

//---------------------------------------------------------------------------
//	@function:
//		CAutoExceptionStack::SetLocalJmp
//
//	@doc:
//		Set the exception stack to the given address
//
//---------------------------------------------------------------------------
void
CAutoExceptionStack::SetLocalJmp
	(
	void *pvLocalJmp
	)
{
	*m_ppvExceptionStack = pvLocalJmp;
}

// EOF

