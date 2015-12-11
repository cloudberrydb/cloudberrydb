//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CLoggerComm.cpp
//
//	@doc:
//		Implementation of socket logging using communicator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/comm/CLoggerComm.h"

using namespace gpos;
using namespace gpnaucrates;


//---------------------------------------------------------------------------
//	@function:
//		CLoggerComm::CLoggerComm
//
//	@doc:
//
//---------------------------------------------------------------------------
CLoggerComm::CLoggerComm
	(
	CCommunicator *pcomm
	)
	:
	CLogger(),
	m_pcomm(pcomm)
{
	GPOS_ASSERT(NULL != pcomm);
}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerComm::~CLoggerComm
//
//	@doc:
//
//---------------------------------------------------------------------------
CLoggerComm::~CLoggerComm()
{}


//---------------------------------------------------------------------------
//	@function:
//		CLoggerComm::Write
//
//	@doc:
//		Write string to socket
//
//---------------------------------------------------------------------------
void
CLoggerComm::Write
	(
	const WCHAR *wszLogEntry,
	ULONG ulSev
	)
{
	CCommMessage msg(CCommMessage::EcmtLog, wszLogEntry, ulSev);
	if (m_pcomm->FCheck())
	{
		m_pcomm->SendMsg(&msg);
	}
}


// EOF

