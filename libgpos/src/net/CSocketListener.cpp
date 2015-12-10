//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketListener.cpp
//
//	@doc:
//		Partial implementation of socket listener
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "gpos/base.h"
#include "gpos/io/CAutoFileDescriptor.h"
#include "gpos/net/CSocketListener.h"
#include "gpos/sync/CAutoSpinlock.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CSocketListener::CSocketListener
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSocketListener::CSocketListener
	(
	IMemoryPool *pmp
	)
	:
	m_psocketListen(NULL),
	m_pmp(pmp)
{
	GPOS_ASSERT(NULL != pmp);

	m_listSockets.Init(GPOS_OFFSET(CSocket, m_link));
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketListener::~CSocketListener
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSocketListener::~CSocketListener()
{
	while (!m_listSockets.FEmpty())
	{
		CSocket *psocket = m_listSockets.RemoveHead();
		GPOS_DELETE(psocket);
	}

	GPOS_DELETE(m_psocketListen);
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketListener::Psocket
//
//	@doc:
//		Create new socket
//
//---------------------------------------------------------------------------
CSocket *
CSocketListener::PsocketNext()
{
	CAutoFileDescriptor afd(ISocketFdNext());
	CSocket *psocket = GPOS_NEW(m_pmp) CSocket(afd.IFileDescr(), this);

	// scope for spinlock locking
	{
		CAutoSpinlock asl(m_slock);
		asl.Lock();
		m_listSockets.Append(psocket);
	}

	afd.Detach();
	return psocket;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketListener::Release
//
//	@doc:
//		Release socket
//
//---------------------------------------------------------------------------
void
CSocketListener::Release
	(
	CSocket *psocket
	)
{
	// scope for spinlock locking
	{
		CAutoSpinlock asl(m_slock);
		asl.Lock();

		m_listSockets.Remove(psocket);
	}

	GPOS_DELETE(psocket);
}


// EOF
