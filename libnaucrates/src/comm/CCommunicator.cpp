//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCommunicator.cpp
//
//	@doc:
//		Implementation of communication handler for message exchange
//		between OPT and QD
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/sync/CAutoMutex.h"

#include "naucrates/comm/CCommunicator.h"

using namespace gpos;
using namespace gpnaucrates;


// initialization of static members
ULLONG CCommunicator::SMessageHeader::m_ullSequence(0);

//---------------------------------------------------------------------------
//	@function:
//		CCommunicator::SendMsg
//
//	@doc:
//		Send message
//
//---------------------------------------------------------------------------
void
CCommunicator::SendMsg
	(
	CCommMessage *pmsg
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	Send(pmsg);
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicator::PmsgReceiveMsg
//
//	@doc:
//		Receive message
//
//---------------------------------------------------------------------------
CCommMessage *
CCommunicator::PmsgReceiveMsg()
{
	CAutoMutex am(m_mutex);
	am.Lock();

	return PmsgReceive();
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicator::PmsgSendRequest
//
//	@doc:
//		Send request and block until response is received
//
//---------------------------------------------------------------------------
CCommMessage *
CCommunicator::PmsgSendRequest
	(
	CCommMessage *pmsg
	)
{
	CAutoMutex am(m_mutex);
	am.Lock();

	Send(pmsg);
	return PmsgReceive();
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicator::Send
//
//	@doc:
//		Send message - unsynchronized
//
//---------------------------------------------------------------------------
void
CCommunicator::Send
	(
	CCommMessage *pmsg
	)
{
	// send header
	SMessageHeader mh(pmsg);
	m_psocket->Send(&mh, GPOS_SIZEOF(mh));

	// send message
	if (0 < mh.m_ulSize)
	{
		m_psocket->Send(const_cast<WCHAR*>(pmsg->Wsz()), mh.m_ulSize * GPOS_SIZEOF(WCHAR));
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicator::PmsgReceive
//
//	@doc:
//		Receive message - unsynchronized
//
//---------------------------------------------------------------------------
CCommMessage *
CCommunicator::PmsgReceive()
{
	// receive header
	SMessageHeader mh;
	(void) m_psocket->UlRecv(&mh, GPOS_SIZEOF(mh), GPOS_SIZEOF(mh));

	GPOS_RTL_ASSERT(CCommMessage::FCheckType(mh.m_ulType));

	// receive message
	if (0 < mh.m_ulSize)
	{
		WCHAR *wsz = GPOS_NEW_ARRAY(m_pmp, WCHAR, mh.m_ulSize);
		const ULONG ulWszSize = mh.m_ulSize * GPOS_SIZEOF(*wsz);
		(void) m_psocket->UlRecv(wsz, ulWszSize, ulWszSize);

		GPOS_CHECK_ABORT;
		GPOS_ASSERT(mh.m_ulSize == GPOS_WSZ_LENGTH(wsz) + 1);

		return GPOS_NEW(m_pmp) CCommMessage
				(
				(CCommMessage::ECommMessageType) mh.m_ulType,
				wsz,
				mh.m_ullInfo
				);
	}

	return NULL;
}


// EOF

