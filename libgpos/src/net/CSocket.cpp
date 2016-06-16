//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocket.cpp
//
//	@doc:
//		Socket abstraction
//---------------------------------------------------------------------------


#include "gpos/base.h"
#include "gpos/io/ioutils.h"
#include "gpos/net/netutils.h"
#include "gpos/net/CSocket.h"

using namespace gpos;
using namespace gpos::netutils;


//---------------------------------------------------------------------------
//	@function:
//		CSocket::CSocket
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSocket::CSocket
	(
	INT iFileDescr
	)
	:
	m_iSocketFd(iFileDescr),
	m_psl(NULL)
{
	GPOS_ASSERT(0 < iFileDescr);
}


//---------------------------------------------------------------------------
//	@function:
//		CSocket::CSocket
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSocket::CSocket
	(
	INT iFileDescr,
	CSocketListener *psl
	)
	:
	m_iSocketFd(iFileDescr),
	m_psl(psl)
{
	GPOS_ASSERT(0 < iFileDescr);
	GPOS_ASSERT(NULL != psl);
}


//---------------------------------------------------------------------------
//	@function:
//		CSocket::~CSocket
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSocket::~CSocket()
{
	BOOL fClosed = false;
	while (!fClosed)
	{
		INT iRes = ioutils::IClose(m_iSocketFd);

		// check for error
		if (0 != iRes)
		{
			GPOS_ASSERT(EINTR == errno || EIO == errno);

			// in case an interrupt was received we retry
			if (EINTR == errno)
			{
				continue;
			}
		}

		fClosed = true;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSocket::Send
//
//	@doc:
//		Send data to remote host
//
//---------------------------------------------------------------------------
void
CSocket::Send
	(
	const void *pvBuffer,
	ULONG ulSize
	)
{
	ULONG ulSent = 0;
	while (ulSent < ulSize)
	{
		Poll(m_iSocketFd, EptOut);

		INT iFlags =
#ifdef GPOS_Linux
		MSG_NOSIGNAL;
#else
		0;
#endif // GPOS_Linux

		INT iRes = -1;
		GPOS_CHECK_SIM_NET_ERR
			(
			&iRes,
			ISend(m_iSocketFd, static_cast<const BYTE*>(pvBuffer) + ulSent, ulSize - ulSent, iFlags)
			);

		if (0 > iRes)
		{
			if (!FTransientError(errno))
			{
				GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
			}
		}
		else
		{
			ulSent += iRes;
		}

		GPOS_CHECK_ABORT;
	}

	GPOS_ASSERT(ulSent == ulSent);
}


//---------------------------------------------------------------------------
//	@function:
//		CSocket::UlRecv
//
//	@doc:
//		Receive data from remote host; returns size of received message;
//
//---------------------------------------------------------------------------
ULONG
CSocket::UlRecv
	(
	void *pvBuffer,
	ULONG ulMaxSize,
	ULONG ulExpected
	)
{
	ULONG ulRecv = 0;
	while (ulRecv == 0 || ulRecv < ulExpected)
	{
		Poll(m_iSocketFd, EptIn);

		INT iRes = -1;
		GPOS_CHECK_SIM_NET_ERR
			(
			&iRes,
			IRecv(m_iSocketFd, static_cast<BYTE*>(pvBuffer) + ulRecv, ulMaxSize - ulRecv, 0)
			);

		// when 0 is returned, the peer has shut down the connection
		if (0 == iRes)
		{
			GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, ECONNRESET);
		}

		if (0 > iRes)
		{
			if (!FTransientError(errno))
			{
				GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
			}
		}
		else
		{
			ulRecv += iRes;
		}

		GPOS_CHECK_ABORT;
	}

	GPOS_ASSERT(ulRecv <= ulMaxSize);
	GPOS_ASSERT_IMP(0 < ulExpected, ulRecv == ulExpected);

	return ulRecv;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocket::FCheck
//
//	@doc:
//		Check if socket is operational
//
//---------------------------------------------------------------------------
BOOL
CSocket::FCheck() const
{
	BYTE rgb[1];
	return (0 != IRecv(m_iSocketFd, rgb, GPOS_SIZEOF(rgb), MSG_PEEK | MSG_DONTWAIT));
}


// EOF
