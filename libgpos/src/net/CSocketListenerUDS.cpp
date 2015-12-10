//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketListenerUDS.cpp
//
//	@doc:
//		Creates UDS socket to connect with remote host
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/utils.h"
#include "gpos/io/ioutils.h"
#include "gpos/io/CAutoFileDescriptor.h"
#include "gpos/net/netutils.h"
#include "gpos/net/CSocket.h"
#include "gpos/net/CSocketListenerUDS.h"
#include "gpos/string/CStringStatic.h"

#define GPOS_NET_LISTEN_RETRY_US	(1000)

using namespace gpos;
using namespace gpos::netutils;


//---------------------------------------------------------------------------
//	@function:
//		CSocketListenerUDS::CSocketListenerUDS
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSocketListenerUDS::CSocketListenerUDS
	(
	IMemoryPool *pmp,
	const CHAR *szPath
	)
	:
	CSocketListener(pmp),
	m_szPath(szPath),
	m_fListening(false)
{
	GPOS_ASSERT(NULL != pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketListenerUDS::~CSocketListenerUDS
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSocketListenerUDS::~CSocketListenerUDS()
{
	if (m_fListening)
	{
		ioutils::Unlink(m_szPath);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketListenerUDS::StartListener
//
//	@doc:
//		Start listening for new connection requests
//
//---------------------------------------------------------------------------
void
CSocketListenerUDS::StartListener()
{
	GPOS_ASSERT(NULL == m_psocketListen);
	GPOS_ASSERT(!m_fListening);

	INT iSockFd = -1;
	GPOS_CHECK_SIM_NET_ERR(&iSockFd, ISocket(AF_UNIX, SOCK_STREAM, 0));
	CheckNetError(iSockFd);
	InitSocket(iSockFd);

	// use auto object to close socket on exception
	CAutoFileDescriptor afd(iSockFd);

	SSockAddrUn saddrListen;
	saddrListen.sun_family = AF_UNIX;
	CStringStatic str(saddrListen.sun_path, GPOS_ARRAY_SIZE(saddrListen.sun_path));
	str.AppendBuffer(m_szPath);

	ULONG ulLen = GPOS_SZ_LENGTH(saddrListen.sun_path) + GPOS_SIZEOF(saddrListen.sun_family) + 1;
	INT iRes = -1;
	GPOS_CHECK_SIM_NET_ERR
		(
		&iRes,
		IBind(afd.IFileDescr(), reinterpret_cast<SSockAddr*>(&saddrListen), ulLen)
		);
	CheckNetError(iRes);

	// set flag so destructor will unlink the file in case of an exception
	m_fListening = true;

	GPOS_CHECK_SIM_NET_ERR
		(
		&iRes,
		IListen(afd.IFileDescr(), GPOS_NET_LISTEN_BACKLOG)
		);
	CheckNetError(iRes);

	m_psocketListen = GPOS_NEW(m_pmp) CSocket(afd.IFileDescr());
	afd.Detach();
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketListenerUDS::ISocketFdNext
//
//	@doc:
//		Get file descriptor to handle next connection request
//
//---------------------------------------------------------------------------
INT
CSocketListenerUDS::ISocketFdNext()
{
	GPOS_ASSERT(NULL != m_psocketListen);
	GPOS_ASSERT(m_fListening);

	INT iSockFd = -1;
	while (true)
	{
		SSockAddrUn saddrRemote;
		ULONG ulAddrSize = GPOS_SIZEOF(saddrRemote);
		GPOS_CHECK_SIM_NET_ERR
			(
			&iSockFd,
			IAccept(m_psocketListen->ISocketFd(), reinterpret_cast<SSockAddr*>(&saddrRemote), &ulAddrSize)
			);

		if (0 <= iSockFd)
		{
			break;
		}

		if (!FTransientError(errno) && EINPROGRESS != errno)
		{
			GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
		}

		clib::USleep(GPOS_NET_LISTEN_RETRY_US);
		GPOS_CHECK_ABORT;
	}

	GPOS_ASSERT(0 < iSockFd);

	return iSockFd;
}


// EOF
