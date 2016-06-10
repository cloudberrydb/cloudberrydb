//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketConnectorUDS.cpp
//
//	@doc:
//		Creates UDS socket to connect with remote host
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/utils.h"
#include "gpos/io/CAutoFileDescriptor.h"
#include "gpos/net/netutils.h"
#include "gpos/net/CSocketConnectorUDS.h"
#include "gpos/string/CStringStatic.h"

using namespace gpos;
using namespace gpos::netutils;

#define GPOS_NET_CONNECT_RETRY_US	(1000)

//---------------------------------------------------------------------------
//	@function:
//		CSocketConnectorUDS::CSocketConnectorUDS
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSocketConnectorUDS::CSocketConnectorUDS
	(
	IMemoryPool *pmp,
	const CHAR *szPath
	)
	:
	CSocketConnector(pmp),
	m_szPath(szPath)
{
	GPOS_ASSERT(NULL != szPath);
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketConnectorUDS::~CSocketConnectorUDS
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSocketConnectorUDS::~CSocketConnectorUDS()
{}


//---------------------------------------------------------------------------
//	@function:
//		CSocketConnectorUDS::Connect
//
//	@doc:
//		Connect to remote host
//
//---------------------------------------------------------------------------
void
CSocketConnectorUDS::Connect()
{
	GPOS_ASSERT(NULL == m_psocket);

	INT iSockFd = -1;
	GPOS_CHECK_SIM_NET_ERR(&iSockFd, ISocket(AF_UNIX, SOCK_STREAM, 0));
	CheckNetError(iSockFd);
	InitSocket(iSockFd);

	// use auto object to close socket on exception
	CAutoFileDescriptor afd(iSockFd);

	SSockAddrUn saddrRemote;
	saddrRemote.sun_family = AF_UNIX;
	CStringStatic str(saddrRemote.sun_path, GPOS_ARRAY_SIZE(saddrRemote.sun_path));
	str.AppendBuffer(m_szPath);
	ULONG ulLen = GPOS_SZ_LENGTH(saddrRemote.sun_path) + GPOS_SIZEOF(saddrRemote.sun_family) + 1;

	INT iRes = -1;
	while (0 != iRes)
	{
		// attempt to connect
		GPOS_CHECK_SIM_NET_ERR
			(
			&iRes,
			IConnect(afd.IFileDescr(), reinterpret_cast<SSockAddr*>(&saddrRemote), ulLen)
			);

		// check for transient error or if the remote host is not listening yet
		if (0 == iRes && FConnected(afd.IFileDescr()))
		{
			break;
		}

		if (!FTransientError(errno) &&
		    EINPROGRESS != errno && ENOENT != errno && ECONNREFUSED != errno)
		{
			GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
		}

		clib::USleep(GPOS_NET_CONNECT_RETRY_US);
		GPOS_CHECK_ABORT;
	}

	m_psocket = GPOS_NEW(m_pmp) CSocket(afd.IFileDescr());
	afd.Detach();
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketConnectorUDS::FConnected
//
//	@doc:
//		Check if connection is established
//
//---------------------------------------------------------------------------

BOOL
CSocketConnectorUDS::FConnected
	(
	INT iSocketFd
	)
{
	Poll(iSocketFd, EptOut);
	INT iError = IPollErrno(iSocketFd);
	if (0 == iError)
	{
		return true;
	}

	if (!FTransientError(iError))
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, iError);
	}

	return false;
}


// EOF
