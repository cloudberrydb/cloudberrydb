//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		netutils.cpp
//
//	@doc:
//		Implementation of networking utilities
//---------------------------------------------------------------------------

#include <unistd.h>
#include <fcntl.h>

#include "gpos/base.h"
#include "gpos/error/CFSimulator.h"
#include "gpos/error/CLogger.h"
#include "gpos/net/netutils.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/task/CTaskContext.h"

#define GPOS_NET_POLL_TIMEOUT_MS	(100)

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		netutils::ISocket
//
//	@doc:
//		Create socket
//
//---------------------------------------------------------------------------
INT
gpos::netutils::ISocket
	(
	INT iSocketFamily,
	INT iSocketType,
	INT iProtocol
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = socket(iSocketFamily, iSocketType, iProtocol);
	GPOS_ASSERT_IMP(0 > iRes, EINVAL != errno && EACCES != errno && EAFNOSUPPORT != errno);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::IBind
//
//	@doc:
//		Bind socket to address
//
//---------------------------------------------------------------------------
INT
gpos::netutils::IBind
	(
	INT iSocketFd,
	const SSockAddr *psa,
	SOCKET_LEN slAddressLen
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = bind(iSocketFd, psa, slAddressLen);
	GPOS_ASSERT_IMP
		(
		0 > iRes,
		EACCES != errno && EBADF != errno && EINVAL != errno && ENOTSOCK != errno
		);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::IConnect
//
//	@doc:
//		Connect to a remote host
//
//---------------------------------------------------------------------------
INT
gpos::netutils::IConnect
	(
	INT iSocketFd,
	const SSockAddr *psa,
	SOCKET_LEN slAddressLen
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = connect(iSocketFd, psa, slAddressLen);
	GPOS_ASSERT_IMP
		(
		0 > iRes,
		EINVAL != errno && EBADF != errno && EAFNOSUPPORT != errno && EISCONN != errno
		);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::IListen
//
//	@doc:
//		Listen for new connection requests
//
//---------------------------------------------------------------------------
INT
gpos::netutils::IListen
	(
	INT iSocketFd,
	INT iBacklog
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = listen(iSocketFd, iBacklog);
	GPOS_ASSERT_IMP
		(
		0 > iRes,
		EOPNOTSUPP != errno && EBADF != errno && ENOTSOCK != errno
		);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::IAccept
//
//	@doc:
//		Accept a new connection
//
//---------------------------------------------------------------------------
INT
gpos::netutils::IAccept
	(
	INT iSocketFd,
	SSockAddr *psa,
	SOCKET_LEN *pslAddressLen
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = accept(iSocketFd, psa, pslAddressLen);
	GPOS_ASSERT_IMP
		(
		0 > iRes,
		EINVAL != errno && EOPNOTSUPP != errno && EBADF != errno && ENOTSOCK != errno
		);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::IPoll
//
//	@doc:
//		Poll connection until it is ready to send or receive
//
//---------------------------------------------------------------------------
INT
gpos::netutils::IPoll
	(
	SPollFd *ppfd,
	ULONG ulFdCount,
	INT iTimeout
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = poll(ppfd, ulFdCount, iTimeout);
	GPOS_ASSERT_IMP(0 > iRes, EINVAL != errno);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::ISend
//
//	@doc:
//		Send data over socket
//
//---------------------------------------------------------------------------
INT
gpos::netutils::ISend
	(
	INT iSocketFd,
	const void *pvBuffer,
	INT iLen,
	INT iFlags
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = (INT) send(iSocketFd, pvBuffer, iLen, iFlags);
	GPOS_ASSERT_IMP
		(
		0 > iRes,
		EACCES != errno && EINVAL != errno && ENOTCONN != errno && EBADF != errno &&
		ENOTSOCK != errno && EOPNOTSUPP != errno
		);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::IRecv
//
//	@doc:
//		Receive data over socket
//
//---------------------------------------------------------------------------
INT
gpos::netutils::IRecv
	(
	INT iSocketFd,
	void *pvBuffer,
	INT iLen,
	INT iFlags
	)
{
	GPOS_ASSERT_NO_SPINLOCK;

	INT iRes = (INT) recv(iSocketFd, pvBuffer, iLen, iFlags);
	GPOS_ASSERT_IMP
		(
		0 > iRes,
		EINVAL != errno && ENOTCONN != errno && EBADF != errno && ENOTSOCK != errno
		);

	return iRes;
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::Poll
//
//	@doc:
//		Poll connection for sending/receiving
//
//---------------------------------------------------------------------------
void
gpos::netutils::Poll
	(
	INT iSockFd,
	EPollType ept
	)
{
	SPollFd pfd;
	pfd.fd = iSockFd;
	pfd.events = ept;

	INT iRes = 0;
	while (0 >= (iRes = IPoll(&pfd, 1 /*ulFdCount*/, GPOS_NET_POLL_TIMEOUT_MS)))
	{
		if (0 > iRes && !FTransientError(errno))
		{
			GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
		}

		GPOS_CHECK_ABORT;
	}

	GPOS_ASSERT(iRes == 1);
	GPOS_ASSERT((pfd.events & ept) == ept);
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::InitSocket
//
//	@doc:
//		Initialize socket properties
//
//---------------------------------------------------------------------------
void
gpos::netutils::InitSocket
	(
	INT iSockFd
	)
{
	// get socket flags
	INT iFlags = fcntl(iSockFd, F_GETFL, 0);

	// add non-blocking flag
	if (0 > fcntl(iSockFd, F_SETFL, iFlags | O_NONBLOCK))
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
	}

#ifdef GPOS_Darwin
	// suppress SIGPIPE signal
	iFlags = 1;
	if (0 > setsockopt(iSockFd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&iFlags, sizeof(iFlags)))
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
	}
#endif // GPOS_Darwin
}


//---------------------------------------------------------------------------
//	@function:
//		netutils::IPollErrno
//
//	@doc:
//		Get errno after poll on non-blocking socket
//
//---------------------------------------------------------------------------
INT
gpos::netutils::IPollErrno
	(
	INT iSockFd
	)
{
	INT iError;
	ULONG ulLen = GPOS_SIZEOF(iError);
	if (0 > getsockopt(iSockFd, SOL_SOCKET, SO_ERROR, &iError, &ulLen))
	{
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
	}

	return iError;
}


#ifdef GPOS_FPSIMULATOR

//---------------------------------------------------------------------------
//	@function:
//		netutils::FSimulateNetError
//
//	@doc:
//		Inject networking exception
//
//---------------------------------------------------------------------------
BOOL
gpos::netutils::FSimulateNetError
	(
	INT iErrno,
	const CHAR *szFile,
	ULONG ulLine
	)
{
	GPOS_ASSERT(0 < iErrno);

	ITask *ptsk = ITask::PtskSelf();
	if (NULL != ptsk &&
	    ptsk->FTrace(EtraceSimulateNetError) &&
	    CFSimulator::Pfsim()->FNewStack(CException::ExmaSystem, CException::ExmiNetError) &&
	    !GPOS_MATCH_EX(ptsk->Perrctxt()->Exc(), CException::ExmaSystem, CException::ExmiNetError))
	{
		// disable simulation temporarily to log injection
		CAutoTraceFlag(EtraceSimulateNetError, false);

		CLogger *plogger = dynamic_cast<CLogger*>(ITask::PtskSelf()->Ptskctxt()->PlogErr());
		if (!plogger->FLogging())
		{
			GPOS_TRACE_FORMAT_ERR("Simulating networking error at %s:%d", szFile, ulLine);
		}

		errno = iErrno;

		if (ptsk->Perrctxt()->FPending())
		{
			ptsk->Perrctxt()->Reset();
		}

		return true;
	}

	return false;
}

#endif // GPOS_FPSIMULATOR

// EOF
