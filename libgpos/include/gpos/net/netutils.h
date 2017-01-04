//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		netutils.h
//
//	@doc:
//		Networking utilities;
//---------------------------------------------------------------------------

#ifndef GPOS_netutils_H
#define GPOS_netutils_H

#include "gpos/base.h"
#include "gpos/net/nettypes.h"

// macro for networking error simulation
#ifdef GPOS_FPSIMULATOR
// simulate networking error with specified address of returned error value,
// and specified errno
#define GPOS_CHECK_SIM_NET_ERR_CODE(pRes,iErrno, PfNet) \
		do \
		{\
			*pRes = -1; \
			if (!netutils::FSimulateNetError(iErrno, __FILE__, __LINE__)) \
			{ \
				*pRes = (INT) PfNet; \
			} \
		} while(0)
#else
// execute the networking function
#define GPOS_CHECK_SIM_NET_ERR_CODE(pRes, iErrno, PfNet) \
		do \
		{\
			GPOS_ASSERT(NULL != pRes); \
                 \
			*pRes = PfNet; \
		} while(0)
#endif // GPOS_FPSIMULATOR

// simulate networking error with specified address of returned error value;
// set errno to ETIMEDOUT
#define GPOS_CHECK_SIM_NET_ERR(pRes, PfNet) \
	GPOS_CHECK_SIM_NET_ERR_CODE(pRes, ETIMEDOUT, PfNet)


namespace gpos
{
	namespace netutils
	{
		// create socket
		INT ISocket(INT iSocketFamily, INT iSocketType, INT iProtocol);

		// bind socket to address
		INT IBind(INT iSocketFd, const SSockAddr *psa, SOCKET_LEN slAddressLen);

		// connect to a remote host
		INT IConnect(INT iSocketFd, const SSockAddr *psa, SOCKET_LEN slAddressLen);

		// listen for new connection requests
		INT IListen(INT iSocketFd, INT iBacklog);

		// accept a new connection
		INT IAccept(INT iSocketFd, SSockAddr *psa, SOCKET_LEN *pslAddressLen);

		// poll connection until it is ready to send or receive
		INT IPoll(SPollFd *ppfd, ULONG ulFdCount, INT iTimeout);

		// send data over socket
		INT ISend(INT iSocketFd, const void *pvBuffer, INT iLen, INT iFlags);

		// receive data over socket
		INT IRecv(INT iSocketFd, void *pvBuffer, INT iLen, INT iFlags);

		// poll connection for sending/receiving
		void Poll(INT iSockFd, EPollType ept);

		// initialize socket properties
		void InitSocket(INT iSockFd);

		// get errno after poll on non-blocking socket
		INT IPollErrno(INT iSockFd);

		// check for transient error
		inline
		BOOL FTransientError(INT iErrno)
		{
			return (EINTR == iErrno || EAGAIN == iErrno || EWOULDBLOCK == iErrno);
		}

		// check result of networking system call for error
		inline
		void CheckNetError(INT iRes)
		{
			if (0 > iRes)
			{
				GPOS_RAISE(CException::ExmaSystem, CException::ExmiNetError, errno);
			}
		}

#ifdef GPOS_FPSIMULATOR
		// inject networking error for functions whose returned value type is INT
		BOOL FSimulateNetError(INT iErrno, const CHAR *szFile, ULONG ulLine);
#endif // GPOS_FPSIMULATOR

	}	// namespace netutils
}

#endif // !GPOS_netutils_H

// EOF

