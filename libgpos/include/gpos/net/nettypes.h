//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		nettypes.h
//
//	@doc:
//		Networking type definitions for GPOS;
//---------------------------------------------------------------------------
#ifndef GPOS_nettypes_H
#define GPOS_nettypes_H

#include <errno.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>


namespace gpos
{
	// socket length
	typedef socklen_t SOCKET_LEN;

	// socket address descriptor
	typedef struct sockaddr SSockAddr;

	// socket address descriptor - used for stream connections
	typedef struct sockaddr_un SSockAddrUn;

	// structure containing file descriptors for poll
	typedef struct pollfd SPollFd;

	// poll  type
	enum EPollType
	{
		EptOut = POLLOUT,
		EptIn = POLLIN
	};
}

#endif // !GPOS_nettypes_H

// EOF

