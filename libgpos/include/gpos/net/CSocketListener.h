//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketListener.h
//
//	@doc:
//		Listens for connection requests on socket
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSocketListener_H
#define GPOS_CSocketListener_H

#include "gpos/base.h"
#include "gpos/net/CSocket.h"
#include "gpos/sync/CSpinlock.h"

#define GPOS_NET_LISTEN_BACKLOG	(1024)

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSocketListener
	//
	//	@doc:
	//		Abstraction for listening for connection requests on socket
	//
	//---------------------------------------------------------------------------

	class CSocketListener
	{
		private:

			// list of created sockets
			CList<CSocket> m_listSockets;

			// spinlock to protect list operations
			CSpinlockOS m_slock;

			// no copy ctor
			CSocketListener(const CSocketListener&);

		protected:

			// socket to listen for new connection requests
			CSocket *m_psocketListen;

			// internal memory pool
			IMemoryPool *m_pmp;

			// ctor
			explicit
			CSocketListener(IMemoryPool *pmp);

			// dtor
			virtual
			~CSocketListener();

			// get file descriptor to handle next connection request
			virtual
			INT ISocketFdNext() = 0;

		public:

			// start listening for new connection requests
			virtual
			void StartListener() = 0;

			// get socket to handle next connection request
			CSocket *PsocketNext();

			// release socket
			void Release(CSocket *psocket);

	};	// class CSocketListener
}

#endif // !GPOS_CSocketListener_H

// EOF

