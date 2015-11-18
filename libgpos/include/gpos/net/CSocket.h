//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocket.h
//
//	@doc:
//		Socket abstraction
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSocket_H
#define GPOS_CSocket_H

#include "gpos/base.h"
#include "gpos/common/CList.h"

namespace gpos
{
	// class prototypes
	class CSocketListener;

	//---------------------------------------------------------------------------
	//	@class:
	//		CSocket
	//
	//	@doc:
	//		Networking socket abstraction;
	//
	//---------------------------------------------------------------------------
	class CSocket
	{
		// friend classes
		friend class CAutoSocketProxy;

		private:

			// file descriptor
			INT m_iSocketFd;

			// listener owning this socket
			CSocketListener *m_psl;

			// no copy ctor
			CSocket(const CSocket&);

		public:

			// ctor
			explicit
			CSocket(INT iFileDescr);

			// ctor
			CSocket(INT iFileDescr, CSocketListener *psl);

			// dtor
			~CSocket();

			// file descriptor accessor
			INT ISocketFd() const
			{
				return m_iSocketFd;
			}

			// socket listener accessor
			CSocketListener *Psl() const
			{
				GPOS_ASSERT(NULL != m_psl);

				return m_psl;
			}

			// send data to remote host
			void Send(const void *pvBuffer, ULONG ulSize);

			// receive data from remote host; returns size of received message;
			ULONG UlRecv(void *pvBuffer, ULONG ulMaxSize, ULONG ulExpected = 0);

			// check if socket is operational
			BOOL FCheck() const;

			// link for socket list in listener
			SLink m_link;

	};	// class CSocket
}

#endif // !GPOS_CSocket_H

// EOF

