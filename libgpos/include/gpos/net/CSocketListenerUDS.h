//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketListenerUDS.h
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
#ifndef GPOS_CSocketListenerUDS_H
#define GPOS_CSocketListenerUDS_H

#include "gpos/base.h"
#include "gpos/net/CSocketListener.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSocketListenerUDS
	//
	//	@doc:
	//		Listener using UDS socket
	//
	//---------------------------------------------------------------------------

	class CSocketListenerUDS : public CSocketListener
	{
		private:

			// path where target UDS is bound to
			const CHAR *m_szPath;

			// flag indicating if object has started serving connection requests
			BOOL m_fListening;

			// no copy ctor
			CSocketListenerUDS(const CSocketListenerUDS&);

		protected:

			// get file descriptor to handle next connection request
			virtual
			INT ISocketFdNext();

		public:

			// ctor
			CSocketListenerUDS(IMemoryPool *pmp, const CHAR *szPath);

			// dtor
			virtual
			~CSocketListenerUDS();

			// start listening for new connection requests
			virtual
			void StartListener();

	};	// class CSocketListenerUDS
}

#endif // !GPOS_CSocketListenerUDS_H

// EOF

