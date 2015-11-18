//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketConnectorUDS.h
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
#ifndef GPOS_CSocketConnectorUDS_H
#define GPOS_CSocketConnectorUDS_H

#include "gpos/base.h"
#include "gpos/net/CSocketConnector.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSocketConnectorUDS
	//
	//	@doc:
	//		Connector using UDS socket
	//
	//---------------------------------------------------------------------------

	class CSocketConnectorUDS : public CSocketConnector
	{
		private:

			// path where target UDS is bound to
			const CHAR *m_szPath;

			// check if connection is established
			static
			BOOL FConnected(INT iSocketFd);

			// no copy ctor
			CSocketConnectorUDS(const CSocketConnectorUDS&);

		public:

			// ctor
			CSocketConnectorUDS(IMemoryPool *pmp, const CHAR *szPath);

			// dtor
			virtual
			~CSocketConnectorUDS();

			// connect to remote host
			virtual
			void Connect();

	};	// class CSocketConnectorUDS
}

#endif // !GPOS_CSocketConnectorUDS_H

// EOF

