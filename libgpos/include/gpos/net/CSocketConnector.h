//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketConnector.h
//
//	@doc:
//		Creates socket to connect with remote host
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSocketConnector_H
#define GPOS_CSocketConnector_H

#include "gpos/base.h"
#include "gpos/net/CSocket.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSocketConnector
	//
	//	@doc:
	//		Abstraction for creating socket to connect with remote host
	//
	//---------------------------------------------------------------------------

	class CSocketConnector
	{
		private:

			// no copy ctor
			CSocketConnector(const CSocketConnector&);

		protected:

			// socket
			CSocket *m_psocket;

			// internal memory pool
			IMemoryPool *m_pmp;

		public:

			// ctor
			explicit
			CSocketConnector
				(
				IMemoryPool *pmp
				)
				:
				m_psocket(NULL),
				m_pmp(pmp)
			{
				GPOS_ASSERT(NULL != pmp);
			}

			// dtor
			virtual
			~CSocketConnector()
			{
				delete m_psocket;
			}

			// connect to remote host
			virtual
			void Connect() = 0;

			// socket accessor
			CSocket *Psocket() const
			{
				GPOS_ASSERT(NULL != m_psocket);
				return m_psocket;
			}

	};	// class CSocketConnector
}

#endif // !GPOS_CSocketConnector_H

// EOF

