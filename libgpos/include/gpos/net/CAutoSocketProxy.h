//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CAutoSocketProxy.h
//
//	@doc:
//		Socket proxy; ensures proper release of socket
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoSocketProxy_H
#define GPOS_CAutoSocketProxy_H

#include "gpos/base.h"
#include "gpos/net/CSocket.h"
#include "gpos/net/CSocketListener.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoSocketProxy
	//
	//	@doc:
	//		Socket proxy
	//
	//---------------------------------------------------------------------------
	class CAutoSocketProxy
	{
		private:

			// socket
			CSocket *m_psocket;

			// no copy ctor
			CAutoSocketProxy(const CAutoSocketProxy&);

		public:

			// ctor
			explicit
			CAutoSocketProxy
				(
				CSocket *psocket
				)
				:
				m_psocket(psocket)
			{
				GPOS_ASSERT(NULL != psocket);
				GPOS_ASSERT(NULL != psocket->m_psl);
			}

			// dtor
			~CAutoSocketProxy()
			{
				m_psocket->m_psl->Release(m_psocket);
			}

			// socket accessor
			CSocket *Psocket() const
			{
				return m_psocket;
			}

	};	// class CAutoSocketProxy
}

#endif // !GPOS_CAutoSocketProxy_H

// EOF

