//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CLoggerComm.h
//
//	@doc:
//		Implementation of logging interface over communicator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CLoggerComm_H
#define GPOPT_CLoggerComm_H

#include "gpos/error/CLogger.h"

#include "naucrates/comm/CCommunicator.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLoggerComm
	//
	//	@doc:
	//		Logging over socket using communicator
	//
	//---------------------------------------------------------------------------
	class CLoggerComm : public CLogger
	{
		private:

			// communicator instance
			CCommunicator *m_pcomm;

			// write string to socket
			void Write(const WCHAR *wszLogEntry, ULONG ulSev);

			// no copy ctor
			CLoggerComm(const CLoggerComm&);

		public:

			// ctor
			CLoggerComm(CCommunicator *pcomm);

			// dtor
			virtual
			~CLoggerComm();

	};	// class CLoggerComm
}

#endif // !GPOPT_CLoggerComm_H

// EOF

