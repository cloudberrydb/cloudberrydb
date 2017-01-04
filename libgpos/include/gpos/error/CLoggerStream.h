//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerStream.h
//
//	@doc:
//		Implementation of logging interface over stream
//---------------------------------------------------------------------------
#ifndef GPOS_CLoggerStream_H
#define GPOS_CLoggerStream_H

#include "gpos/error/CLogger.h"
#include "gpos/io/IOstream.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CLoggerStream
	//
	//	@doc:
	//		Stream logging.
	//
	//---------------------------------------------------------------------------

	class CLoggerStream : public CLogger
	{
		private:

			// log stream
			IOstream &m_os;

			// write string to stream
			void Write
				(
				const WCHAR *wszLogEntry,
				ULONG // ulSev
				)
			{
				 m_os = m_os << wszLogEntry;
			}

			// no copy ctor
			CLoggerStream(const CLoggerStream&);

		public:

			// ctor
			CLoggerStream
				(
				IOstream &os
				);

			// dtor
			virtual
			~CLoggerStream();

			// wrapper for stdout
			static
			CLoggerStream m_plogStdOut;

			// wrapper for stderr
			static
			CLoggerStream m_plogStdErr;

	};	// class CLoggerStream
}

#endif // !GPOS_CLoggerStream_H

// EOF

