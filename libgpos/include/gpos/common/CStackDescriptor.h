//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CStackTrace.h
//
//	@doc:
//		Interface class for execution stack tracing.
//---------------------------------------------------------------------------

#ifndef GPOS_CStackDescriptor_H
#define GPOS_CStackDescriptor_H

#include "gpos/types.h"
#include "gpos/common/clibtypes.h"
#include "gpos/io/IOstream.h"

#define GPOS_STACK_TRACE_BUFFER_SIZE    4096
#define GPOS_STACK_TRACE_DEPTH          32
#define GPOS_STACK_SYMBOL_SIZE          16384

#define GPOS_STACK_TRACE_FORMAT_SIZE    192

namespace gpos
{
	// prototype
	class CWString;

	class CStackDescriptor
	{

		private:

			// stack depth
			ULONG m_ulDepth;

			// array with frame return addresses
			void *m_rgpvAddresses[GPOS_STACK_TRACE_DEPTH];

			// append formatted symbol description
			void AppendSymbolInfo(CWString *pws, CHAR *szDemangleBuf, SIZE_T size, const Dl_info &rgInfo, ULONG ulIdx) const;

#if (GPOS_sparc)
			//  method called by walkcontext function to store return addresses
			static INT IGetStackFrames(ULONG_PTR ulpFp, INT sig __attribute__((unused)), void *pvContext);
#endif // GPOS_sparc

			// reset descriptor
			void Reset()
			{
				// reset stack depth
				m_ulDepth = 0;
			}

		public:

			// ctor
			CStackDescriptor()
				:
				m_ulDepth(0)
			{
				Reset();
			}

			// store current stack skipping (ulSkip) top frames
			void BackTrace(ULONG ulSkip = 0);

			// append trace of stored stack to string
			void AppendTrace
				(
				CWString *pws,
				ULONG ulDepth = GPOS_STACK_TRACE_DEPTH
				) const;

			// append trace of stored stack to stream
			void AppendTrace
				(
				IOstream &os,
				ULONG ulDepth = GPOS_STACK_TRACE_DEPTH
				) const;

			// get hash value for stored stack
			ULONG UlHash() const;

	}; // class CStackTrace

}

#endif // !GPOS_CStackDescriptor_H_

// EOF

