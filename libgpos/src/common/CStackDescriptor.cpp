//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CStackDescriptor.cpp
//
//	@doc:
//		Implementation of interface class for execution stack tracing.
//---------------------------------------------------------------------------

#include "gpos/utils.h"
#include "gpos/common/CStackDescriptor.h"
#include "gpos/string/CWString.h"
#include "gpos/task/IWorker.h"

#define GPOS_STACK_DESCR_TRACE_BUF   (4096)

using namespace gpos;

#if (GPOS_sparc)


//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::IGetStackFrames
//
//	@doc:
//		Method called by walkcontext function to store return addresses
//
//---------------------------------------------------------------------------
INT
CStackDescriptor::IGetStackFrames
	(
	ULONG_PTR ulpFp,
	INT sig __attribute__((unused)),
	void *pvContext
	)
{
	CStackDescriptor *psd = (CStackDescriptor *) pvContext;

	// check if max number of frames has been reached
	if (psd->m_ulDepth < GPOS_STACK_TRACE_DEPTH)
	{
		// set frame address
		psd->m_rgpvAddresses[psd->m_ulDepth++] = (void *) ulpFp;
	}

	return 0;
}

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::BackTrace
//
//	@doc:
//		Store current stack
//
//---------------------------------------------------------------------------
void
CStackDescriptor::BackTrace
	(
	ULONG ulSkip
	)
{
	// reset stack depth
	Reset();

	// retrieve stack context
	ucontext_t uc;
	if (0 != clib::IGetContext(&uc))
	{
		return;
	}

	// walk stack context to get stack addresses
	if (0 != clib::IWalkContext(&uc, IGetStackFrames, this))
	{
		return;
	}

	// skip top frames
	if (ulSkip <= m_ulDepth)
	{
		m_ulDepth -= ulSkip;

		for (ULONG i = 0; i < m_ulDepth; i++)
		{
			m_rgpvAddresses[i] = m_rgpvAddresses[i + ulSkip];
		}
	}
}

#elif (GPOS_i386 || GPOS_i686 || GPOS_x86_64)

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::BackTrace
//
//	@doc:
//		Store current stack
//
//---------------------------------------------------------------------------
void
CStackDescriptor::BackTrace
	(
	ULONG ulSkip
	)
{
	// get base pointer of current frame
	ULONG_PTR ulpCurrFrame;
	GPOS_GET_FRAME_POINTER(ulpCurrFrame);

	// reset stack depth
	Reset();

	// pointer to next frame in stack
	void **ppvNextFrame = (void**)ulpCurrFrame;

	// get stack start address
	ULONG_PTR ulpStackStart = 0;
	IWorker *pwrkr = IWorker::PwrkrSelf();
	if (NULL == pwrkr)
	{
		// no worker in stack, return immediately
		return;
	}

	// get address from worker
	ulpStackStart = pwrkr->UlpStackStart();

	// consider the first GPOS_STACK_TRACE_DEPTH frames below worker object
	for (ULONG ulFrame = 0; ulFrame < GPOS_STACK_TRACE_DEPTH; ulFrame++)
	{
		// check if the frame pointer is after stack start and before previous frame
		if ((ULONG_PTR) *ppvNextFrame > ulpStackStart ||
			(ULONG_PTR) *ppvNextFrame < (ULONG_PTR) ppvNextFrame)
		{
			break;
	 	}

		// skip top frames
		if (0 < ulSkip)
		{
			ulSkip--;
		}
		else
		{
			// get return address (one above the base pointer)
			ULONG_PTR *pulpIP = (ULONG_PTR*)(ppvNextFrame + 1);
			m_rgpvAddresses[m_ulDepth++] = (void *) *pulpIP;
		}

		// move to next frame
		ppvNextFrame = (void**)*ppvNextFrame;
	}
}

#else // unsupported platform

void
CStackDescriptor::BackTrace
	(
	ULONG
	)
{
	GPOS_CPL_ASSERT(!"Backtrace is not supported for this platform");
}

#endif


//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendSymbolInfo
//
//	@doc:
//		Append formatted symbol description
//
//---------------------------------------------------------------------------
void
CStackDescriptor::AppendSymbolInfo
	(
	CWString *pws,
	CHAR *szDemangleBuf,
	SIZE_T size,
	const DL_INFO &info,
	ULONG ulIdx
	)
	const
{
	const CHAR* szSymbol = szDemangleBuf;

	// resolve symbol name
	if (info.dli_sname)
	{
		INT iStatus = 0;
		szSymbol = info.dli_sname;

		// demangle C++ symbol
		CHAR *pcDemangled = clib::SzDemangle(szSymbol, szDemangleBuf, &size, &iStatus);
		GPOS_ASSERT(size <= GPOS_STACK_SYMBOL_SIZE);
		
		if (0 == iStatus)
		{
			// skip args and template info
			for (ULONG ul = 0; ul < size; ul++)
			{
				if ('(' == szDemangleBuf[ul] || '<' == szDemangleBuf[ul])
				{
					szDemangleBuf[ul] = '\0';
					break;
				}
			}

			szSymbol = pcDemangled;
		}
	}
	else
	{
		szSymbol = "<symbol not found>";
	}

	// format symbol info
	pws->AppendFormat
		(
		GPOS_WSZ_LIT("%-4d 0x%016lx %s + %lu\n"),
		ulIdx + 1,										// frame no.
		(long unsigned int) m_rgpvAddresses[ulIdx],		// current address in frame
		szSymbol,										// symbol name
		(long unsigned int) m_rgpvAddresses[ulIdx] - (ULONG_PTR) info.dli_saddr
														// offset from frame start
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendTrace
//
//	@doc:
//		Append trace of stored stack to string
//
//---------------------------------------------------------------------------
void
CStackDescriptor::AppendTrace
	(
	CWString *pws,
	ULONG ulDepth
	)
	const
{
	GPOS_ASSERT(GPOS_STACK_TRACE_DEPTH >= m_ulDepth && "Stack exceeds maximum depth");

	// symbol info
	Dl_info info;


	// buffer for symbol demangling
	CHAR szDemangleBuf[GPOS_STACK_SYMBOL_SIZE];
	
	// print info for frames in stack
	for (ULONG i = 0; i < m_ulDepth && i < ulDepth; i++)
	{
		// resolve address
		clib::DlAddr(m_rgpvAddresses[i], &info);

		// get symbol description
		AppendSymbolInfo(pws, szDemangleBuf, GPOS_STACK_SYMBOL_SIZE, info, i);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendTrace
//
//	@doc:
//		Append trace of stored stack to stream
//
//---------------------------------------------------------------------------
void
CStackDescriptor::AppendTrace
	(
	IOstream &os,
	ULONG ulDepth
	)
	const
{
	WCHAR wsz[GPOS_STACK_DESCR_TRACE_BUF];
	CWStringStatic str(wsz, GPOS_ARRAY_SIZE(wsz));

	AppendTrace(&str, ulDepth);
	os << str.Wsz();
}


//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::UlHash
//
//	@doc:
//		Get hash value for stored stack
//
//---------------------------------------------------------------------------
ULONG
CStackDescriptor::UlHash() const
{
	GPOS_ASSERT(0 < m_ulDepth && "No stack to hash");
	GPOS_ASSERT(GPOS_STACK_TRACE_DEPTH >= m_ulDepth && "Stack exceeds maximum depth");

	return gpos::UlHashByteArray
				(
				(BYTE *) m_rgpvAddresses,
				m_ulDepth * GPOS_SIZEOF(m_rgpvAddresses[0])
				);
}

// EOF

