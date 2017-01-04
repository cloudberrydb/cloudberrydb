//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CErrorContext.cpp
//
//	@doc:
//		Implements context container for error handling
//---------------------------------------------------------------------------

#include "gpos/utils.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/error/CMessageRepository.h"
#include "gpos/error/CMiniDumper.h"
#include "gpos/error/CSerializable.h"

using namespace gpos;


// logger buffer must be large enough to store error messages
GPOS_CPL_ASSERT
	(
	GPOS_ERROR_MESSAGE_BUFFER_SIZE <= GPOS_LOG_ENTRY_BUFFER_SIZE
	)
	;


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::CErrorContext
//
//	@doc:
//
//---------------------------------------------------------------------------
CErrorContext::CErrorContext
	(
	CMiniDumper *pmdr
	)
	:
	m_exc(CException::m_excInvalid),
	m_ulSev(CException::ExsevError),
	m_fPending(false),
	m_fRethrow(false),
	m_wss(m_wsz, GPOS_ARRAY_SIZE(m_wsz)),
	m_pmdr(pmdr)
{
	m_listSerial.Init(GPOS_OFFSET(CSerializable, m_linkErrCtxt));
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::~CErrorContext
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CErrorContext::~CErrorContext()
{
	GPOS_ASSERT(!m_fPending && "unhandled error pending");
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::Reset
//
//	@doc:
//		Clean up error context
//
//---------------------------------------------------------------------------
void
CErrorContext::Reset()
{
	GPOS_ASSERT(m_fPending);
	
	m_fPending = false;
	m_fRethrow = false;
	m_exc = CException::m_excInvalid;
	m_wss.Reset();
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::Record
//
//	@doc:
//		Grab error context and save it off and format error message
//
//---------------------------------------------------------------------------
void
CErrorContext::Record
	(
	CException &exc,
	VA_LIST vl
	)
{
#ifdef GPOS_DEBUG
	if (m_fPending)
	{
		// reset pending flag so we can throw from here
		m_fPending = false;
		
		GPOS_ASSERT(!"Pending error unhandled when raising new error");
 	}
#endif // GPOS_DEBUG
	
	m_fPending = true;
	m_exc = exc;
	
	// store stack, skipping current frame
	m_sd.BackTrace(1);

	ELocale eloc = ITask::PtskSelf()->Eloc();
	CMessage *pmsg = CMessageRepository::Pmr()->PmsgLookup(exc, eloc);
	pmsg->Format(&m_wss, vl);

	m_ulSev = pmsg->UlSev();

	if (GPOS_FTRACE(EtracePrintExceptionOnRaise))
	{
		std::wcerr << GPOS_WSZ_LIT("Exception: ") << m_wss.Wsz() << std::endl;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::AppendErrno
//
//	@doc:
//		Print errno
//
//---------------------------------------------------------------------------
void
CErrorContext::AppendErrnoMsg()
{
	GPOS_ASSERT(m_fPending);
	GPOS_ASSERT
		(
		GPOS_MATCH_EX(m_exc, CException::ExmaSystem, CException::ExmiIOError) ||
		GPOS_MATCH_EX(m_exc, CException::ExmaSystem, CException::ExmiNetError)
		);
	GPOS_ASSERT(0 < errno && "Errno has not been set");

	// get errno description
	clib::StrErrorR(errno, m_sz, GPOS_ARRAY_SIZE(m_sz));

	m_wss.AppendFormat(GPOS_WSZ_LIT(" (%s)"), m_sz);
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::CopyPropErrCtxt
//
//	@doc:
//		Copy necessary info for error propagation
//
//---------------------------------------------------------------------------
void
CErrorContext::CopyPropErrCtxt
	(
	const IErrorContext *perrctxt
	)
{
	GPOS_ASSERT(!m_fPending);

	m_fPending = true;

	// copy exception
	m_exc = perrctxt->Exc();

	// copy error message
	m_wss.Reset();
	m_wss.Append(&(reinterpret_cast<const CErrorContext*>(perrctxt)->m_wss));

	// copy severity
	m_ulSev = perrctxt->UlSev();
}


//---------------------------------------------------------------------------
//	@function:
//		CErrorContext::Serialize
//
//	@doc:
//		Serialize registered objects
//
//---------------------------------------------------------------------------
void
CErrorContext::Serialize()
{
	if (NULL == m_pmdr || m_listSerial.FEmpty())
	{
		return;
	}

	// calculate entry size
	ULONG_PTR ulpEntrySize =
			m_pmdr->UlpRequiredSpaceEntryHeader() + m_pmdr->UlpRequiredSpaceEntryFooter();
	for (CSerializable *pserial = m_listSerial.PtFirst();
	     NULL != pserial;
	     pserial = m_listSerial.PtNext(pserial))
	{
		ulpEntrySize += pserial->UlpRequiredSpace();

	}

	// attempt to reserve space in mini-dumper
	WCHAR *wszEntry = m_pmdr->WszReserve(ulpEntrySize);
	if (NULL == wszEntry)
	{
		return;
	}

	WCHAR *wszEntryElem = wszEntry;
	ULONG_PTR ulpRemainingSpace = ulpEntrySize;

	// serialize objects to reserved space
	ULONG_PTR ulpEntry = m_pmdr->UlpSerializeEntryHeader(wszEntryElem, ulpRemainingSpace);
	wszEntryElem += ulpEntry;
	ulpRemainingSpace -= ulpEntry;

	for (CSerializable *pserial = m_listSerial.PtFirst();
	     NULL != pserial;
	     pserial = m_listSerial.PtNext(pserial))
	{
		ulpEntry = pserial->UlpSerialize(wszEntryElem, ulpRemainingSpace);
		wszEntryElem += ulpEntry;
		ulpRemainingSpace -= ulpEntry;
	}

	ulpEntry = m_pmdr->UlpSerializeEntryFooter(wszEntryElem, ulpRemainingSpace);
	wszEntryElem += ulpEntry;
	ulpRemainingSpace -= ulpEntry;

	GPOS_ASSERT(0 == ulpRemainingSpace &&
	            "discrepancy between calculated and actual minidump entry size");
}


// EOF
