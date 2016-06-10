//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSerializableStackTrace.cpp
//
//	@doc:
//		Serializable stack trace object
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/task/CTask.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CDXLSections.h"

#include "gpopt/minidump/CSerializableStackTrace.h"

#define GPOPT_MINIDUMP_BUF_SIZE (1024 * 4)

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CSerializableStackTrace::CSerializableStackTrace
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSerializableStackTrace::CSerializableStackTrace()
	:
	CSerializable(),
	m_wszBuffer(NULL),
	m_fAllocated(false)
{}


//---------------------------------------------------------------------------
//	@function:
//		CSerializableStackTrace::~CSerializableStackTrace
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSerializableStackTrace::~CSerializableStackTrace()
{
	GPOS_DELETE_ARRAY(m_wszBuffer);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableStackTrace::AllocateBuffer
//
//	@doc:
//		allocate serializable trace buffer
//
//---------------------------------------------------------------------------
void
CSerializableStackTrace::AllocateBuffer
	(
	IMemoryPool *pmp
	)
{
	if (!m_fAllocated)
	{
		m_wszBuffer = GPOS_NEW_ARRAY(pmp, WCHAR, GPOPT_MINIDUMP_BUF_SIZE);
		m_fAllocated = true;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CSerializableStackTrace::UlpRequiredSpace
//
//	@doc:
//		Calculate space needed for serialization
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableStackTrace::UlpRequiredSpace()
{
	return UlpSerialize(m_wszBuffer, GPOPT_MINIDUMP_BUF_SIZE);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableStackTrace::UlpSerialize
//
//	@doc:
//		Serialize contents into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableStackTrace::UlpSerialize
	(
	WCHAR *wszBuffer, 
	ULONG_PTR ulpAllocSize
	)
{
	if (!m_fAllocated)
	{
		// buffer was not allocated, cannot serialize
		return 0;
	}

	if (!ITask::PtskSelf()->FPendingExc())
	{
		// no pending exception: no need to serialize stack trace
		return 0;
	}
	WCHAR wszStackBuffer[GPOPT_MINIDUMP_BUF_SIZE];
	CWStringStatic str(wszStackBuffer, GPOS_ARRAY_SIZE(wszStackBuffer));

	str.AppendFormat(CDXLSections::m_wszStackTraceHeader);

	CErrorContext *perrctxt = CTask::PtskSelf()->PerrctxtConvert();
	perrctxt->Psd()->AppendTrace(&str);

	str.AppendFormat(CDXLSections::m_wszStackTraceFooter);

	ULONG_PTR ulpSize = std::min
		(
		(ULONG_PTR) str.UlLength(),
		ulpAllocSize
		);

	(void) clib::PvMemCpy
		(
		(BYTE *) wszBuffer,
		(BYTE *) wszStackBuffer,
		ulpSize * GPOS_SIZEOF(WCHAR)
		);

	return ulpSize;
}

// EOF

