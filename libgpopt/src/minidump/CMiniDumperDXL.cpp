//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMiniDumperDXL.cpp
//
//	@doc:
//		Implementation of DXL-specific minidump handler
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/string/CWStringBase.h"
#include "gpos/task/CTask.h"
#include "gpos/task/CWorker.h"

#include "naucrates/dxl/xml/CDXLSections.h"

#include "gpopt/minidump/CMiniDumperDXL.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;

// size of temporary buffer for expanding XML entry header
#define GPOPT_THREAD_HEADER_SIZE 128

// size of buffer used to serialize minidump -- creation fails if its size exceeds this number
#define GPOPT_DXL_MINIDUMP_SIZE (16 * 1024 * 1024)

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXL::CMiniDumperDXL
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMiniDumperDXL::CMiniDumperDXL
	(
	IMemoryPool *pmp
	)
	:
	CMiniDumper(pmp, GPOPT_DXL_MINIDUMP_SIZE)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXL::~CMiniDumperDXL
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMiniDumperDXL::~CMiniDumperDXL()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXL::UlpRequiredSpaceEntryHeader
//
//	@doc:
//		Size to reserve for entry header
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperDXL::UlpRequiredSpaceEntryHeader()
{
	WCHAR szBuffer[GPOPT_THREAD_HEADER_SIZE];

	return UlpSerializeEntryHeader(szBuffer, GPOS_ARRAY_SIZE(szBuffer));

}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXL::UlpRequiredSpaceEntryFooter
//
//	@doc:
//		Size to reserve for entry footer
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperDXL::UlpRequiredSpaceEntryFooter()
{
	return GPOS_WSZ_LENGTH(CDXLSections::m_wszThreadFooter);
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXL::SerializeHeader
//
//	@doc:
//		Serialize minidump header
//
//---------------------------------------------------------------------------
void
CMiniDumperDXL::SerializeHeader()
{
	const WCHAR *wsz = CDXLSections::m_wszDocumentHeader;
	ULONG ulLength = GPOS_WSZ_LENGTH(wsz);
	
	WCHAR *wszHeader = WszReserve(ulLength);
	(void) clib::PvMemCpy(wszHeader, (BYTE *) wsz, ulLength * GPOS_SIZEOF(WCHAR));	
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXL::SerializeFooter
//
//	@doc:
//		Serialize minidump footer
//
//---------------------------------------------------------------------------
void
CMiniDumperDXL::SerializeFooter()
{
	const WCHAR *wsz = CDXLSections::m_wszDocumentFooter;
	ULONG ulLength = GPOS_WSZ_LENGTH(wsz);

	WCHAR *wszFooter = WszReserve(ulLength);
	(void) clib::PvMemCpy(wszFooter, (BYTE *) wsz, ulLength * GPOS_SIZEOF(WCHAR));
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXL::UlpSerializeEntryHeader
//
//	@doc:
//		Serialize entry header
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperDXL::UlpSerializeEntryHeader
	(
	WCHAR * wszEntry,
	ULONG_PTR ulpAllocSize
	)
{
	WCHAR wszBuffer[GPOPT_THREAD_HEADER_SIZE];
	
	CWStringStatic str(wszBuffer, GPOS_ARRAY_SIZE(wszBuffer));
	str.AppendFormat
		(
		CDXLSections::m_wszThreadHeaderTemplate,
		CWorker::PwrkrSelf()->UlThreadId()
		);

	ULONG_PTR ulpSize = (ULONG_PTR) str.UlLength();
	
	GPOS_RTL_ASSERT(ulpSize <= ulpAllocSize);
	
	(void) clib::PvMemCpy(wszEntry, (WCHAR *) wszBuffer, ulpSize * GPOS_SIZEOF(WCHAR));

	return ulpSize;

}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperDXL::UlpSerializeEntryFooter
//
//	@doc:
//		Serialize entry footer
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperDXL::UlpSerializeEntryFooter
	(
	WCHAR *wszEntry,
	ULONG_PTR ulpAllocSize
	)
{
	ULONG_PTR ulpLength = UlpRequiredSpaceEntryFooter();
	GPOS_RTL_ASSERT(ulpLength <= ulpAllocSize);
	
	(void) clib::PvMemCpy(wszEntry, (BYTE *) CDXLSections::m_wszThreadFooter, ulpLength * GPOS_SIZEOF(WCHAR));
	return ulpLength;
}

// EOF
