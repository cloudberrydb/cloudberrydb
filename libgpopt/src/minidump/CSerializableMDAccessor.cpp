//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSerializableMDAccessor.cpp
//
//	@doc:
//		Wrapper for serializing MD objects
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/task/CTask.h"

#include "naucrates/dxl/xml/CDXLSections.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/minidump/CSerializableMDAccessor.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CSerializableMDAccessor::CSerializableMDAccessor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSerializableMDAccessor::CSerializableMDAccessor
	(
	CMDAccessor *pmda
	)
	:
	CSerializable(),
	m_pmda(pmda)
{
	GPOS_ASSERT(NULL != pmda);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableMDAccessor::UlpRequiredSpace
//
//	@doc:
//		Calculate space needed for serialization
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableMDAccessor::UlpRequiredSpace()
{
	return GPOS_WSZ_LENGTH(CDXLSections::m_wszMetadataHeaderPrefix) +
			GPOS_WSZ_LENGTH(CDXLSections::m_wszMetadataHeaderSuffix)  + 
			m_pmda->UlpRequiredSysIdSpace() +
			m_pmda->UlpRequiredSpace() + 
			GPOS_WSZ_LENGTH(CDXLSections::m_wszMetadataFooter);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableMDAccessor::UlpSerializeHeader
//
//	@doc:
//		Serialize header into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableMDAccessor::UlpSerializeHeader
	(
	WCHAR *wszBuffer,
	ULONG_PTR ulpAllocSize
	)
{
	const WCHAR *wszHeaderPrefix = CDXLSections::m_wszMetadataHeaderPrefix;
	ULONG ulHeaderPrefixLength = GPOS_WSZ_LENGTH(wszHeaderPrefix);
	
	GPOS_RTL_ASSERT(ulpAllocSize >= ulHeaderPrefixLength);

	(void) clib::PvMemCpy
		(
		(BYTE *) wszBuffer,
		(BYTE *) wszHeaderPrefix,
		ulHeaderPrefixLength * GPOS_SIZEOF(WCHAR)
		);
	
	wszBuffer += ulHeaderPrefixLength;
	ulpAllocSize -= ulHeaderPrefixLength;
	
	ULONG_PTR ulpSysidLength = m_pmda->UlpSerializeSysid(wszBuffer, ulpAllocSize);
	
	wszBuffer += ulpSysidLength;
	ulpAllocSize -= ulpSysidLength;
	
	// serialize header suffix ">"
	const WCHAR *wszHeaderSuffix = CDXLSections::m_wszMetadataHeaderSuffix;
	ULONG ulHeaderSuffixLength = GPOS_WSZ_LENGTH(wszHeaderSuffix);
	
	GPOS_RTL_ASSERT(ulpAllocSize >= ulHeaderSuffixLength);

	(void) clib::PvMemCpy
		(
		(BYTE *) wszBuffer,
		(BYTE *) wszHeaderSuffix,
		ulHeaderSuffixLength * GPOS_SIZEOF(WCHAR)
		);
		
	return ulHeaderPrefixLength + ulHeaderSuffixLength + ulpSysidLength;
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableMDAccessor::UlpSerializeFooter
//
//	@doc:
//		Serialize footer into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableMDAccessor::UlpSerializeFooter
	(
	WCHAR *wszBuffer,
	ULONG_PTR ulpAllocSize
	)
{
	const WCHAR *wszFooter = CDXLSections::m_wszMetadataFooter;
	
	ULONG ulFooterLength = GPOS_WSZ_LENGTH(wszFooter);
	
	GPOS_RTL_ASSERT(ulpAllocSize >= ulFooterLength);

	(void) clib::PvMemCpy
		(
		(BYTE *) wszBuffer,
		(BYTE *) wszFooter,
		ulFooterLength * GPOS_SIZEOF(WCHAR)
		);
	
	return ulFooterLength;
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableMDAccessor::UlpSerialize
//
//	@doc:
//		Serialize contents into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableMDAccessor::UlpSerialize
	(
	WCHAR *wszBuffer, 
	ULONG_PTR ulpAllocSize
	)
{
	ULONG_PTR ulpRequiredSpace = UlpRequiredSpace();
	
	ULONG_PTR ulpHeaderSize = UlpSerializeHeader(wszBuffer, ulpAllocSize);
	ULONG_PTR ulpMDASize = m_pmda->UlpSerialize(wszBuffer + ulpHeaderSize, ulpAllocSize - ulpHeaderSize);
	(void) UlpSerializeFooter(wszBuffer + ulpHeaderSize + ulpMDASize, ulpAllocSize - ulpHeaderSize - ulpMDASize);
	return ulpRequiredSpace;
}

// EOF

