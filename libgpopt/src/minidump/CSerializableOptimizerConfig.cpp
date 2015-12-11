//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CSerializableOptimizerConfig.cpp
//
//	@doc:
//		Class for serializing optimizer config objects
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
#include "gpos/task/CTraceFlagIter.h"

#include "naucrates/dxl/xml/CDXLSections.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/engine/CCTEConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/minidump/CSerializableOptimizerConfig.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

// max length of the string representation of traceflag codes
#define GPOPT_MAX_TRACEFLAG_CODE_LENGTH 10

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::CSerializableOptimizerConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSerializableOptimizerConfig::CSerializableOptimizerConfig
	(
	const COptimizerConfig *poconf
	)
	:
	CSerializable(),
	m_poconf(poconf),
	m_pstrOptimizerConfig(NULL)
{
	GPOS_ASSERT(NULL != poconf);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::~CSerializableOptimizerConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSerializableOptimizerConfig::~CSerializableOptimizerConfig()
{
	GPOS_DELETE(m_pstrOptimizerConfig);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::Serialize
//
//	@doc:
//		Serialize optimizer config to string
//
//---------------------------------------------------------------------------
void
CSerializableOptimizerConfig::Serialize
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL == m_pstrOptimizerConfig);

	m_pstrOptimizerConfig = CDXLUtils::PstrSerializeOptimizerConfig(pmp, m_poconf, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::UlpRequiredSpace
//
//	@doc:
//		Calculate space needed for serialization
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableOptimizerConfig::UlpRequiredSpace()
{
	if (NULL == m_pstrOptimizerConfig)
	{
		return 0;
	}
	
	return	m_pstrOptimizerConfig->UlLength() + 
			UlpRequiredSpaceTraceflags() + 
			GPOS_WSZ_LENGTH(CDXLSections::m_wszOptimizerConfigFooter);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::UlpSerializeFooter
//
//	@doc:
//		Serialize footer into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableOptimizerConfig::UlpSerializeFooter
	(
	WCHAR *wszBuffer,
	ULONG_PTR ulpAllocSize
	)
{
	const WCHAR *wszFooter = CDXLSections::m_wszOptimizerConfigFooter;
	
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
//		CSerializableOptimizerConfig::UlpRequiredSpaceTraceflags
//
//	@doc:
//		Compute required space for serializing traceflags
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableOptimizerConfig::UlpRequiredSpaceTraceflags()
{
	CTraceFlagIter tfi;
	ULONG ul = 0;
	ULONG ulLength = 0;
	WCHAR wsz[GPOPT_MAX_TRACEFLAG_CODE_LENGTH];
	CWStringStatic str(wsz, GPOPT_MAX_TRACEFLAG_CODE_LENGTH);
	
	while (tfi.FAdvance())
	{
		ul++;
		str.Reset();
		str.AppendFormat(GPOS_WSZ_LIT("%d"), tfi.UlBit());
		ulLength += str.UlLength();
	}
	
	// number of commas
	ULONG ulCommas = 0;
	
	if (1 < ul)
	{
		ulCommas = ul - 1;
	}
	
	// compute the length of <TraceFlags Value = "TF1,TF2"/>
	return GPOS_WSZ_LENGTH(CDXLSections::m_wszTraceFlagsSectionPrefix) +
			GPOS_WSZ_LENGTH(CDXLSections::m_wszTraceFlagsSectionSuffix)  +
			ulLength + ulCommas;
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::UlpSerializeTraceflags
//
//	@doc:
//		Serialize traceflags into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableOptimizerConfig::UlpSerializeTraceflags
	(
	WCHAR *wszBuffer, 
	ULONG_PTR ulpAllocSize
	)
{
	ULONG_PTR ulpRequiredSpace = UlpRequiredSpaceTraceflags();
	
	GPOS_RTL_ASSERT(ulpAllocSize >= ulpRequiredSpace);
	
	// copy prefix
	ULONG_PTR ulpPrefixLength = GPOS_WSZ_LENGTH(CDXLSections::m_wszTraceFlagsSectionPrefix); 
	(void) clib::PvMemCpy
		(
		(BYTE *) wszBuffer,
		(BYTE *) CDXLSections::m_wszTraceFlagsSectionPrefix,
		ulpPrefixLength * GPOS_SIZEOF(WCHAR)
		);
	
	wszBuffer += ulpPrefixLength;
	
	// populate traceflags
	CTraceFlagIter tfi;
	ULONG ul = 0;
	const CWStringConst *pstrComma = CDXLTokens::PstrToken(EdxltokenComma);
	WCHAR wszTF[GPOPT_MAX_TRACEFLAG_CODE_LENGTH + 2];
	CWStringStatic str(wszTF, GPOPT_MAX_TRACEFLAG_CODE_LENGTH + 2);
	
	while (tfi.FAdvance())
	{
		if (0 < ul)
		{
			str.AppendFormat(GPOS_WSZ_LIT("%ls"), pstrComma->Wsz());
		}
		
		str.AppendFormat(GPOS_WSZ_LIT("%d"), tfi.UlBit());
		
		(void) clib::PvMemCpy
			(
			(BYTE *) wszBuffer,
			(BYTE *) str.Wsz(),
			str.UlLength() * GPOS_SIZEOF(WCHAR)
			);
		wszBuffer += str.UlLength();		
		ul++;
		str.Reset();
	}
	
	// copy suffix
	ULONG_PTR ulpSuffixLength = GPOS_WSZ_LENGTH(CDXLSections::m_wszTraceFlagsSectionSuffix); 
	(void) clib::PvMemCpy
		(
		(BYTE *) wszBuffer,
		(BYTE *) CDXLSections::m_wszTraceFlagsSectionSuffix,
		ulpSuffixLength * GPOS_SIZEOF(WCHAR)
		);
	
	return ulpRequiredSpace;
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::UlpSerialize
//
//	@doc:
//		Serialize contents into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableOptimizerConfig::UlpSerialize
	(
	WCHAR *wszBuffer, 
	ULONG_PTR ulpAllocSize
	)
{
	if (NULL == m_pstrOptimizerConfig)
	{
		return 0;
	}
	
	ULONG_PTR ulpRequiredSpace = UlpRequiredSpace();
	
	ULONG_PTR ulpOptimizerConfigLength = m_pstrOptimizerConfig->UlLength();
	
	if (0 < ulpOptimizerConfigLength)
	{
		(void) clib::PvMemCpy
			(
			(BYTE *) wszBuffer,
			(BYTE *) m_pstrOptimizerConfig->Wsz(),
			ulpOptimizerConfigLength * GPOS_SIZEOF(WCHAR)
			);
		
		wszBuffer += ulpOptimizerConfigLength;
	}
	
	ULONG_PTR ulpTFSize = UlpSerializeTraceflags(wszBuffer, ulpAllocSize);
	wszBuffer += ulpTFSize;
	
	UlpSerializeFooter(wszBuffer, ulpAllocSize - ulpOptimizerConfigLength - ulpTFSize);
	return ulpRequiredSpace;
}

// EOF

