//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CSerializableOptimizerConfig.cpp
//
//	@doc:
//		Class for serializing optimizer config objects
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
	IMemoryPool *pmp,
	const COptimizerConfig *poconf
	)
	:
	CSerializable(),
	m_pmp(pmp),
	m_poconf(poconf)
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
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::SerializeTraceflags
//
//	@doc:
//		Serialize traceflags into provided stream
//
//---------------------------------------------------------------------------
void
CSerializableOptimizerConfig::SerializeTraceflags
	(
	COstream& oos
	)
{
	// copy prefix
	oos << CDXLSections::m_wszTraceFlagsSectionPrefix;
	
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

		oos << str.Wsz();
		ul++;
		str.Reset();
	}
	
	// copy suffix
	oos << CDXLSections::m_wszTraceFlagsSectionSuffix;
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::Serialize
//
//	@doc:
//		Serialize contents into provided stream
//
//---------------------------------------------------------------------------
void
CSerializableOptimizerConfig::Serialize
	(
	COstream &oos
	)
{
	CDXLUtils::SerializeOptimizerConfig(m_pmp, oos, m_poconf, false /*fIndent*/);
	SerializeTraceflags(oos);

	// Footer
	oos << CDXLSections::m_wszOptimizerConfigFooter;
}

// EOF

