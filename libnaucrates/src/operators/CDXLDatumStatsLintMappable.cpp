//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLDatumStatsLintMappable.cpp
//
//	@doc:
//		Implementation of DXL datum of types having LINT mapping
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumStatsLintMappable.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumStatsLintMappable::CDXLDatumStatsLintMappable
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumStatsLintMappable::CDXLDatumStatsLintMappable
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	INT iTypeModifier,
	BOOL fByVal,
	BOOL fNull,
	BYTE *pba,
	ULONG ulLength,
	LINT lValue
	)
	:
	CDXLDatumGeneric(pmp, pmdidType, iTypeModifier, fByVal, fNull, pba, ulLength),
	m_lValue(lValue)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumStatsLintMappable::Serialize
//
//	@doc:
//		Serialize datum in DXL format
//
//---------------------------------------------------------------------------
void
CDXLDatumStatsLintMappable::Serialize
	(
	CXMLSerializer *pxmlser
	)
{
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	if (IDefaultTypeModifier != ITypeModifier())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), ITypeModifier());
	}
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsNull), m_fNull);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsByValue), m_fByVal);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenValue), m_fNull, Pba(), UlLength());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenLintValue), LStatsMapping());
}


// EOF
