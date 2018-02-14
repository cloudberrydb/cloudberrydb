//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLDatumStatsDoubleMappable.cpp
//
//	@doc:
//		Implementation of DXL datum of types having double mapping
//		
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumStatsDoubleMappable.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumStatsDoubleMappable::CDXLDatumStatsDoubleMappable
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumStatsDoubleMappable::CDXLDatumStatsDoubleMappable
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	INT iTypeModifier,
	BOOL fByVal,
	BOOL fNull,
	BYTE *pba,
	ULONG ulLength,
	CDouble dValue
	)
	:
	CDXLDatumGeneric(pmp, pmdidType, iTypeModifier, fByVal, fNull, pba, ulLength),
	m_dValue(dValue)
{
}
//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumStatsDoubleMappable::Serialize
//
//	@doc:
//		Serialize datum in DXL format
//
//---------------------------------------------------------------------------
void
CDXLDatumStatsDoubleMappable::Serialize
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
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDoubleValue), DStatsMapping());
}

// EOF
