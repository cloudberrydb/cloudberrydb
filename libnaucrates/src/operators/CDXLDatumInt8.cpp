//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumInt8.cpp
//
//	@doc:
//		Implementation of DXL datum of type long int
//		
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLDatumInt8.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt8::CDXLDatumInt8
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumInt8::CDXLDatumInt8
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	BOOL fNull,
	LINT lVal
	)
	:
	CDXLDatum(pmp, pmdidType, IDefaultTypeModifier, fNull, 8 /*ulLength*/),
	m_lVal(lVal)
{
	if (fNull)
	{
		m_lVal = 0;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt8::LValue
//
//	@doc:
//		Return the long int value
//
//---------------------------------------------------------------------------
LINT
CDXLDatumInt8::LValue() const
{
	return m_lVal;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumInt8::Serialize
//
//	@doc:
//		Serialize datum in DXL format
//
//---------------------------------------------------------------------------
void
CDXLDatumInt8::Serialize
	(
	CXMLSerializer *pxmlser
	)
{
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsNull), m_fNull);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsByValue), FByValue());
	
	if (!m_fNull)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenValue), m_lVal);
	}
}

// EOF
