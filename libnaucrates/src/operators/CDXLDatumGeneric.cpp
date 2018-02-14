//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatumGeneric.cpp
//
//	@doc:
//		Implementation of DXL datum of type generic
//		
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDatumGeneric.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumGeneric::CDXLDatumGeneric
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLDatumGeneric::CDXLDatumGeneric
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	INT iTypeModifier,
	BOOL fByVal,
	BOOL fNull,
	BYTE *pba,
	ULONG ulLength
	)
	:
	CDXLDatum(pmp, pmdidType, iTypeModifier, fNull, ulLength),
	m_fByVal(fByVal),
	m_pba(pba)
{
	GPOS_ASSERT_IMP(m_fNull, (m_pba == NULL) && (m_ulLength == 0));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumGeneric::~CDXLDatumGeneric
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLDatumGeneric::~CDXLDatumGeneric()
{
	GPOS_DELETE_ARRAY(m_pba);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumGeneric::Pba
//
//	@doc:
//		Returns the bytearray of the datum
//
//---------------------------------------------------------------------------
const BYTE *
CDXLDatumGeneric::Pba() const
{
	return m_pba;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLDatumGeneric::Serialize
//
//	@doc:
//		Serialize datum in DXL format
//
//---------------------------------------------------------------------------
void
CDXLDatumGeneric::Serialize
	(
	CXMLSerializer *pxmlser
	)
{
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	if (IDefaultTypeModifier != ITypeModifier())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), m_iTypeModifier);
	}
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsNull), m_fNull);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsByValue), m_fByVal);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenValue), m_fNull, Pba(), UlLength());
}

// EOF
