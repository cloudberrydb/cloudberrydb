//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalProperties.cpp
//
//	@doc:
//		Implementation of DXL physical operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalProperties.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalProperties::CDXLPhysicalProperties
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalProperties::CDXLPhysicalProperties
	(
	CDXLOperatorCost *pdxlopcost
	)
	:
	CDXLProperties(),
	m_pdxlopcost(pdxlopcost)
{}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalProperties::~CDXLPhysicalProperties
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalProperties::~CDXLPhysicalProperties()
{
	CRefCount::SafeRelease(m_pdxlopcost);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalProperties::SerializePropertiesToDXL
//
//	@doc:
//		Serialize operator properties in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalProperties::SerializePropertiesToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenProperties));

	m_pdxlopcost->SerializeToDXL(pxmlser);
	SerializeStatsToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenProperties));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalProperties::Pdxlopcost
//
//	@doc:
//		Return cost of operator
//
//---------------------------------------------------------------------------
CDXLOperatorCost *
CDXLPhysicalProperties::Pdxlopcost() const
{
	return m_pdxlopcost;
}			

// EOF
