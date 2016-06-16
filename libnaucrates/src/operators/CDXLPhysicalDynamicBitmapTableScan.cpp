//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalDynamicBitmapTableScan.cpp
//
//	@doc:
//		Class for representing DXL bitmap table scan operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalDynamicBitmapTableScan.h"

#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicBitmapTableScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalDynamicBitmapTableScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalDynamicBitmapTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicBitmapTableScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDynamicBitmapTableScan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartIndexId), m_ulPartIndexId);
	if (m_ulPartIndexIdPrintable != m_ulPartIndexId)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartIndexIdPrintable), m_ulPartIndexIdPrintable);
	}
	pdxln->SerializePropertiesToDXL(pxmlser);
	pdxln->SerializeChildrenToDXL(pxmlser);
	m_pdxltabdesc->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

// EOF
