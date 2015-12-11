//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalBitmapTableScan.cpp
//
//	@doc:
//		Class for representing DXL bitmap table scan operators.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalBitmapTableScan.h"

#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalBitmapTableScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalBitmapTableScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalBitmapTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalBitmapTableScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalBitmapTableScan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	// serialize table descriptor
	m_pdxltabdesc->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

// EOF
