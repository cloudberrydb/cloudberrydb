//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalRoutedDistributeMotion.cpp
//
//	@doc:
//		Implementation of DXL physical routed redistribute motion operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalRoutedDistributeMotion.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRoutedDistributeMotion::CDXLPhysicalRoutedDistributeMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalRoutedDistributeMotion::CDXLPhysicalRoutedDistributeMotion
	(
	IMemoryPool *pmp,
	ULONG ulSegmentIdCol
	)
	:
	CDXLPhysicalMotion(pmp),
	m_ulSegmentIdCol(ulSegmentIdCol)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRoutedDistributeMotion::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalRoutedDistributeMotion::Edxlop() const
{
	return EdxlopPhysicalMotionRoutedDistribute;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRoutedDistributeMotion::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalRoutedDistributeMotion::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalRoutedDistributeMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRoutedDistributeMotion::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalRoutedDistributeMotion::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSegmentIdCol), m_ulSegmentIdCol);
	
	SerializeSegmentInfoToDXL(pxmlser);
	
	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRoutedDistributeMotion::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalRoutedDistributeMotion::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	
	GPOS_ASSERT(m_pdrgpiInputSegIds != NULL);
	GPOS_ASSERT(0 < m_pdrgpiInputSegIds->UlLength());
	GPOS_ASSERT(m_pdrgpiOutputSegIds != NULL);
	GPOS_ASSERT(0 < m_pdrgpiOutputSegIds->UlLength());
	
	GPOS_ASSERT(EdxlroutedmIndexSentinel == pdxln->UlArity());
	
	CDXLNode *pdxlnChild = (*pdxln)[EdxlroutedmIndexChild];

	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
