//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalRedistributeMotion.cpp
//
//	@doc:
//		Implementation of DXL physical redistribute motion operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalRedistributeMotion.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRedistributeMotion::CDXLPhysicalRedistributeMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalRedistributeMotion::CDXLPhysicalRedistributeMotion
	(
	IMemoryPool *pmp,
	BOOL fDuplicateSensitive
	)
	:
	CDXLPhysicalMotion(pmp),
	m_fDuplicateSensitive(fDuplicateSensitive)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRedistributeMotion::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalRedistributeMotion::Edxlop() const
{
	return EdxlopPhysicalMotionRedistribute;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRedistributeMotion::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalRedistributeMotion::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalRedistributeMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRedistributeMotion::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalRedistributeMotion::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
	SerializeSegmentInfoToDXL(pxmlser);
	
	if (m_fDuplicateSensitive)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDuplicateSensitive), true);
	}	
	
	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRedistributeMotion::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalRedistributeMotion::AssertValid
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
	
	GPOS_ASSERT(EdxlrmIndexSentinel == pdxln->UlArity());
	
	CDXLNode *pdxlnChild = (*pdxln)[EdxlrmIndexChild];
	CDXLNode *pdxlnHashExprList = (*pdxln)[EdxlrmIndexHashExprList];

	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		pdxlnHashExprList->Pdxlop()->AssertValid(pdxlnHashExprList, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
