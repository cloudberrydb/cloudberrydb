//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalBroadcastMotion.cpp
//
//	@doc:
//		Implementation of DXL physical broadcast motion operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalBroadcastMotion.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalBroadcastMotion::CDXLPhysicalBroadcastMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalBroadcastMotion::CDXLPhysicalBroadcastMotion
	(
	IMemoryPool *pmp
	)
	:
	CDXLPhysicalMotion(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalBroadcastMotion::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalBroadcastMotion::Edxlop() const
{
	return EdxlopPhysicalMotionBroadcast;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalBroadcastMotion::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalBroadcastMotion::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalBroadcastMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalBroadcastMotion::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalBroadcastMotion::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
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
//		CDXLPhysicalBroadcastMotion::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalBroadcastMotion::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);

	GPOS_ASSERT(m_pdrgpiInputSegIds != NULL);
	GPOS_ASSERT(0 < m_pdrgpiInputSegIds->UlLength());
	GPOS_ASSERT(m_pdrgpiOutputSegIds != NULL);
	GPOS_ASSERT(0 < m_pdrgpiOutputSegIds->UlLength());

	GPOS_ASSERT(EdxlbmIndexSentinel == pdxln->UlArity());
	
	CDXLNode *pdxlnChild = (*pdxln)[EdxlbmIndexChild];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
