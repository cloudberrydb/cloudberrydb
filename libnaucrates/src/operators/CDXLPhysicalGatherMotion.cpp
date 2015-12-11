//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalGatherMotion.cpp
//
//	@doc:
//		Implementation of DXL physical gather motion operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalGatherMotion.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalGatherMotion::CDXLPhysicalGatherMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalGatherMotion::CDXLPhysicalGatherMotion
	(
	IMemoryPool *pmp
	)
	:
	CDXLPhysicalMotion(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalGatherMotion::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalGatherMotion::Edxlop() const
{
	return EdxlopPhysicalMotionGather;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalGatherMotion::IOutputSegIdx
//
//	@doc:
//		Output segment index
//
//---------------------------------------------------------------------------
INT
CDXLPhysicalGatherMotion::IOutputSegIdx() const
{
	GPOS_ASSERT(1 == m_pdrgpiOutputSegIds->UlSafeLength());
	return *((*m_pdrgpiOutputSegIds)[0]);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalGatherMotion::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalGatherMotion::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalGatherMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalGatherMotion::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalGatherMotion::SerializeToDXL
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
//		CDXLPhysicalGatherMotion::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalGatherMotion::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	GPOS_ASSERT(0 < m_pdrgpiInputSegIds->UlSafeLength());
	GPOS_ASSERT(1 == m_pdrgpiOutputSegIds->UlSafeLength());

	GPOS_ASSERT(EdxlgmIndexSentinel == pdxln->UlArity());
	
	CDXLNode *pdxlnChild = (*pdxln)[EdxlgmIndexChild];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
