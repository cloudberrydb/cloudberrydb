//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalRandomMotion.cpp
//
//	@doc:
//		Implementation of DXL physical random motion operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalRandomMotion.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRandomMotion::CDXLPhysicalRandomMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalRandomMotion::CDXLPhysicalRandomMotion
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
//		CDXLPhysicalRandomMotion::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalRandomMotion::Edxlop() const
{
	return EdxlopPhysicalMotionRandom;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRandomMotion::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalRandomMotion::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalRandomMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRandomMotion::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalRandomMotion::SerializeToDXL
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
//		CDXLPhysicalRandomMotion::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalRandomMotion::AssertValid
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

	GPOS_ASSERT(EdxlrandommIndexSentinel == pdxln->UlArity());
	
	CDXLNode *pdxlnChild = (*pdxln)[EdxlrandommIndexChild];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
