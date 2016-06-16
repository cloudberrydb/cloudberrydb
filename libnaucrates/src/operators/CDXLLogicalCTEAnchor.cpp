//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEAnchor.cpp
//
//	@doc:
//		Implementation of DXL logical CTE anchors
//		
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalCTEAnchor.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEAnchor::CDXLLogicalCTEAnchor
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalCTEAnchor::CDXLLogicalCTEAnchor
	(
	IMemoryPool *pmp,
	ULONG ulId
	)
	:
	CDXLLogical(pmp),
	m_ulId(ulId)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEAnchor::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalCTEAnchor::Edxlop() const
{
	return EdxlopLogicalCTEAnchor;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEAnchor::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalCTEAnchor::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalCTEAnchor);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEAnchor::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalCTEAnchor::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCTEId), UlId());
	
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalCTEAnchor::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalCTEAnchor::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(1 == pdxln->UlArity());

	CDXLNode *pdxlnChild = (*pdxln)[0];
	GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
