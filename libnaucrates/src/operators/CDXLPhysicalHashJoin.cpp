//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalHashJoin.cpp
//
//	@doc:
//		Implementation of DXL physical hash join operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalHashJoin.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::CDXLPhysicalHashJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalHashJoin::CDXLPhysicalHashJoin
	(
	IMemoryPool *pmp,
	EdxlJoinType edxljt
	)
	:
	CDXLPhysicalJoin(pmp, edxljt)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalHashJoin::Edxlop() const
{
	return EdxlopPhysicalHashJoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalHashJoin::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalHashJoin);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalHashJoin::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenJoinType), PstrJoinTypeName());
	
	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);		
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalHashJoin::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalHashJoin::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	
	GPOS_ASSERT(EdxlhjIndexSentinel == pdxln->UlArity());
	GPOS_ASSERT(EdxljtSentinel > Edxltype());
	
	CDXLNode *pdxlnJoinFilter = (*pdxln)[EdxlhjIndexJoinFilter];
	CDXLNode *pdxlnHashClauses = (*pdxln)[EdxlhjIndexHashCondList];
	CDXLNode *pdxlnLeft = (*pdxln)[EdxlhjIndexHashLeft];
	CDXLNode *pdxlnRight = (*pdxln)[EdxlhjIndexHashRight];

	// assert children are of right type (physical/scalar)
	GPOS_ASSERT(EdxlopScalarJoinFilter == pdxlnJoinFilter->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxlopScalarHashCondList == pdxlnHashClauses->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnLeft->Pdxlop()->Edxloperatortype());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnRight->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnJoinFilter->Pdxlop()->AssertValid(pdxlnJoinFilter, fValidateChildren);
		pdxlnHashClauses->Pdxlop()->AssertValid(pdxlnHashClauses, fValidateChildren);
		pdxlnLeft->Pdxlop()->AssertValid(pdxlnLeft, fValidateChildren);
		pdxlnRight->Pdxlop()->AssertValid(pdxlnRight, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
