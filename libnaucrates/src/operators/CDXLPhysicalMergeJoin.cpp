//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalMergeJoin.cpp
//
//	@doc:
//		Implementation of DXL physical merge join operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalMergeJoin.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMergeJoin::CDXLPhysicalMergeJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalMergeJoin::CDXLPhysicalMergeJoin
	(
	IMemoryPool *pmp,
	EdxlJoinType edxljt,
	BOOL fUniqueOuter
	)
	:
	CDXLPhysicalJoin(pmp, edxljt),
	m_fUniqueOuter(fUniqueOuter)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMergeJoin::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalMergeJoin::Edxlop() const
{
	return EdxlopPhysicalMergeJoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMergeJoin::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalMergeJoin::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalMergeJoin);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMergeJoin::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalMergeJoin::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenJoinType), PstrJoinTypeName());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMergeJoinUniqueOuter), m_fUniqueOuter);

	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);		
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMergeJoin::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalMergeJoin::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	
	GPOS_ASSERT(EdxlmjIndexSentinel == pdxln->UlArity());
	GPOS_ASSERT(EdxljtSentinel > Edxltype());
	
	CDXLNode *pdxlnJoinFilter = (*pdxln)[EdxlmjIndexJoinFilter];
	CDXLNode *pdxlnMergeClauses = (*pdxln)[EdxlmjIndexMergeCondList];
	CDXLNode *pdxlnLeft = (*pdxln)[EdxlmjIndexLeftChild];
	CDXLNode *pdxlnRight = (*pdxln)[EdxlmjIndexRightChild];

	// assert children are of right type (physical/scalar)
	GPOS_ASSERT(EdxlopScalarJoinFilter == pdxlnJoinFilter->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxlopScalarMergeCondList == pdxlnMergeClauses->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnLeft->Pdxlop()->Edxloperatortype());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnRight->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnJoinFilter->Pdxlop()->AssertValid(pdxlnJoinFilter, fValidateChildren);
		pdxlnMergeClauses->Pdxlop()->AssertValid(pdxlnMergeClauses, fValidateChildren);
		pdxlnLeft->Pdxlop()->AssertValid(pdxlnLeft, fValidateChildren);
		pdxlnRight->Pdxlop()->AssertValid(pdxlnRight, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
