//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalLimit.cpp
//
//	@doc:
//		Implementation of DXL physical LIMIT operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalLimit.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalLimit::CDXLPhysicalLimit
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalLimit::CDXLPhysicalLimit
	(
	IMemoryPool *pmp
	)
	:
	CDXLPhysical(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalLimit::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalLimit::Edxlop() const
{
	return EdxlopPhysicalLimit;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalLimit::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalLimit::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalLimit);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalLimit::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalLimit::SerializeToDXL
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

	// serialize children nodes

	const DrgPdxln *pdrgpdxln = pdxln->PdrgpdxlnChildren();

	GPOS_ASSERT(4 == pdxln->UlArity());
	// serialize the first two children: target-list and plan
	for (ULONG i = 0; i < 4; i++)
	{
		CDXLNode *pdxlnChild = (*pdrgpdxln)[i];
		pdxlnChild->SerializeToDXL(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}



#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalLimit::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalLimit::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(4 == pdxln->UlArity());

	// Assert proj list is valid
	CDXLNode *pdxlnProjList = (*pdxln)[EdxllimitIndexProjList];
	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnProjList->Pdxlop()->Edxlop());

	// assert child plan is a physical plan and is valid

	CDXLNode *pdxlnChild = (*pdxln)[EdxllimitIndexChildPlan];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

	// Assert the validity of Count and Offset

	CDXLNode *pdxlnCount = (*pdxln)[EdxllimitIndexLimitCount];
	GPOS_ASSERT(EdxlopScalarLimitCount == pdxlnCount->Pdxlop()->Edxlop());

	CDXLNode *pdxlnOffset = (*pdxln)[EdxllimitIndexLimitOffset];
	GPOS_ASSERT(EdxlopScalarLimitOffset == pdxlnOffset->Pdxlop()->Edxlop());

	if (fValidateChildren)
	{
		pdxlnProjList->Pdxlop()->AssertValid(pdxlnProjList, fValidateChildren);
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		pdxlnCount->Pdxlop()->AssertValid(pdxlnCount, fValidateChildren);
		pdxlnOffset->Pdxlop()->AssertValid(pdxlnOffset, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
