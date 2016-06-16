//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarInitPlan.cpp
//
//	@doc:
//		Implementation of DXL Scalar InitPlan operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLScalarInitPlan.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarInitPlan::CDXLScalarInitPlan
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarInitPlan::CDXLScalarInitPlan
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarInitPlan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarInitPlan::Edxlop() const
{
	return EdxlopScalarInitPlan;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarInitPlan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarInitPlan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarInitPlan);
}



//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarInitPlan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarInitPlan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	const DrgPdxln *pdrgpdxln = pdxln->PdrgpdxlnChildren();

	GPOS_ASSERT(1 == pdxln->UlArity());

	// serialize the child plan
	CDXLNode *pdxlnChild = (*pdrgpdxln)[0];
	pdxlnChild->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}



#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarInitPlan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarInitPlan::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(EdxlInitPlanIndexSentinel == pdxln->UlArity());

	// assert child plan is a physical plan and is valid

	CDXLNode *pdxlnChild = (*pdxln)[EdxlInitPlanIndexChildPlan];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
