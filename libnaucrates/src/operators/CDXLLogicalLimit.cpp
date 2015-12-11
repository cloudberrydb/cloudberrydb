//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDXLLogicalLimit.cpp
//
//	@doc:
//		Implementation of DXL logical limit operator
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLLogicalLimit.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalLimit::CDXLLogicalLimit
//
//	@doc:
//		Construct a DXL Logical limit node
//
//---------------------------------------------------------------------------
CDXLLogicalLimit::CDXLLogicalLimit
	(
	IMemoryPool *pmp,
	BOOL fNonRemovableLimit
	)
	:
	CDXLLogical(pmp),
	m_fTopLimitUnderDML(fNonRemovableLimit)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalLimit::~CDXLLogicalLimit
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLLogicalLimit::~CDXLLogicalLimit()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalLimit::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalLimit::Edxlop() const
{
	return EdxlopLogicalLimit;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalLimit::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalLimit::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalLimit);
}
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalLimit::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalLimit::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	if (m_fTopLimitUnderDML)
	{
		pxmlser->AddAttribute
					(
					CDXLTokens::PstrToken(EdxltokenTopLimitUnderDML),
					m_fTopLimitUnderDML
					);
	}

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalLimit::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalLimit::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(4 == pdxln->UlArity());

	// Assert the validity of sort column list
	CDXLNode *pdxlnSortColList = (*pdxln)[EdxllogicallimitIndexSortColList];
	GPOS_ASSERT(EdxloptypeScalar == pdxlnSortColList->Pdxlop()->Edxloperatortype());

	// Assert the validity of Count and Offset

	CDXLNode *pdxlnCount = (*pdxln)[EdxllogicallimitIndexLimitCount];
	GPOS_ASSERT(EdxlopScalarLimitCount == pdxlnCount->Pdxlop()->Edxlop());
	
	CDXLNode *pdxlnOffset = (*pdxln)[EdxllogicallimitIndexLimitOffset];
	GPOS_ASSERT(EdxlopScalarLimitOffset == pdxlnOffset->Pdxlop()->Edxlop());
		
	// Assert child plan is a logical plan and is valid
	CDXLNode *pdxlnChild = (*pdxln)[EdxllogicallimitIndexChildPlan];
	GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnSortColList->Pdxlop()->AssertValid(pdxlnSortColList, fValidateChildren);
		pdxlnOffset->Pdxlop()->AssertValid(pdxlnOffset, fValidateChildren);
		pdxlnCount->Pdxlop()->AssertValid(pdxlnCount, fValidateChildren);
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}

}
#endif // GPOS_DEBUG

// EOF
