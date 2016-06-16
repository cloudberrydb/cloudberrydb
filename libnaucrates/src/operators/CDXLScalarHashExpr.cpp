//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarHashExpr.cpp
//
//	@doc:
//		Implementation of DXL hash expressions for redistribute operators
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarHashExpr.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExpr::CDXLScalarHashExpr
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarHashExpr::CDXLScalarHashExpr
	(
	IMemoryPool *pmp,
	IMDId *pmdidType
	)
	:
	CDXLScalar(pmp),
	m_pmdidType(pmdidType)
{
	GPOS_ASSERT(m_pmdidType->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExpr::~CDXLScalarHashExpr
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarHashExpr::~CDXLScalarHashExpr()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExpr::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarHashExpr::Edxlop() const
{
	return EdxlopScalarHashExpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExpr::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarHashExpr::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarHashExpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExpr::PmdidType
//
//	@doc:
//		Hash expression type from the catalog
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarHashExpr::PmdidType() const
{
	return m_pmdidType;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExpr::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarHashExpr::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarHashExpr::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarHashExpr::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren 
	) 
	const
{
	GPOS_ASSERT(1 == pdxln->UlArity());
	CDXLNode *pdxlnChild = (*pdxln)[0];
	
	GPOS_ASSERT(EdxloptypeScalar == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}

#endif // GPOS_DEBUG


// EOF
