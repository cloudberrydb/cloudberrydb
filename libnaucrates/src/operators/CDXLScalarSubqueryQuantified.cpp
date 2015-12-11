//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubqueryQuantified.cpp
//
//	@doc:
//		Implementation of quantified subquery operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLScalarSubqueryQuantified.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryQuantified::CDXLScalarSubqueryQuantified
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryQuantified::CDXLScalarSubqueryQuantified
	(
	IMemoryPool *pmp,
	IMDId *pmdidScalarOp,
	CMDName *pmdnameScalarOp,
	ULONG ulColId
	)
	:
	CDXLScalar(pmp),
	m_pmdidScalarOp(pmdidScalarOp),
	m_pmdnameScalarOp(pmdnameScalarOp),
	m_ulColId(ulColId)
{
	GPOS_ASSERT(pmdidScalarOp->FValid());
	GPOS_ASSERT(NULL != pmdnameScalarOp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryQuantified::~CDXLScalarSubqueryQuantified
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarSubqueryQuantified::~CDXLScalarSubqueryQuantified()
{
	m_pmdidScalarOp->Release();
	GPOS_DELETE(m_pmdnameScalarOp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryQuantified::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSubqueryQuantified::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize operator id and name
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOpName), m_pmdnameScalarOp->Pstr());
	m_pmdidScalarOp->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenOpNo));

	// serialize computed column id
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), m_ulColId);

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubqueryQuantified::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarSubqueryQuantified::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	GPOS_ASSERT(2 == pdxln->UlArity());

	CDXLNode *pdxlnScalarChild = (*pdxln)[EdxlsqquantifiedIndexScalar];
	CDXLNode *pdxlnRelationalChild = (*pdxln)[EdxlsqquantifiedIndexRelational];

	GPOS_ASSERT(EdxloptypeScalar == pdxlnScalarChild->Pdxlop()->Edxloperatortype());
	GPOS_ASSERT(EdxloptypeLogical == pdxlnRelationalChild->Pdxlop()->Edxloperatortype());

	pdxln->AssertValid(fValidateChildren);
}
#endif // GPOS_DEBUG

// EOF
